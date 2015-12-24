from .. import app
from . import GenericWorker
import time
import traceback
from ..models import KeyValue, ClaimsLog
from ..utils import get_date
from .. import importer
import requests
import datetime
import threading
from sqlalchemy import and_
from dateutil.tz import tzutc

class ClaimsImporter(GenericWorker.RabbitMQWorker):
    """
    Checks if a claim exists in the remote ADSWS service.
    It then creates the claim and pushes it into the RabbitMQ pipeline.
    """
    def __init__(self, params=None):
        super(ClaimsImporter, self).__init__(params)
        app.init_app()
        self.start_cronjob()
        
    def start_cronjob(self):
        """Initiates the task in the background"""
        self.keep_running = True
        def runner(worker):
            time.sleep(1)
            while worker.keep_running:
                try:
                    # keep consuming the remote stream until there is 0 recs
                    while worker.check_orcid_updates():
                        pass
                    time.sleep(app.config.get('ORCID_CHECK_FOR_CHANGES', 60*5) / 2)
                except Exception, e:
                    worker.logger.error('Error fetching profiles: '
                                '{0} ({1})'.format(e.message,
                                                   traceback.format_exc()))
        
        self.checker = threading.Thread(target=runner, kwargs={'GenericWorker': self})
        self.checker.setDaemon(True)
        self.checker.start()
        
        
    def check_orcid_updates(self):
        """Checks the remote server for updates"""
        with app.session_scope() as session:
            kv = session.query(KeyValue).filter_by(key='last.check').first()
            if kv is None:
                kv = KeyValue(key='last.check', value='1974-11-09T22:56:52.518001Z') #force update
            
            latest_point = get_date(kv.value) # RFC 3339 format
            now = get_date()
            
            delta = now - latest_point
            if delta.total_seconds() > app.config.get('ORCID_CHECK_FOR_CHANGES', 60*5): #default 5min
                self.logger.info("Checking for orcid updates")
                
                # increase the timestamp by one microsec and get new updates
                latest_point = latest_point + datetime.timedelta(microseconds=1)
                r = requests.get(app.config.get('API_ORCID_UPDATES_ENDPOINT') % latest_point.isoformat(),
                             headers = {'Authorization': 'Bearer {0}'.format(app.config.get('API_TOKEN'))})
                
                if r.status_code != 200:
                    self.logger.error('Failed getting {0}\n{1}'.format(
                                app.config.get('API_ORCID_UPDATES_ENDPOINT') % kv.value,
                                r.text))
                    return
                
                if r.text.strip() == "":
                    return
                
                # we received the data, immediately update the databaes (so that other processes don't 
                # ask for the same starting date)
                data = r.json()
                
                if len(data) == 0:
                    return
                
                # data should be ordered by date update (but to be sure, let's check it); we'll save it
                # as latest 'check point'
                dates = [get_date(x['updated']) for x in data]
                dates = sorted(dates, reverse=True)
                
                kv.value = dates[0].isoformat()
                session.merge(kv)
                session.commit()
                
                to_claim = []
                for rec in data: # each rec is orcid:profile
                    
                    orcidid = rec['orcid_id']
                    
                    if not 'profile' in rec:
                        self.logger.error('Skipping (because of missing profile) {0}'.format(data['orcid_id']))
                        continue
                    #else: TODO: retrieve the fresh profile
                    
                    # orcid is THE ugliest datastructure of today!
                    profile = rec['profile']
                    try:
                        works = profile['orcid-profile']['orcid-activities']['orcid-works']['orcid-work']
                    except KeyError, e:
                        self.logger.error('Error processing a profile: '
                            '{0} ({1})'.format(orcidid,
                                               traceback.format_exc()))
                        continue
                    except TypeError, e:
                        self.logger.error('Error processing a profile: '
                            '{0} ({1})'.format(orcidid,
                                               traceback.format_exc()))
                        continue

                    # check we haven't seen this very profile already
                    try:
                        updt = str(profile['orcid-profile']['orcid-history']['last-modified-date']['value'])
                        updt = float('%s.%s' % (updt[0:10], updt[10:]))
                        updt = datetime.datetime.fromtimestamp(updt, tzutc())
                        updt = get_date(updt.isoformat())
                    except KeyError:
                        updt = get_date()
                                            
                    # find the most recent #full-import record
                    last_update = session.query(ClaimsLog).filter(
                        and_(ClaimsLog.status == '#full-import', ClaimsLog.orcidid == orcidid)
                        ).order_by(ClaimsLog.id.desc()).first()
                        
                    if last_update is None:
                        q = session.query(ClaimsLog).filter_by(orcidid=orcidid).order_by(ClaimsLog.id.asc())
                    else:
                        if get_date(last_update.created) == updt:
                            self.logger.info("Skipping {0} (profile unchanged)".format(orcidid))
                            continue
                        q = session.query(ClaimsLog).filter(
                            and_(ClaimsLog.orcidid == orcidid, ClaimsLog.id > last_update.id)) \
                            .order_by(ClaimsLog.id.asc())
                    
                            
                    # find all records we have processed at some point
                    updated = {}
                    removed = {}
                    
                    for cl in q.all():
                        if not cl.bibcode:
                            continue
                        bibc = cl.bibcode.lower()
                        if cl.status == 'removed':
                            removed[bibc] = (cl.bibcode, get_date(cl.created))
                            if bibc in updated:
                                del updated[bibc]
                        elif cl.status in ('claimed', 'updated'):
                            updated[bibc] = (cl.bibcode, get_date(cl.created))
                            if bibc in removed:
                                del removed[bibc]
                    
                    
                    orcid_present = {}
                    for w in works:
                        bibc = None
                        try:
                            ids =  w['work-external-identifiers']['work-external-identifier']
                            for x in ids:
                                type = x.get('work-external-identifier-type', None)
                                if type and type.lower() == 'bibcode':
                                    bibc = x['work-external-identifier-id']['value']
                                    break
                            if bibc:
                                # would you believe that orcid doesn't return floats?
                                ts = str(w['last-modified-date']['value'])
                                ts = float('%s.%s' % (ts[0:10], ts[10:]))
                                ts = datetime.datetime.fromtimestamp(ts, tzutc())
                                try:
                                    provenance = w['source']['source-name']['value']
                                except KeyError:
                                    provenance = 'orcid-profile'
                                orcid_present[bibc.lower().strip()] = (bibc.strip(), get_date(ts.isoformat()), provenance)
                        except KeyError, e:
                            self.logger.error('Error processing a record: '
                                '{0} ({1})'.format(w,
                                                   traceback.format_exc()))
                            continue
                        except TypeError, e:
                            self.logger.error('Error processing a record: '
                                '{0} ({1})'.format(w,
                                                   traceback.format_exc()))
                            continue
                    
                    
                    #always insert a record that marks the beginning of a full-import
                    #TODO: record orcid's last-modified-date
                    to_claim.append(importer.create_claim(bibcode='', 
                                                              orcidid=orcidid, 
                                                              provenance=self.__class__.__name__, 
                                                              status='#full-import',
                                                              date=updt
                                                              ))
                    
                    # find difference between what we have and what orcid has
                    claims_we_have = set(updated.keys()).difference(set(removed.keys()))
                    claims_orcid_has = set(orcid_present.keys())
                    
                    # those guys will be added (with ORCID date signature)
                    for c in claims_orcid_has.difference(claims_we_have):
                        claim = orcid_present[c]
                        to_claim.append(importer.create_claim(bibcode=claim[0], 
                                                              orcidid=orcidid, 
                                                              provenance=claim[2], 
                                                              status='claimed', 
                                                              date=claim[1])
                                                              )
                    
                    # those guys will be removed (since orcid doesn't have them)
                    for c in claims_we_have.difference(claims_orcid_has):
                        claim = updated[c]
                        to_claim.append(importer.create_claim(bibcode=claim[0], 
                                                              orcidid=orcidid, 
                                                              provenance=self.__class__.__name__, 
                                                              status='removed')
                                                              )
                        
                    # and those guys will be updated if their creation date is significantly
                    # off
                    for c in claims_orcid_has.intersection(claims_we_have):
                        
                        orcid_claim = orcid_present[c]
                        ads_claim = updated[c]
                        
                        delta = orcid_claim[1] - ads_claim[1]
                        if delta.total_seconds() > app.config.get('ORCID_UPDATE_WINDOW', 60): 
                            to_claim.append(importer.create_claim(bibcode=orcid_claim[0], 
                                                              orcidid=orcidid, 
                                                              provenance=self.__class__.__name__, 
                                                              status='updated',
                                                              date=orcid_claim[1])
                                                              )
                        else:
                            to_claim.append(importer.create_claim(bibcode=orcid_claim[0], 
                                                              orcidid=orcidid, 
                                                              provenance=self.__class__.__name__, 
                                                              status='unchanged',
                                                              date=orcid_claim[1]))
                if len(to_claim):
                    json_claims = importer.insert_claims(to_claim) # write to db
                    self.process_payload(json_claims, skip_inserting=True) # send to the queue
                    return len(json_claims)
                    
        
    def process_payload(self, msg, skip_inserting=False, **kwargs):
        """
        Normally, this GenericWorker will pro-actively check the remote web
        service, however it will also keep looking into the queue where
        the data can be registered (e.g. by a script)
        
        And if it encounters a claim, it will create log entry for it

        :param msg: contains the message inside the packet
            {'bibcode': '....',
            'orcidid': '.....',
            'provenance': 'string (optional)',
            'status': 'claimed|updated|deleted (optional)',
            'date': 'ISO8801 formatted date (optional)'
            }
        :return: no return
        """
        
        if isinstance(msg, list):
            for x in msg:
                x.setdefault('provenance', self.__class__.__name__)
        elif isinstance(msg, dict):
            msg.setdefault('provenance', self.__class__.__name__)
            msg = [msg]
        else:
            raise Exception('Received unknown payload {0}'.format(msg))
        
        if skip_inserting:
            c = msg
        else:
            c = importer.insert_claims(msg)
        
        if c and len(c) > 0:
            for claim in c:
                if claim.get('status', 'created') in ('unchanged', '#full-import'):
                    continue
                self.publish(claim)

