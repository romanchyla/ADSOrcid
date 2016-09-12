from .. import app
from . import GenericWorker
import time
import traceback
from ..models import KeyValue, ClaimsLog
from ..utils import get_date
from .. import matcher, updater
import requests
import datetime
import threading
from sqlalchemy import and_
from dateutil.tz import tzutc
from ADSOrcid import importer
import random


class OrcidImporter(GenericWorker.RabbitMQWorker):
    """
    This worker has two components:
        1) 'cron-like' job which periodically checks for new
           updates inside orcid-service
        2) regular worker which receives a payload to start
           indexing of claims *for one author*. It will discover
           differences and pushes new/updated claims into the
           queue.
           
        After this step, we'll have a record int he ClaimsLog (but not claiming
        yet).
    """
    def __init__(self, params=None):
        super(OrcidImporter, self).__init__(params)
        self.error_counter = 0
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
        
        self.checker = threading.Thread(target=runner, kwargs={'worker': self})
        self.checker.setDaemon(True)
        self.checker.start()
        
        
    def check_orcid_updates(self):
        """Checks the remote server for updates"""
        
        if self.error_counter:
            sleep_time = max(self.error_counter ** app.config.get('API_ERROR_EXPONENT', 2), 
                             app.config.get('API_ERROR_MAX_SLEEP', 300))
            self.logger.error('Error counter value is {}, forcing sleep for {} secs'.format(
                         self.error_counter, sleep_time))
            time.sleep(sleep_time)
        
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
                            params={'fields': ['orcid_id', 'updated', 'created']},
                            headers = {'Authorization': 'Bearer {0}'.format(app.config.get('API_TOKEN'))})
                
                if r.status_code != 200:
                    self.logger.error('Failed getting {0}\n{1}'.format(
                                app.config.get('API_ORCID_UPDATES_ENDPOINT') % kv.value,
                                r.text))
                    self.error_counter += 1
                    return False
                
                if r.text.strip() == "":
                    return False
                
                # we received the data, immediately update the databaes (so that other processes don't 
                # ask for the same starting date)
                data = r.json()
                
                if len(data) == 0:
                    return False
                
                # reset the error-counter, we got data from the api
                self.error_counter = 0
                
                # data should be ordered by date update (but to be sure, let's check it); we'll save it
                # as latest 'check point'
                dates = [get_date(x['updated']) for x in data]
                dates = sorted(dates, reverse=True)
                
                kv.value = dates[0].isoformat()
                session.merge(kv)
                session.commit()
                
                for rec in data:
                    payload = {'orcidid': rec['orcid_id'], 'start': latest_point.isoformat()}
                    # publish data to ourselves
                    self.publish(payload, topic=self.params.get('subscribe', 'ads.orcid.fresh-claims'))
                return True # continue processing

    def _get_ads_orcid_profile(self, orcidid):
        r = requests.get(app.config.get('API_ORCID_EXPORT_PROFILE') % orcidid,
                 params={'reload': True},
                 headers={'Accept': 'application/json', 'Authorization': 'Bearer:%s' % app.config.get('API_TOKEN')})
        if r.status_code == 200:
            return r.json()
        else:
            self.logger.warning('Missing profile for: {0}'.format(orcidid))
            self.logger.warning(r.text)
            return {}
        
    def process_payload(self, msg, skip_inserting=False, **kwargs):
        """
        Fetch a fresh profile from the orcid-service and compare
        it against the state of the storage (diff). And re-index/update
        them.
        

        :param msg: contains the message inside the packet
            {
             'orcidid': '.....',
             'start': 'ISO8801 formatted date (optional), indicates 
                 the moment we checked the orcid-service'
             'force': Boolean (if present, we'll not skip unchanged
                 profile)
            }
        :return: no return
        """
        
        assert 'orcidid' in msg
        orcidid = msg['orcidid']

        # make sure the author is there (even if without documents) 
        author = matcher.retrieve_orcid(orcidid) # @UnusedVariable
        
        data = self._get_ads_orcid_profile(orcidid)
        if data is None:
            return #TODO: remove all existing claims?
        
        profile = data.get('profile', {})
        if not profile:
            pass #TODO: remove all existing claims?
        
        
        to_claim = []
        with app.session_scope() as session:
              
            # orcid is THE ugliest datastructure of today!
            try:
                works = profile['orcid-profile']['orcid-activities']['orcid-works']['orcid-work']
            except:
                self.logger.warning('Nothing to do for: '
                    '{0} ({1})'.format(orcidid,
                                       traceback.format_exc()))
                return
    
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
                    if msg.get('force'):
                        self.logger.info("Profile {0} unchanged, but force in effect.".format(orcidid))
                    else:
                        self.logger.info("Skipping {0} (profile unchanged)".format(orcidid))
                        return
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
                elif cl.status in ('claimed', 'updated', 'forced'):
                    updated[bibc] = (cl.bibcode, get_date(cl.created))
                    if bibc in removed:
                        del removed[bibc]
            
            
            # now get info about each record #TODO: enhance the matching (and refactor)
            orcid_present = {}
            for w in works:
                bibc = None
                try:
                    ids =  w['work-external-identifiers']['work-external-identifier']
                    seek_ids = []
                    
                    # painstakingly check ids (start from a bibcode) if we can find it
                    # we'll send it through (but start from bibcodes, then dois, arxiv...)
                    fmap = app.config.get('ORCID_IDENTIFIERS_ORDER', {'bibcode': 9, '*': -1})
                    for x in ids:
                        xtype = x.get('work-external-identifier-type', None)
                        if xtype:
                            seek_ids.append((fmap.get(xtype.lower().strip(), fmap.get('*', -1)), 
                                             x['work-external-identifier-id']['value']))
                    
                    if len(seek_ids) == 0:
                        continue
                    
                    seek_ids = sorted(seek_ids, key=lambda x: x[0], reverse=True)
                    for _priority, fvalue in seek_ids:
                        try:
                            time.sleep(1.0/random.randint(1, 20)) # be nice to the api
                            metadata = updater.retrieve_metadata(fvalue, search_identifiers=True)
                            bibc = metadata.get('bibcode')
                            self.logger.info('Match found {0} -> {1}'.format(fvalue, bibc))
                            break
                        except Exception, e:
                            self.logger.warning(e.message)
                            
                    
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
                    else:
                        self.logger.warning('Found no bibcode for {0}'.format(ids))
                        
                except KeyError, e:
                    self.logger.warning('Error processing a record: '
                        '{0} ({1})'.format(w,
                                           traceback.format_exc()))
                    continue
                except TypeError, e:
                    self.logger.warning('Error processing a record: '
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
                elif msg.get('force', False):
                    to_claim.append(importer.create_claim(bibcode=orcid_claim[0], 
                                                      orcidid=orcidid, 
                                                      provenance=self.__class__.__name__, 
                                                      status='forced',
                                                      date=orcid_claim[1])
                                                      )
                else:
                    to_claim.append(importer.create_claim(bibcode=orcid_claim[0], 
                                                      orcidid=orcidid, 
                                                      provenance=self.__class__.__name__, 
                                                      status='unchanged',
                                                      date=orcid_claim[1]))
        if len(to_claim):
            json_claims = importer.insert_claims(to_claim)
            for claim in json_claims:
                claim['bibcode_verified'] = True
                self.publish(claim)
