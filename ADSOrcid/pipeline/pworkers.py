"""
This contains the classes required for each worker, which are all inheriting
from the RabbitMQ class.
"""



from .. import app
from . import worker
from .. import importer, matcher, updater
from copy import deepcopy
from ..models import KeyValue, ClaimsLog
import datetime
from dateutil import parser
import requests
import json
import time
from sqlalchemy import and_
import threading
import traceback

class ClaimsImporter(worker.RabbitMQWorker):
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
                    print(traceback.format_exc())
                    worker.logger.error('Error fetching profiles: '
                                '{0} ({1})'.format(e.message,
                                                   traceback.format_exc()))
        
        self.checker = threading.Thread(target=runner, kwargs={'worker': self})
        self.checker.setDaemon(True)
        self.checker.start()
        
        
    def check_orcid_updates(self):
        """Checks the remote server for updates"""
        with app.session_scope() as session:
            kv = session.query(KeyValue).filter_by(key='last.check').first()
            if kv is None:
                kv = KeyValue(key='last.check', value='1974-11-09T22:56:52.518001') #force update
            
            latest_point = parser.parse(kv.value) # RFC 3339 format
            now = datetime.datetime.utcnow()
            
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
                dates = [parser.parse(x['updated']) for x in data]
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
                        updt = datetime.datetime.fromtimestamp(updt)
                    except KeyError:
                        updt = datetime.datetime.utcnow()
                                            
                    # find the most recent #full-import record
                    last_update = session.query(ClaimsLog).filter(
                        and_(ClaimsLog.status == '#full-import', ClaimsLog.orcidid == orcidid)
                        ).order_by(ClaimsLog.id.desc()).first()
                        
                    if last_update is None:
                        q = session.query(ClaimsLog).filter_by(orcidid=orcidid).order_by(ClaimsLog.id.asc())
                    else:
                        if last_update.created == updt:
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
                            removed[bibc] = (cl.bibcode, cl.created)
                            if bibc in updated:
                                del updated[bibc]
                        elif cl.status in ('claimed', 'updated'):
                            updated[bibc] = (cl.bibcode, cl.created)
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
                                try:
                                    provenance = w['source']['source-name']['value']
                                except KeyError:
                                    provenance = 'orcid-profile'
                                orcid_present[bibc.lower().strip()] = (bibc.strip(), datetime.datetime.fromtimestamp(ts), provenance)
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
        Normally, this worker will pro-actively check the remote web
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


class ClaimsIngester(worker.RabbitMQWorker):
    """
    Processes claims in the system; it enhances the claim
    with the information about the claimer. (and in the
    process, updates our knowledge about the ORCIDID)
    """
    def __init__(self, params=None):
        super(ClaimsIngester, self).__init__(params)
        app.init_app()
        
    def process_payload(self, msg, **kwargs):
        """
        :param msg: contains the message inside the packet
            {'bibcode': '....',
            'orcidid': '.....',
            'provenance': 'string (optional)',
            'status': 'claimed|updated|deleted (optional)',
            'date': 'ISO8801 formatted date (optional)'
            }
        :return: no return
        """
        
        if not isinstance(msg, dict):
            raise Exception('Received unknown payload {0}'.format(msg))
        
        if not msg.get('orcidid'):
            raise Exception('Unusable payload, missing orcidid {0}'.format(msg))
        
        author = matcher.retrieve_orcid(msg['orcidid'])
        
        if not author:
            raise Exception('Unable to retrieve info for {0}'.format(msg['orcidid']))
        
        msg['name'] = author['name']
        if author.get('facts', None):
            for k, v in author['facts'].iteritems():
                msg[k] = v
                
        msg['author_status'] = author['status']
        msg['account_id'] = author['account_id']
        msg['author_updated'] = author['updated']
        msg['author_id'] = author['id']
        
        if msg['author_status'] in ('blacklisted', 'postponed'):
            return
        
        self.publish(msg)


class MongoUpdater(worker.RabbitMQWorker):
    """
    Update the adsdata database; insert the orcid claim
    into 'orcid_claims' collection. This solution is a
    temporary one, until we have a better (actually new
    one) pipeline
    
    When ADS Classic data gets synchronized, it *first*
    mirrors files into the adsdata mongodb collection.
    After that, the import pipeline is ran. Therefore,
    we are assuming here that a claim gets registered
    and will already find the author's in the mongodb.
    So we update the mongodb, writing into a special 
    collection 'orcid_claims' -- and the solr updater
    has to grab data from there when pushing to indexer.
    
    """
    def __init__(self, params=None):
        super(MongoUpdater, self).__init__(params)
        app.init_app()
        self.init_mongo()
        
    def init_mongo(self):
        from pymongo import MongoClient
        self.mongo = MongoClient(app.config.get('MONGODB_URL'))
        self.mongodb = self.mongo[app.config.get('MONGODB_DB', 'adsdata')]
        self.mongocoll = self.mongodb[app.config.get('MONGODB_COLL', 'orcid_claims')]
        
    def process_payload(self, claim, **kwargs):
        """
        :param msg: contains the orcid claim with all
            information necessary for updating the
            database, mainly:
            
            {'bibcode': '....',
            'orcidid': '.....',
            'name': 'author name',
            'facts': 'author name variants',
            }
        :return: no return
        """
        
        assert(claim['bibcode'] and claim['orcidid'])
        bibcode = claim['bibcode']
        
        # retrieve authors (and bail if not available)
        authors = self.mongodb['authors'].find_one({'_id': bibcode})
        if not authors:
            raise Exception('{0} has no authors in the mongodb'.format(bibcode))
        
        # find existing claims (if any)
        orcid_claims = self.mongocoll.find_one({'_id': bibcode})
        if not orcid_claims:
            orcid_claims = {}
        
        # merge the two
        rec = {}
        rec.update(deepcopy(authors))
        rec.update(deepcopy(orcid_claims))
        
        
        # find the position and update
        idx = updater.update_record(rec, claim)
        if idx is not None and idx > -1:
            for x in ('verified', 'unverified'):
                if x in rec:
                    orcid_claims[x] = rec[x]
            if '_id' in orcid_claims:
                self.mongocoll.replace_one({'_id': bibcode}, orcid_claims)
            else:
                orcid_claims['_id'] = bibcode
                self.mongocoll.insert_one(orcid_claims)
            
            # save the claim in our own psql storage
            cl = dict(orcid_claims)
            del cl['_id']
            updater.record_claims(bibcode, cl)
            
            return True
        else:
            raise Exception('Unable to process: {0}'.format(claim))
        
        
        
class ErrorHandler(worker.RabbitMQWorker):
    def process_payload(self, msg):
        pass
    

