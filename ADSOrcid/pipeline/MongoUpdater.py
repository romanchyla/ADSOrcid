from . import GenericWorker
from .. import app
from .. import updater
from copy import deepcopy

class MongoUpdater(GenericWorker.RabbitMQWorker):
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
        # stupid mongo will not tell us if we have access, so let's fire/fail
        self.mongodb.collection_names()
        
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
            
            # publish results to the queue
            orcid_claims['bibcode'] = bibcode
            del orcid_claims['_id']
            self.publish(orcid_claims)
            
            return True
        else:
            raise Exception('Unable to process: {0}'.format(claim))
        
        


