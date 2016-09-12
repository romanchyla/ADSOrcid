from . import GenericWorker
from .. import app 
from copy import deepcopy
from .exceptions import ProcessingException


class OutputHandler(GenericWorker.RabbitMQWorker):
    """
    This GenericWorker will forward results to the outside 
    exchange and also it will update MongoDB (that is a HACK
    to force the external queue to pick metadata that we
    updated)
    """
    
    def __init__(self, *args, **kwargs):
        super(OutputHandler, self).__init__(*args, **kwargs)
        app.init_app()
        self.init_mongo()
        
    def init_mongo(self):
        from pymongo import MongoClient
        self.mongo = MongoClient(app.config.get('MONGODB_URL'))
        self.mongodb = self.mongo[app.config.get('MONGODB_DB', 'adsdata')]
        # stupid mongo will not tell us if we have access, so let's fire/fail
        self.mongodb.collection_names()
        
    
    def process_payload(self, claim, **kwargs):
        """
        :param msg: contains the orcid claim with all
            information necessary for updating the
            database, mainly:
            
            {'bibcode': '....',
             'authors': [....],
             'claims': {
                 'verified': [....],
                 'unverified': [...]
             }
            }
        :return: no return
        """
        
        self._update_mongo(claim)
        # notify solr that it needs to re-index this bibcode
        self.forward([claim['bibcode']], topic='SolrUpdateRoute')
        
    def _update_mongo(self, claim):
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
        
        assert(claim['bibcode'] and claim['claims'] and claim['authors'])
        bibcode = claim['bibcode']
        
        # retrieve authors (and bail if not available)
        authors = self.mongodb[app.config.get('MONGODB_AUTHORS', 'authors')].find_one({'_id': bibcode})
        if not authors:
            raise ProcessingException('{0} has no authors in the mongodb'.format(bibcode))
        
        if claim['authors'] != authors:
            if self._authors_differ(authors, claim['authors']):
                self.logger.warning('The authors as retrieved from MongoDB differ!. {0} : {1}'
                                .format(claim['authors'], authors))
        
        # find existing claims (if any)
        mongocoll = self.mongodb[app.config.get('MONGODB_COLL', 'orcid_claims')]
        orcid_claims = mongocoll.find_one({'_id': bibcode})
        if not orcid_claims:
            cl = deepcopy(claim['claims'])
            cl['_id'] = claim['bibcode']
            mongocoll.insert_one(cl)
        else:
            mongocoll.replace_one({'_id': bibcode}, claim['claims'])
            
    
    def _normalize(self, v):
        out = []
        for x in v:
            if x.isalpha():
                out.append(x.lower())
        return u''.join(out)
    
    def _authors_differ(self, canonical, claim):
        if len(canonical) != len(claim):
            return True
        normalize = self._normalize
        for v, w in zip(canonical, claim):
            v = normalize(v)
            w = normalize(w)
            if len(v) > len(w):
                v, w = w, v
            if v not in w:
                return True
        return False
            