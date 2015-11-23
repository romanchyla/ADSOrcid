from . import GenericWorker
from .. import app 

class OutputHandler(GenericWorker.RabbitMQWorker):
    """
    This GenericWorker will forward results to the outside 
    exchange
    """
    
    def __init__(self, *args, **kwargs):
        super(OutputHandler, self).__init__(*args, **kwargs)

    
    def process_payload(self, claim, **kwargs):
        """
        :param msg: contains the orcid claim with all
            information necessary for updating the
            database, mainly:
            
            {'bibcode': '....',
             'authors': [....],
             'orcid_claims': {
                 'verified': [....],
                 'unverified': [...]
             }
            }
        :return: no return
        """
        
        # for now, we are receiving the data from MongoDB Updater
        # hence the recs were already updated, so all we have to do 
        # is to send a bibcode to the ADSimportpipeline
        
        self.forward([claim['bibcode']], topic='SolrUpdateRoute')
        
        