"""
This contains the classes required for each worker, which are all inheriting
from the RabbitMQ class.
"""



from .. import app
from . import worker
from .. import importer, matcher, updater



class ClaimsImporter(worker.RabbitMQWorker):
    """
    Checks if a claim exists in the remote ADSWS service.
    It then creates the claim and pushes it into the RabbitMQ pipeline.
    """
    def __init__(self, params=None):
        super(ClaimsImporter, self).__init__(params)
        app.init_app()
        
    def process_payload(self, msg, **kwargs):
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
        
        c = importer.insert_claims(msg)
        if c and len(c) > 0:
            for claim in c:
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
        msg['facts'] = author['facts']
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
    
    """
    def __init__(self, params=None):
        super(ClaimsIngester, self).__init__(params)
        app.init_app()
        
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
        
        # retrieve mongodb data (check authors are there)
        rec = {}
        
        # merge it with existing orcid claims (if any)
        
        
        # find the position and update
        if updater.update_record(rec, claim):
            # save the results back into mongo
            pass
        else:
            raise Exception('Unable to process: {0}'.format(claim))
        
        
        
class ErrorHandler(worker.RabbitMQWorker):
    def process_payload(self, msg):
        pass
    

