

from .. import app
from . import GenericWorker
from ADSOrcid import updater


class ClaimsRecorder(GenericWorker.RabbitMQWorker):
    """
    Takes the claim, matches it in the database (will create
    entry for the record, if not existing yet) and updates 
    the metadata.
    """
    def __init__(self, params=None):
        super(ClaimsRecorder, self).__init__(params)
        app.init_app()
        
    def process_payload(self, claim, **kwargs):
        """
        :param claim: contains the message inside the packet
            {'bibcode': '....',
            'orcidid': '.....',
            'name': 'author name',
            'facts': 'author name variants',
            }
        :return: no return
        """
        
        if not isinstance(claim, dict):
            raise Exception('Received unknown payload {0}'.format(claim))
        
        if not claim.get('orcidid'):
            raise Exception('Unusable payload, missing orcidid {0}'.format(claim))

        bibcode = claim['bibcode']
        rec = updater.retrieve_record(bibcode)
        
        
        cl = updater.update_record(rec, claim)
        if cl:
            updater.record_claims(bibcode, rec['claims'], rec['authors'])
            self.publish({'authors': rec.get('authors'), 'bibcode': rec['bibcode'], 'claims': rec.get('claims')})
        else:
            self.logger.warning('Claim refused for bibcode:{0} and orcidid:{1}'
                            .format(claim['bibcode'], claim['orcidid']))
        
