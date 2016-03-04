

from .. import app
from . import GenericWorker
from .. import matcher
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
        
    def process_payload(self, msg, **kwargs):
        """
        :param msg: contains the message inside the packet
            {'bibcode': '....',
            'orcidid': '.....',
            'name': 'author name',
            'facts': 'author name variants',
            }
        :return: no return
        """
        
        if not isinstance(msg, dict):
            raise Exception('Received unknown payload {0}'.format(msg))
        
        if not msg.get('orcidid'):
            raise Exception('Unusable payload, missing orcidid {0}'.format(msg))

        record = updater.retrieve_metadata(msg['bibcode'])
        
