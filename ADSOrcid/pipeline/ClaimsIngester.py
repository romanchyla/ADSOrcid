

from .. import app
from . import GenericWorker
from .. import matcher
from .. import updater

class ClaimsIngester(GenericWorker.RabbitMQWorker):
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

        if msg.get('status', 'created') in ('unchanged', '#full-import'):
            return
                        
        author = matcher.retrieve_orcid(msg['orcidid'])
        
        if not author:
            raise Exception('Unable to retrieve info for {0}'.format(msg['orcidid']))
        
        # clean up the bicode
        bibcode = msg['bibcode'].strip()
        
        if not msg.get('bibcode_verified', False):
            if ' ' in bibcode:
                parts = bibcode.split()
                l = [len(x) for x in parts]
                if 19 in l:
                    bibcode = parts[l.index(19)] 
            
            # check if we can translate the bibcode/identifier
            rec = updater.retrieve_metadata(bibcode)
            if rec.get('bibcode') != bibcode:
                self.logger.warning('Resolving {0} into {1}'.format(bibcode, rec.get('bibcode')))
            bibcode = rec.get('bibcode') 
        
        msg['bibcode'] = bibcode
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
