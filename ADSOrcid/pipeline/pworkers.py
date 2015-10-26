"""
This contains the classes required for each worker, which are all inheriting
from the RabbitMQ class.
"""


import sys
import os
import pika
import json
import sys
import traceback

from .. import utils
from .. import app
from . import psettings
from . import worker
from .. import importer



class ClaimIngestWorker(worker.RabbitMQWorker):
    """
    Checks if a claim exists in the remote ADSWS service.
    """

    def process_payload(self, msg):
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
        d = msg.copy()
        d.setdefault('provenance', self.__class__.__name__)
        importer.upsert_claim(**d)


