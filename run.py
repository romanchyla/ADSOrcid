#!/usr/bin/env python
"""
"""

__author__ = 'rca'
__maintainer__ = 'rca'
__copyright__ = 'Copyright 2015'
__version__ = '1.0'
__email__ = 'ads@cfa.harvard.edu'
__status__ = 'Production'
__credit__ = ['J. Elliott']
__license__ = 'MIT'

import sys
import time
import pika
import argparse
import json
from ADSOrcid import app, importer
from ADSOrcid.pipeline.worker import RabbitMQWorker
from ADSOrcid.utils import setup_logging

logger = setup_logging(__file__, __name__)


def purge_queues(queues):
    """
    Purges the queues on the RabbitMQ instance of its content

    :param queues: queue name that needs to be purged
    :return: no return
    """

    publish_worker = RabbitMQWorker()
    publish_worker.connect(app.config.get('RABBITMQ_URL'))

    for worker, wconfig in app.config.get('WORKERS').iteritems():
            for x in ('publish', 'subscribe'):
                if x in wconfig and wconfig[x]:
                    try:
                        publish_worker.channel.queue_delete(queue=wconfig[x])
                    except pika.exceptions.ChannelClosed, e:
                        pass


def run_import(claims_file, queue='ads.orcid.claims', **kwargs):
    """
    Import claims from a file and inserts them into
    ads.orcid.claims queue
    
    :param: claims_file - path to the claims
    :type: str
    :param: queue - where to send the claims
    :type: str (ads.orcid.claims)
    
    :return: no return
    """

    logger.info('Loading records from: {0}'.format(claims_file))
    c = []
    importer.import_recs(claims_file, collector=c)
    
    if len(c):
        worker = RabbitMQWorker(params={
                            'publish': queue,
                            'exchange': app.config.get('EXCHANGE', 'ads-orcid')
                        })
        worker.connect(app.config.get('RABBITMQ_URL'))
        for claim in c:
            worker.publish(claim)
        
    logger.info('Done processing {0} claims.'.format(len(c)))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Process user input.')


    parser.add_argument('-q',
                        '--purge_queues',
                        dest='purge_queues',
                        action='store_true',
                        help='Purge all the queues so there are no remaining'
                             ' packets')

    parser.add_argument('-i',
                        '--import_claims',
                        dest='import_claims',
                        action='store',
                        type=str,
                        help='Path to the claims file to import')

    parser.set_defaults(purge_queues=False)
    args = parser.parse_args()
    
    app.init_app()
    
    if args.purge_queues:
        purge_queues(app.config.get('WORKERS'))
        sys.exit(0)

    if not args.import_claims:
        print 'You need to give the input list'
        parser.print_help()
        sys.exit(0)

    # Send the files to be put on the queue
    run_import(args.full_text_links)
