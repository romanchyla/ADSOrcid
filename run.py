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
from ADSOrcid import app, importer, check_orcidids
from ADSOrcid.pipeline.GenericWorker import RabbitMQWorker
from ADSOrcid.pipeline import pstart
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

def run_recheck(queue='ads.orcid.reindex', **kwargs):
    """
    Compare state of the orcid-service, discover missing
    orcidids and reindex them
    
    :param: start_point - RFC3229 date formatted string
    :type: str
    :param: queue - where to send the claims
    :type: str (ads.orcid.reindex)
    
    :return: no return
    """
    check_orcidids.run()
    
    

def start_pipeline():
    """Starts the workers and let them do their job"""
    pstart.start_pipeline({}, app)
    

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
    
    parser.add_argument('-r',
                        '--recheck_database',
                        dest='recheck_database',
                        action='store_true',
                        help='Compare orcid-service and re-index missing')
    
    parser.add_argument('-p',
                        '--start_pipeline',
                        dest='start_pipeline',
                        action='store_true',
                        help='Start the pipeline')
    
    parser.set_defaults(purge_queues=False)
    parser.set_defaults(start_pipeline=False)
    parser.set_defaults(recheck_database=False)
    args = parser.parse_args()
    
    app.init_app()
    
    work_done = False
    if args.purge_queues:
        purge_queues(app.config.get('WORKERS'))

    if args.start_pipeline:
        start_pipeline()
        work_done = True
        
    if args.import_claims:
        # Send the files to be put on the queue
        run_import(args.import_claims)
        work_done = True
        
    if args.recheck_database:
        run_recheck(args)
        work_done = True
        
    if not work_done:
        parser.print_help()
        sys.exit(0)