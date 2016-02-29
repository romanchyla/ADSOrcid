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
from ADSOrcid import app, importer, updater
from ADSOrcid.pipeline import GenericWorker
from ADSOrcid.pipeline import pstart
from ADSOrcid.utils import setup_logging, get_date
from ADSOrcid.models import ClaimsLog

logger = setup_logging(__file__, __name__)
RabbitMQWorker = GenericWorker.RabbitMQWorker

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


def reindex_claims(since=None, **kwargs):
    """
    Re-runs all claims, both from the pipeline and
    from the orcid-service storage.
    
    :param: since - RFC889 formatted string
    :type: str
    
    :return: no return
    """
    if not since or isinstance(since, basestring) and since.strip() == "":
        since = '1974-11-09T22:56:52.518001Z' 
    
    from_date = get_date(since)
    orcidids = set()
    
    logger.info('Loading records since: {0}'.format(from_date.isoformat()))
    
    # first re-check our own database (replay the logs)
    with app.session_scope() as session:
        for claim in session.query(ClaimsLog.orcidid.distinct().label('orcidid')).all():
            orcidid = claim.orcidid
            if orcidid and orcidid.strip() != "":
                changed = updater.reindex_all_claims(orcidid, since=from_date.isoformat())
                if len(changed):
                    orcidids.add(orcidid)
    
    # then get all new/old orcidids from orcid-service
    orcidids = orcidids.union(updater.get_all_touched_profiles(from_date.isoformat()))
    
    # trigger re-indexing
    worker = RabbitMQWorker(params={
        'publish': 'ads.orcid.fresh-claims',
        'exchange': app.config.get('EXCHANGE', 'ads-orcid')
    })
    worker.connect(app.config.get('RABBITMQ_URL'))  
    for orcidid in orcidids:
        worker.publish({'orcidid': orcidid})

    logger.info('Done processing {0} orcid ids.'.format(len(orcidids)))

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
    
    parser.add_argument('-p',
                        '--start_pipeline',
                        dest='start_pipeline',
                        action='store_true',
                        help='Start the pipeline')
    
    parser.add_argument('-r',
                        '--reindex_claims',
                        dest='reindex_claims',
                        action='store',
                        help='Reindex claims [since]')
    
    parser.set_defaults(purge_queues=False)
    parser.set_defaults(start_pipeline=False)
    args = parser.parse_args()
    
    app.init_app()
    
    work_done = False
    if args.purge_queues:
        purge_queues(app.config.get('WORKERS'))
        sys.exit(0)

    if args.start_pipeline:
        start_pipeline()
        work_done = True
        
    if args.import_claims:
        # Send the files to be put on the queue
        run_import(args.import_claims)
        work_done = True
    
    if args.reindex_claims:
        reindex_claims(args.reindex_claims)
        work_done = True
    
    if not work_done:
        parser.print_help()
        sys.exit(0)