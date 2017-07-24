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
import argparse
import logging
import traceback
import requests
import warnings
from requests.packages.urllib3 import exceptions
warnings.simplefilter('ignore', exceptions.InsecurePlatformWarning)

from adsputils import setup_logging, get_date
from ADSOrcid import updater, tasks
from ADSOrcid.models import ClaimsLog, KeyValue, Records, AuthorInfo

app = tasks.app
logger = setup_logging('run.py')



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
    logging.captureWarnings(True)
    logger.info('Loading records from: {0}'.format(claims_file))
    c = []
    importer.import_recs(claims_file, collector=c)
    
    if len(c):
        for claim in c:
            tasks.task_ingest_claim.delay(claim)
        
    logger.info('Done processing {0} claims.'.format(len(c)))


def reindex_claims(since=None, orcid_ids=None, **kwargs):
    """
    Re-runs all claims, both from the pipeline and
    from the orcid-service storage.
    
    :param: since - RFC889 formatted string
    :type: str
    
    :return: no return
    """
    if orcid_ids:
        for oid in orcid_ids.split(','):
            tasks.task_index_orcid_profile.delay({'orcidid': oid, 'force': True})
        if not since:
            print 'Done (just the supplied orcidids)'
            return
        
    logging.captureWarnings(True)
    if not since or isinstance(since, basestring) and since.strip() == "":
        with app.session_scope() as session:
            kv = session.query(KeyValue).filter_by(key='last.reindex').first()
            if kv is not None:
                since = kv.value
            else:
                since = '1974-11-09T22:56:52.518001Z' 
    
    from_date = get_date(since)
    orcidids = set()
    
    
    logger.info('Loading records since: {0}'.format(from_date.isoformat()))
    
    # first re-check our own database (replay the logs)
    with app.session_scope() as session:
        for author in session.query(AuthorInfo.orcidid.distinct().label('orcidid')).all():
            orcidid = author.orcidid
            if orcidid and orcidid.strip() != "":
                try:
                    changed = updater.reindex_all_claims(orcidid, since=from_date.isoformat(), ignore_errors=True)
                    if len(changed):
                        orcidids.add(orcidid)
                    tasks.task_index_orcid_profile.delay({'orcidid': oid, 'force': True})
                except:
                    print 'Error processing: {0}'.format(orcidid)
                    traceback.print_exc()
                    continue
                if len(orcidids) % 100 == 0:
                    print 'Done replaying {0} profiles'.format(len(orcidids))
    
    print 'Now harvesting orcid profiles...'
    
    # then get all new/old orcidids from orcid-service
    all_orcids = set(updater.get_all_touched_profiles(from_date.isoformat()))
    orcidids = all_orcids.difference(orcidids)
    from_date = get_date()
    
      
    for orcidid in orcidids:
        try:
            tasks.task_index_orcid_profile.delay({'orcidid': oid, 'force': True})
        except: # potential backpressure (we are too fast)
            time.sleep(2)
            print 'Conn problem, retrying...', orcidid
            tasks.task_index_orcid_profile.delay({'orcidid': oid, 'force': True})
        
    with app.session_scope() as session:
        kv = session.query(KeyValue).filter_by(key='last.reindex').first()
        if kv is None:
            kv = KeyValue(key='last.reindex', value=from_date.isoformat())
            session.add(kv)
        else:
            kv.value = from_date.isoformat()
        session.commit()

    print 'Done'
    logger.info('Done submitting {0} orcid ids.'.format(len(orcidids)))


def repush_claims(since=None, orcid_ids=None, **kwargs):
    """
    Re-pushes all recs that were added since date 'X'
    to the output (i.e. forwards them onto the Solr queue)
    
    :param: since - RFC889 formatted string
    :type: str
    
    :return: no return
    """
    if orcid_ids:
        for oid in orcid_ids.split(','):
            tasks.task_index_orcid_profile({'orcidid': oid, 'force': False})
        if not since:
            print 'Done (just the supplied orcidids)'
            return
        
    logging.captureWarnings(True)
    if not since or isinstance(since, basestring) and since.strip() == "":
        with app.session_scope() as session:
            kv = session.query(KeyValue).filter_by(key='last.repush').first()
            if kv is not None:
                since = kv.value
            else:
                since = '1974-11-09T22:56:52.518001Z' 
    
    from_date = get_date(since)
    orcidids = set()
    
    logger.info('Re-pushing records since: {0}'.format(from_date.isoformat()))
    
    
    num_bibcodes = 0
    with app.session_scope() as session:
        for rec in session.query(Records) \
            .filter(Records.updated >= from_date) \
            .order_by(Records.updated.asc()) \
            .all():
            
            data = rec.toJSON()
            try:
                tasks.task_output_results.delay({'bibcode': data['bibcode'], 'authors': data['authors'], 'claims': data['claims']})
            except: # potential backpressure (we are too fast)
                time.sleep(2)
                print 'Conn problem, retrying ', data['bibcode']
                tasks.task_output_results.delay({'bibcode': data['bibcode'], 'authors': data['authors'], 'claims': data['claims']})
            num_bibcodes += 1
    
    with app.session_scope() as session:
        kv = session.query(KeyValue).filter_by(key='last.repush').first()
        if kv is None:
            kv = KeyValue(key='last.repush', value=get_date())
            session.add(kv)
        else:
            kv.value = get_date()
        session.commit()
        
    logger.info('Done processing {0} orcid ids.'.format(num_bibcodes))



def refetch_orcidids(since=None, orcid_ids=None, **kwargs):
    """
    Gets all orcidids that were updated since time X.
    
    :param: since - RFC889 formatted string
    :type: str
    
    :return: no return
    """
    if orcid_ids:
        for oid in orcid_ids.split(','):
            tasks.task_index_orcid_profile({'orcidid': oid, 'force': False})
        if not since:
            print 'Done (just the supplied orcidids)'
            return
    
    
    logging.captureWarnings(True)
    if not since or isinstance(since, basestring) and since.strip() == "":
        with app.session_scope() as session:
            kv = session.query(KeyValue).filter_by(key='last.refetch').first()
            if kv is not None:
                since = kv.value
            else:
                since = '1974-11-09T22:56:52.518001Z' 
    
    from_date = get_date(since)
    logger.info('Re-fetching orcidids updated since: {0}'.format(from_date.isoformat()))
    
        
    # then get all new/old orcidids from orcid-service
    orcidids = set(updater.get_all_touched_profiles(from_date.isoformat()))
    from_date = get_date()
    
      
    for orcidid in orcidids:
        try:
            tasks.task_index_orcid_profile({'orcidid': orcidid, 'force': False})
        except: # potential backpressure (we are too fast)
            time.sleep(2)
            print 'Conn problem, retrying...', orcidid
            tasks.task_index_orcid_profile.delay({'orcidid': orcidid, 'force': False})
        
    with app.session_scope() as session:
        kv = session.query(KeyValue).filter_by(key='last.refetch').first()
        if kv is None:
            kv = KeyValue(key='last.refetch', value=from_date.isoformat())
            session.add(kv)
        else:
            kv.value = from_date.isoformat()
        session.commit()

    print 'Done'
    logger.info('Done submitting {0} orcid ids.'.format(len(orcidids)))




def print_kvs():    
    """Prints the values stored in the KeyValue table."""
    print 'Key, Value from the storage:'
    print '-' * 80
    with app.session_scope() as session:
        for kv in session.query(KeyValue).order_by('key').all():
            print kv.key, kv.value


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Process user input.')


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
                        action='store_true',
                        help='Reindex claims')
    
    parser.add_argument('-u',
                        '--repush_claims',
                        dest='repush_claims',
                        action='store_true',
                        help='Re-push claims')
    
    parser.add_argument('-f',
                        '--refetch_orcidids',
                        dest='refetch_orcidids',
                        action='store_true',
                        help='Gets all orcidids changed since X (as discovered from ads api) and sends them to the queue.')
    
    parser.add_argument('-s', 
                        '--since', 
                        dest='since_date', 
                        action='store',
                        default=None,
                        help='Starting date for reindexing')
    
    parser.add_argument('-o', 
                        '--oid', 
                        dest='orcid_ids', 
                        action='store',
                        default=None,
                        help='Comma delimited list of orcid-ids to re-index (use with refetch orcidids)')
    
    parser.add_argument('-k', 
                        '--kv', 
                        dest='kv', 
                        action='store_true',
                        default=False,
                        help='Show current values of KV store')
    
    parser.set_defaults(purge_queues=False)
    parser.set_defaults(start_pipeline=False)
    args = parser.parse_args()
    
    
    work_done = False
    
    if args.kv:
        work_done = True
        print_kvs()

    if args.import_claims:
        # Send the files to be put on the queue
        run_import(args.import_claims)
        work_done = True
    
    if args.reindex_claims:
        reindex_claims(args.since_date, args.orcid_ids)
        work_done = True
    elif args.repush_claims:
        repush_claims(args.since_date, args.orcid_ids)
        work_done = True
    elif args.refetch_orcidids:
        refetch_orcidids(args.since_date, args.orcid_ids)
        work_done = True
    
    if not work_done:
        parser.print_help()
        sys.exit(0)