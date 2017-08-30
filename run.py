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
    app.import_recs(claims_file, collector=c)
    
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
        for oid in orcid_ids:
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
                    changed = updater.reindex_all_claims(app, orcidid, since=from_date.isoformat(), ignore_errors=True)
                    if len(changed):
                        orcidids.add(orcidid)
                    tasks.task_index_orcid_profile.delay({'orcidid': orcidid, 'force': True})
                except:
                    print 'Error processing: {0}'.format(orcidid)
                    traceback.print_exc()
                    continue
                if len(orcidids) % 100 == 0:
                    print 'Done replaying {0} profiles'.format(len(orcidids))
    
    print 'Now harvesting orcid profiles...'
    
    # then get all new/old orcidids from orcid-service
    all_orcids = set(updater.get_all_touched_profiles(app, from_date.isoformat()))
    orcidids = all_orcids.difference(orcidids)
    from_date = get_date()
    
      
    for orcidid in orcidids:
        try:
            tasks.task_index_orcid_profile.delay({'orcidid': orcidid, 'force': True})
        except: # potential backpressure (we are too fast)
            time.sleep(2)
            print 'Conn problem, retrying...', orcidid
            tasks.task_index_orcid_profile.delay({'orcidid': orcidid, 'force': True})
        
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
        for oid in orcid_ids:
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
        for oid in orcid_ids:
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
    orcidids = set(updater.get_all_touched_profiles(app, from_date.isoformat()))
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

def show_api_diagnostics(orcid_ids=None, bibcodes=None, ):
    """
    Prints various responses that we receive from our API.
    """
    
    print 'API_ENDPOINT', app.conf.get('API_ENDPOINT', None)
    print 'API_SOLR_QUERY_ENDPOINT', app.conf.get('API_SOLR_QUERY_ENDPOINT', None)
    print 'API_ORCID_EXPORT_PROFILE', app.conf.get('API_ORCID_EXPORT_PROFILE', None)
    print 'API_ORCID_UPDATES_ENDPOINT', app.conf.get('API_ORCID_UPDATES_ENDPOINT', None)
    
    
    if orcid_ids:
        for o in orcid_ids:
            print o
            print 'DB Model', app.retrieve_orcid(o)
            print '=' * 80 + '\n'
            print 'Author info', app.harvest_author_info(o)
            print '=' * 80 + '\n'
            print 'Public orcid profile', app.get_public_orcid_profile(o)
            print '=' * 80 + '\n'
            print 'ADS Orcid Profile', app.get_ads_orcid_profile(o)
            print '=' * 80 + '\n'
            print 'Harvested Author Info', app.retrieve_orcid(o)
            print '=' * 80 + '\n'
            orcid_present, updated, removed = app.get_claims(o,
                         app.conf.get('API_TOKEN'), 
                         app.conf.get('API_ORCID_EXPORT_PROFILE') % o,
                         orcid_identifiers_order=app.conf.get('ORCID_IDENTIFIERS_ORDER', {'bibcode': 9, '*': -1})
                         )
            print 'All of orcid', len(orcid_present), orcid_present
            print 'In need of update', len(updated), updated
            print 'In need of removal', len(removed), removed
            print '=' * 80 + '\n'
    else:
        print 'If you want to see what I see for authors, give me some orcid ids'
        
    if bibcodes:
        for b in bibcodes:
            print b, app.retrieve_metadata(b)
    else:
        print 'If you want to see what I see give me some bibcodes'
    
    
    if orcid_ids:
        print '=' * 80 + '\n'
        print 'Now submitting ORCiD for processing'
        for o in orcid_ids:
            m = {'orcidid': o}
            print 'message=%s, taskid=%s' % (m, tasks.task_index_orcid_profile.delay(m))

    if orcid_ids and bibcodes:
        print '=' * 80 + '\n'
        print 'Now submitting claims (orcid:bibcode) for processing\n' + \
              '(pairs are autogenerated; invalid claims should be rejected)'
        for o in orcid_ids:
            for b in bibcodes:
                m = {'bibcode': b, 'orcidid': o, 'provenance': 'run.py'}
                print 'message=%s, taskid=%s' % (m, tasks.task_ingest_claim.delay(m)) 


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Process user input.')


    parser.add_argument('-i',
                        '--import_claims',
                        dest='import_claims',
                        action='store',
                        type=str,
                        help='Path to the claims file to import')
    
    
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
    
    parser.add_argument('-b', 
                        '--bibcodes', 
                        dest='bibcodes', 
                        action='store',
                        default=None,
                        help='Comma delimited list of bibcodes (for diagnostics)')
    
    parser.add_argument('-k', 
                        '--kv', 
                        dest='kv', 
                        action='store_true',
                        default=False,
                        help='Show current values of KV store')
    
    parser.add_argument('-d', 
                        '--diagnose', 
                        dest='diagnose', 
                        action='store_true',
                        default=False,
                        help='Show me what you would do with ORCiDs/bibcodes')
    
    args = parser.parse_args()
    if args.orcid_ids:
        args.orcid_ids = [x.strip() for x in args.orcid_ids.split(',')]
    if args.bibcodes:
        args.bibcodes = [x.strip() for x in args.bibcodes.split(',')]
    
    
    if args.kv:
        print_kvs()
        
    if args.diagnose:
        show_api_diagnostics(args.orcid_ids or ['0000-0003-3041-2092'], args.bibcodes or ['2015arXiv150305881C'])

    if args.import_claims:
        # Send the files to be put on the queue
        run_import(args.import_claims)
    
    if args.reindex_claims:
        reindex_claims(args.since_date, args.orcid_ids)
    elif args.repush_claims:
        repush_claims(args.since_date, args.orcid_ids)
    elif args.refetch_orcidids:
        refetch_orcidids(args.since_date, args.orcid_ids)
    
