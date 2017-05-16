import os 
import sys
from .models import ClaimsLog, Records
from . import db, app, matcher, updater
from .utils import get_date, setup_logging
from dateutil.tz import tzutc
from sqlalchemy import and_

import datetime
import requests
import traceback
import json
import random
import time

ALLOWED_STATUS = set(['claimed', 'updated', 'removed', 'unchanged', 'forced', '#full-import'])
logger = setup_logging('importer', 'importer')

"""
Set of utilities for creating claims.
"""


def insert_claims(claims):
    """
    Build a batch of claims and saves them into a database
    
    :param: claims - list of json values, with claims
                   - or list of claims (ClaimLog) instances
    :return number of claims that were successfuly added
            to the database
    """
    res = []
    with db.session_scope() as session:
        for c in claims:
            if isinstance(c, ClaimsLog):
                claim = c
            else:
                claim = create_claim(**c)
            if claim:
                session.add(claim)
                res.append(claim)
        session.commit()
        res = [x.toJSON() for x in res]
    return res

def create_claim(bibcode=None, 
                 orcidid=None, 
                 provenance=None, 
                 status=None, 
                 date=None, 
                 force_new=True,
                 **kwargs):
    """
    Inserts (or updates) ClaimLog entry.
    
    :return: ClaimsLog instance (however this is only for reading, you should
        not try to do anything with it; the session will have been closed already)
    """
    assert(orcidid)
    if isinstance(date, basestring):
        date = get_date(date)
    if status and status.lower() not in ALLOWED_STATUS:
        raise Exception('Unknown status %s' % status)
    
    if not date or force_new is True: # we don't need to verify the record exists
        return ClaimsLog(bibcode=bibcode, 
                  orcidid=orcidid,
                  provenance=provenance, 
                  status=status,
                  created=date or get_date())
    else:
        with db.session_scope() as session:
            f = session.query(ClaimsLog).filter_by(created=date).first()
            if f and f.bibcode == bibcode and f.orcidid == orcidid:
                f.provenance = provenance
                f.status = status
            else:
                return ClaimsLog(bibcode=bibcode, 
                  orcidid=orcidid,
                  provenance=provenance, 
                  status=status,
                  created=date)
                
                     

def import_recs(input_file, default_provenance=None, 
                default_status='claimed', collector=None):
    """
    Imports (creates log records) of claims from
    :param: input_file - String, path to the file with the following 
            information (tab delimited):
                bibcode
                orcid_id
                provenance - optional
                status - optional
                date - optional
    :param: default_provenance - String, this will be used if the records
            don't provide provenance
    :param: default_status - String, used when status is not supplied
    :param: collector - if passed in, the results will be inserted
            into it
    :type: array
    """
    
    if not os.path.exists(input_file):
        raise Exception('{file} does not exist'.format(
                           file=input_file
                           ))
    if collector is not None:
        assert(isinstance(collector, list))
        
    if default_provenance is None:
        default_provenance = os.path.abspath(input_file)
        
    def rec_builder(bibcode=None, orcidid=None, provenance=None, status=None, date=None):
        assert(bibcode and orcidid)
        return ClaimsLog(bibcode=bibcode, 
                      orcidid=orcidid,
                      provenance=provenance or default_provenance, 
                      status=status or default_status,
                      created=date and get_date(date) or get_date())
        
    i = 0
    with open(input_file, 'r') as fi:
        with db.session_scope() as session:
            for line in fi:
                i += 1
                l = line.strip()
                if len(l) == 0 or l[0] == '#':
                    continue
                parts = l.split('\t')
                try:
                    rec = rec_builder(*parts)
                    session.add(rec)
                    if collector is not None:
                        collector.append(rec.toJSON())
                except Exception, e:
                    app.logger.error('Error importing line %s (%s) - %s' % (i, l, e))
                if i % 1000 == 0:
                    session.commit()
            session.commit()


def _get_ads_orcid_profile(orcidid, api_token, api_url):
    r = requests.get(api_url,
             params={'reload': True},
             headers={'Accept': 'application/json', 'Authorization': 'Bearer:%s' % api_token})
    if r.status_code == 200:
        return r.json()
    else:
        logger.warning('Missing profile for: {0}'.format(orcidid))
        logger.warning(r.text)
        return {}

def get_claims(orcidid, api_token, api_url, force=False, 
                  orcid_identifiers_order=None):
    """
    Fetch a fresh profile from the orcid-service and compare
    it against the state of the storage (diff). Return the docs
    that need updating, and removal
    

    :param orcidid
        - string, orcid identifier
    :param: api_token
        - string, OAuth token to access ADS API
    :param: api_url:
        - string, URL for getting the orcid profiles
    :param: force
        - bool, when True it forces claims to be counted
            as new (even if we have already indexed them)
    :param: orcid_identifiers_order
        - dict, helps to sort claims by their identifies.
            (e.g. to say that bibcodes have higher priority than
            dois)
    :return: 
        - updated: dict of bibcodes that were updated
            - keys are lowercased bibcodes
            - values are (bibcode, timestamp)
        - removed: set of bibcodes that were removed
            - keys are lowercased bibcodes
            - values are (bibcode, timestamp)
    """
    
    
    

    # make sure the author is there (even if without documents) 
    author = matcher.retrieve_orcid(orcidid) # @UnusedVariable
    data = _get_ads_orcid_profile(orcidid, api_token, api_url)
    
    if data is None:
        return #TODO: remove all existing claims?
    
    profile = data.get('profile', {})
    if not profile:
        return #TODO: remove all existing claims?
    

    with db.session_scope() as session:
          
        # orcid is THE ugliest datastructure of today!
        try:
            works = profile['orcid-profile']['orcid-activities']['orcid-works']['orcid-work']
        except:
            logger.warning('Nothing to do for: '
                '{0} ({1})'.format(orcidid,
                                   traceback.format_exc()))
            return

        # check we haven't seen this very profile already
        try:
            updt = str(profile['orcid-profile']['orcid-history']['last-modified-date']['value'])
            updt = float('%s.%s' % (updt[0:10], updt[10:]))
            updt = datetime.datetime.fromtimestamp(updt, tzutc())
            updt = get_date(updt.isoformat())
        except KeyError:
            updt = get_date()
                                
        # find the most recent #full-import record
        last_update = session.query(ClaimsLog).filter(
            and_(ClaimsLog.status == '#full-import', ClaimsLog.orcidid == orcidid)
            ).order_by(ClaimsLog.id.desc()).first()
            
        if last_update is None:
            q = session.query(ClaimsLog).filter_by(orcidid=orcidid).order_by(ClaimsLog.id.asc())
        else:
            if get_date(last_update.created) == updt:
                if force:
                    logger.info("Profile {0} unchanged, but forced update in effect.".format(orcidid))
                else:
                    logger.info("Skipping {0} (profile unchanged)".format(orcidid))
                    return
            q = session.query(ClaimsLog).filter(
                and_(ClaimsLog.orcidid == orcidid, ClaimsLog.id > last_update.id)) \
                .order_by(ClaimsLog.id.asc())
                    
        
        # now get info about each record #TODO: enhance the matching (and refactor)
        # we'll try to match identifiers against our own API; if a document is found
        # it will be added to the `orcid_present` with corresponding timestamp (cdate)
        orcid_present = {}
        for w in works:
            bibc = None
            try:
                ids =  w['work-external-identifiers']['work-external-identifier']
                seek_ids = []
                
                # painstakingly check ids (start from a bibcode) if we can find it
                # we'll send it through (but start from bibcodes, then dois, arxiv...)
                fmap = orcid_identifiers_order
                for x in ids:
                    xtype = x.get('work-external-identifier-type', None)
                    if xtype:
                        seek_ids.append((fmap.get(xtype.lower().strip(), fmap.get('*', -1)), 
                                         x['work-external-identifier-id']['value']))
                
                if len(seek_ids) == 0:
                    continue
                
                seek_ids = sorted(seek_ids, key=lambda x: x[0], reverse=True)
                for _priority, fvalue in seek_ids:
                    try:
                        time.sleep(1.0/random.randint(1, 10)) # be nice to the api
                        metadata = updater.retrieve_metadata(fvalue, search_identifiers=True)
                        if metadata and metadata.get('bibcode', None):
                            bibc = metadata.get('bibcode')
                            logger.info('Match found {0} -> {1}'.format(fvalue, bibc))
                            break
                    except Exception, e:
                        logger.error('Exception while searching for matching bibcode for: {}'.format(fvalue))
                        logger.warning(e.message)
                        
                
                if bibc:
                    # would you believe that orcid doesn't return floats?
                    ts = str(w['last-modified-date']['value'])
                    ts = float('%s.%s' % (ts[0:10], ts[10:]))
                    ts = datetime.datetime.fromtimestamp(ts, tzutc())
                    try:
                        provenance = w['source']['source-name']['value']
                    except KeyError:
                        provenance = 'orcid-profile'
                    orcid_present[bibc.lower().strip()] = (bibc.strip(), get_date(ts.isoformat()), provenance)
                else:
                    logger.warning('Found no bibcode for: {orcidid}. {ids}'.format(ids=json.dumps(ids), orcidid=orcidid))
                    
            except KeyError, e:
                logger.warning('Error processing a record: '
                    '{0} ({1})'.format(w,
                                       traceback.format_exc()))
                continue
            except TypeError, e:
                logger.warning('Error processing a record: '
                    '{0} ({1})'.format(w,
                                       traceback.format_exc()))
                continue

        
        # find all records we have processed at some point
        updated = {}
        removed = {}
        
        for cl in q.all():
            if not cl.bibcode:
                continue
            bibc = cl.bibcode.lower()
            if cl.status == 'removed':
                removed[bibc] = (cl.bibcode, get_date(cl.created))
                if bibc in updated:
                    del updated[bibc]
            else: #elif cl.status in ('claimed', 'updated', 'forced', 'unchanged'):
                updated[bibc] = (cl.bibcode, get_date(cl.created))
                if bibc in removed:
                    del removed[bibc]
        
        return orcid_present, updated, removed