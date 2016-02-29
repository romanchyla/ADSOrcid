
"""
Library for updating papers (db claims/records). 
"""

import Levenshtein
from . import matcher
import app
import json
from .models import Records
from .utils import get_date
import datetime
from ADSOrcid.models import ClaimsLog
from sqlalchemy.sql.expression import and_
from types import NoneType
import requests

def record_claims(bibcode, claims):
    """
    Stores results of the processing in the database (this is purely
    for book-keeping purposes; and should happen after the data was
    written to the pipeline. However, in the future we can use these
    records to build the document for indexing
    
    :param: bibcode
    :type: string
    :param: claims, as stored in the mongo
    :type: dict
    """
    with app.session_scope() as session:
        if not isinstance(claims, basestring):
            claims = json.dumps(claims)
        r = session.query(Records).filter_by(bibcode=bibcode).first()
        if r is None:
            t = get_date()
            r = Records(bibcode=bibcode, claims=claims, 
                        created=t,
                        updated=t,
                        )
            session.add(r)
        else:
            r.updated = datetime.datetime.now()
            r.claims = claims
            session.merge(r)
        session.commit()
        
def mark_processed(bibcode):
    """Updates the date on which the record has been processed (i.e.
    something has consumed it
    
    :param: bibcode
    :type: str
    
    :return: None
    """
    
    with app.session_scope() as session:
        r = session.query(Records).filter_by(bibcode=bibcode).first()
        if r is None:
            raise Exception('Nonexistant record for {0}'.format(bibcode))
        r.processed = get_date()
        session.commit()
        return True        

def update_record(rec, claim):
    """
    update the ADS Document; we'll add ORCID information into it 
    (at the correct position)
    
    :param: rec - JSON structure, it contains metadata; we expect
            it to have 'author' field
    :param: claim - JSON structure, it contains claim data, 
            especially:
                orcidid
                author
                author_norm
            We use those field to find out which author made the
            claim.
    
    :return: None - it updates the `rec` directly
    """
    assert(isinstance(rec, dict))
    assert(isinstance(claim, dict))
    assert('authors' in rec)
    assert(isinstance(rec['authors'], list))
    
    fld_name = 'unverified'
    if 'account_id' in claim and claim['account_id']: # the claim was made by ADS verified user
        fld_name = 'verified'
    
    num_authors = len(rec['authors'])
    
    if fld_name not in rec or rec[fld_name] is None:
        rec[fld_name] = ['-'] * num_authors
    elif len(rec[fld_name]) < num_authors: # check the lenght is correct
        rec[fld_name] += ['-'] * (len(rec[fld_name]) - num_authors)
    
    # search using descending priority
    for fx in ('author', 'orcid_name', 'author_norm'):
        if fx in claim and claim[fx]:
            
            assert(isinstance(claim[fx], list))
            
            idx = find_orcid_position(rec['authors'], claim[fx])
            if idx > -1:
                rec[fld_name][idx] = claim.get('status', 'created') == 'removed' and '-' or claim['orcidid']
                return idx


def find_orcid_position(authors_list, name_variants):
    """
    Find the position of ORCID in the list of other strings
    
    :param authors_list - array of names that will be searched
    :param name_variants - array of names of a single author
    
    :return list of positions that match
    """
    al = [matcher.cleanup_name(x).lower().encode('utf8') for x in authors_list]
    nv = [matcher.cleanup_name(x).lower().encode('utf8') for x in name_variants]
    
    # compute similarity between all authors (and the supplied variants)
    # this is not very efficient, however the lists should be small
    # and short, so 3000 operations take less than 1s)
    res = []
    aidx = vidx = 0
    for variant in nv:
        aidx = 0
        for author in al:
            res.append((Levenshtein.ratio(author, variant), aidx, vidx))
            aidx += 1
        vidx += 1
        
    # sort results from the highest match
    res = sorted(res, key=lambda x: x[0], reverse=True)
    
    if len(res) == 0:
        return -1
    
    if res[0][0] < app.config.get('MIN_LEVENSHTEIN_RATIO', 0.9):
        app.logger.debug('No match found: the closest is: %s (required:%s)' \
                        % (res[0], app.config.get('MIN_LEVENSHTEIN_RATIO', 0.9)))
        return -1
    
    return res[0][1]


def _remove_orcid(rec, orcidid):
    """Finds and removes the orcidid from the list of claims.
    
    :return: True/False if the rec was modified
    """
    modified = False
    claims = rec.get('claims', {})
    for data in claims.values():
        if orcidid in data:
            data[data.index(orcidid)] = '-'
            modified = True
    return modified

def reindex_all_claims(orcidid, since=None):
    """
    Procedure that will re-discover and re-index all claims
    that were modified since a given starting point.
    """
    
    last_check = get_date(since or '1974-11-09T22:56:52.518001Z')
    recs_modified = set()
    
    with app.session_scope() as session:
        author = matcher.retrieve_orcid(orcidid)
        claimed = set()
        removed = set()
        for claim in session.query(ClaimsLog).filter(
                        and_(ClaimsLog.orcidid == orcidid, ClaimsLog.created > last_check)
                        ).all():
            if claim.status == 'claimed' or claim.status == 'updated':
                claimed.add(claim.bibcode)
            elif claim.status == 'removed':
                removed.add(claim.bibcode)
            
        with app.session_scope() as session:    
            for bibcode in removed:
                r = session.query(Records).filter_by(bibcode=bibcode).first()
                if r is None:
                    continue
                rec = r.toJSON()
                if _remove_orcid(rec, orcidid):
                    r.claims = json.dumps(rec.claims)
                    r.processed = get_date()
                    recs_modified.add(bibcode)
                    session.merge(r)
        
            for bibcode in claimed:
                r = session.query(Records).filter_by(bibcode=bibcode).first()
                if r is None:
                    continue
                rec = r.toJSON()
                modified = _remove_orcid(rec, orcidid) # always remove orcid, if any
                claim = {'bibcode': bibcode, 'orcidid': orcidid}
                claim.update(author.facts)
                idx = update_record(rec, claim)
                if idx > -1 or modified:
                    r.claims = json.dumps(rec.claims)
                    r.processed = get_date()
                    recs_modified.add(bibcode)
                    session.merge(r)
                    
            session.commit()
        
        return list(recs_modified)


def get_all_touched_profiles(since='1974-11-09T22:56:52.518001Z'):
    """Queries the orcid-service for all new/updated
    orcid profiles"""
    
    orcid_ids = set()
    latest_point = get_date(since) # RFC 3339 format
        
    while True:
        # increase the timestamp by one microsec and get new updates
        latest_point = latest_point + datetime.timedelta(microseconds=1)
        r = requests.get(app.config.get('API_ORCID_UPDATES_ENDPOINT') % latest_point.isoformat(),
                    params={'fields': ['orcid_id', 'updated', 'created']},
                    headers = {'Authorization': 'Bearer {0}'.format(app.config.get('API_TOKEN'))})
    
        if r.status_code != 200:
            raise Exception(r.text)
        
        if r.text.strip() == "":
            break
        
        # we received the data, immediately update the databaes (so that other processes don't 
        # ask for the same starting date)
        data = r.json()
        
        if len(data) == 0:
            break
        
        # data should be ordered by date update (but to be sure, let's check it); we'll save it
        # as latest 'check point'
        dates = [get_date(x['updated']) for x in data]
        dates = sorted(dates, reverse=True)
        latest_point = dates[0]
        for rec in data:
            orcid_ids.add(rec['orcid_id'])
        
    return list(orcid_ids)
            