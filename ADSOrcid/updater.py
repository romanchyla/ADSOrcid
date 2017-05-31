
"""
Library for updating papers (db claims/records). 
"""

import Levenshtein
import json
from .utils import get_date
from ADSOrcid.models import ClaimsLog, Records
from ADSOrcid import utils, names
from sqlalchemy.sql.expression import and_
import requests
from datetime import timedelta


logger = utils.setup_logging('updater', 'updater')     


def update_record(rec, claim):
    """
    update the ADS Record; we'll add ORCID information into it 
    (at the correct position)
    
    :param: rec - JSON structure, it contains metadata; we expect
            it to have 'authors' field, and 'claims' field
            
    :param: claim - JSON structure, it contains claim data, 
            especially:
                orcidid
                author
                author_norm
            We use those field to find out which author made the
            claim.
    
    :return: tuple(clain_category, position) or None if no record
        was updated
    """
    assert(isinstance(rec, dict))
    assert(isinstance(claim, dict))
    assert('authors' in rec)
    assert('claims' in rec)
    assert(isinstance(rec['authors'], list))
    
    claims = rec.get('claims', {})
    rec['claims'] = claims
    authors = rec.get('authors', [])
    
    # make sure the claims have the necessary structure
    fld_name = u'unverified'
    if 'account_id' in claim and claim['account_id']: # the claim was made by ADS verified user
        fld_name = u'verified'
    
    num_authors = len(authors)
    
    if fld_name not in claims or claims[fld_name] is None:
        claims[fld_name] = ['-'] * num_authors
    elif len(claims[fld_name]) < num_authors: # check the lenght is correct
        claims[fld_name] += ['-'] * (num_authors - len(claims[fld_name]))
    elif len(claims[fld_name]) > num_authors:
        claims[fld_name] = claims[fld_name][0:num_authors]

    # always remove the orcidid
    modified = False    
    orcidid = claim['orcidid']
    for v in claims.values():
        while orcidid in v:
            v[v.index(orcidid)] = '-'
            modified = True
            
    # search using descending priority
    for fx in ('author', 'orcid_name', 'author_norm', 'short_name'):
        if fx in claim and claim[fx]:
            
            assert(isinstance(claim[fx], list))
            idx = find_orcid_position(rec['authors'], claim[fx])
            if idx > -1:              
                if idx >= num_authors:
                    logger.error(u'Index is beyond list boundary: \n' + 
                                     u'Field {fx}, author {author}, len(authors)={la}, len({fx})=lfx'
                                     .format(
                                       fx=fx, author=claim[fx], la=num_authors, lfx=len(claim[fx])
                                       )
                                     )
                    continue
                
                claims[fld_name][idx] = claim.get('status', 'created') == 'removed' and '-' or orcidid
                return (fld_name, idx)
    
    if modified:
        return ('removed', -1)

def find_orcid_position(authors_list, name_variants,
                        min_levenshtein=0.9):
    """
    Find the position of ORCID in the list of other strings
    
    :param authors_list - array of names that will be searched
    :param name_variants - array of names of a single author
    
    :return list of positions that match
    """
    al = [names.cleanup_name(x).lower().encode('utf8') for x in authors_list]
    nv = [names.cleanup_name(x).lower().encode('utf8') for x in name_variants]
    
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
    
    if res[0][0] < min_levenshtein:
        # test submatch (0.6470588235294118, 19, 0) (required:0.69) closest: vernetto, s, variant: vernetto, silvia teresa
        author_name = al[res[0][1]]
        variant_name = nv[res[0][2]]
        if author_name in variant_name or variant_name in author_name:
            logger.debug(u'Using submatch for: %s (required:%s) closest: %s, variant: %s' \
                        % (res[0], min_levenshtein, 
                           unicode(author_name, 'utf-8'), 
                           unicode(variant_name, 'utf-8')))
            return res[0][1]
            
        logger.debug(u'No match found: the closest is: %s (required:%s) closest: %s, variant: %s' \
                        % (res[0], min_levenshtein, 
                           unicode(author_name, 'utf-8'), 
                           unicode(variant_name, 'utf-8')))
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

def reindex_all_claims(app, orcidid, since=None, ignore_errors=False):
    """
    Procedure that will re-play all claims
    that were modified since a given starting point.
    """
    
    last_check = get_date(since or '1974-11-09T22:56:52.518001Z')
    recs_modified = set()
    
    with app.session_scope() as session:
        author = app.retrieve_orcid(orcidid)
        claimed = set()
        removed = set()
        for claim in session.query(ClaimsLog).filter(
                        and_(ClaimsLog.orcidid == orcidid, ClaimsLog.created > last_check)
                        ).all():
            if claim.status in ('claimed', 'updated', 'forced'):
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
                    r.claims = json.dumps(rec.get('claims'))
                    r.updated = get_date()
                    recs_modified.add(bibcode)
        
            for bibcode in claimed:
                r = session.query(Records).filter_by(bibcode=bibcode).first()
                if r is None:
                    continue
                rec = r.toJSON()

                claim = {'bibcode': bibcode, 'orcidid': orcidid}
                claim.update(author.get('facts', {}))
                try:
                    _claims = update_record(rec, claim)
                    if _claims:
                        r.claims = json.dumps(rec.get('claims', {}))
                        r.updated = get_date()
                        recs_modified.add(bibcode)
                except Exception, e:
                    if ignore_errors:
                        app.logger.error(u'Error processing {0} {1}'.format(bibcode, orcidid))
                    else:
                        raise e
                    
                    
            session.commit()
        
        return list(recs_modified)


def get_all_touched_profiles(app, since='1974-11-09T22:56:52.518001Z'):
    """Queries the orcid-service for all new/updated
    orcid profiles"""
    
    orcid_ids = set()
    latest_point = get_date(since) # RFC 3339 format
        
    while True:
        # increase the timestamp by one microsec and get new updates
        latest_point = latest_point + timedelta(microseconds=1)
        r = requests.get(app.conf.get('API_ORCID_UPDATES_ENDPOINT') % latest_point.isoformat(),
                    params={'fields': ['orcid_id', 'updated', 'created']},
                    headers = {'Authorization': 'Bearer {0}'.format(app.conf.get('API_TOKEN'))})
    
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
            