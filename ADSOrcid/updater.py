"""
This library merges stuff that travels in the RabbitMQ (but here
we just deal with the logic; not with the queue) 
"""

import Levenshtein
from . import matcher
import app
import json
from .models import Records
import datetime


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
            t = datetime.datetime.utcnow()
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
        r.processed = datetime.datetime.utcnow()
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
    if 'accnt_id' in claim: # the claim was made by ADS verified user
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
    
    if res[0] < app.config.get('MIN_LEVENSHTEIN_RATIO', 0.9):
        app.logger.debug('No match found: the closest is: %s (required:%s)' \
                        % (res[0], app.config.get('MIN_LEVENSHTEIN_RATIO', 0.9)))
        return -1
    
    return res[0][1]