"""
This library merges stuff that travels in the RabbitMQ (but here
we just deal with the logic; not with the queue) 
"""

import Levenshtein
from . import matcher
import app

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
    """
    assert(isinstance(rec, dict))
    assert(isinstance(claim, dict))
    assert('author' in rec)
    
    fld_name = 'orcid_unverified'
    if 'accnt_id' in claim: # the claim was made by ADS verified user
        fld_name = 'orcid_verified'
    
    num_authors = len(rec['author'])
    
    if fld_name not in rec:
        rec[fld_name] = ['-'] * num_authors
    
    # search using descending priority
    for fx in ('author', 'orcid_name', 'author_norm'):
        if fx in rec:
            idx = find_orcid_position(rec['author'], claim[fx])
            if idx > -1:
                rec[fld_name][idx] = claim['orcidid'] 


def find_orcid_position(authors_list, name_variants):
    """
    Find the position of ORCID in the list of other strings
    
    :param authors_list - array of names that will be searched
    :param name_variants - array of names of a single author
    
    :return list of positions that match
    """
    al = [matcher.cleanup_name(x) for x in authors_list]
    nv = [matcher.cleanup_name(x) for x in name_variants]
    
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