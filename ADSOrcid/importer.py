import os 
import sys
from .models import ClaimsLog, Records
from . import app
from .utils import get_date

import datetime

ALLOWED_STATUS = set(['claimed', 'updated', 'removed', 'unchanged', '#full-import'])


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
    with app.session_scope() as session:
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
        with app.session_scope() as session:
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
        with app.session_scope() as session:
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
