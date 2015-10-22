import os 
import sys
from .models import ClaimsLog
from . import app

import dateutil.parser
import datetime

def import_recs(input_file, default_provenance=None, default_status=None):
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
    """
    
    if not os.path.exists(input_file):
        raise Exception('{file} does not exist'.format(
                           file=input_file
                           ))
    
    def rec_builder(bibcode=None, orcidid=None, provenance=None, status=None, date=None):
        assert(bibcode and orcidid)
        return ClaimsLog(bibcode=bibcode, 
                      orcidid=orcidid,
                      provenance=provenance or default_provenance, 
                      status=status or default_status,
                      date=date and dateutil.parser.parse(date) or datetime.datetime.utcnow)
        
    i = 0
    with open(input_file) as fi:
        with app.session_scope() as session:
            for line in fi:
                l = line.strip()
                if len(l) == 0 or l[0] == '#':
                    continue
                parts = l.split('\t')
                try:
                    rec = rec_builder(*parts)
                    session.add(rec)
                except Exception, e:
                    app.logger.error('Error importing line %s (%s) - %s' % (i, l, e))
            session.commit()
