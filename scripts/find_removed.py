from ADSOrcid.models import ClaimsLog
from ADSOrcid import tasks
from collections import defaultdict
import sys

app = tasks.app

def run():
    offended_authors = []
    orcidids = set()
    
    # go through the rows in order and count those that had all rows removed
    with app.session_scope() as session:
        for r in session.query(ClaimsLog).distinct(ClaimsLog.orcidid).yield_per(100):
            orcidids.add(r.orcidid)
        
        print 'collected', len(orcidids), 'orcidids'
        
        j = 0
        for orcid in orcidids:
            j += 1
            i = removed = others = 0
            
            if j % 100 == 0:
                print 'processing', j, 'authors, found so far', len(offended_authors)

            for r in session.query(ClaimsLog).filter(ClaimsLog.orcidid == orcid).order_by(ClaimsLog.id.desc()).yield_per(1000):
                if r.status == '#full-import': # that concludes the batch
                    if removed == i and i > 0:
                        offended_authors.append(r.orcidid)
                    break
                
                if r.status == 'removed':
                    removed += 1
                else:
                    others += 1
                
                i += 1
    
    print 'found', len(offended_authors), 'instances of all-removed profiles'
    
    if 'submit' in sys.argv:
        for x in offended_authors:
            tasks.task_index_orcid_profile({'orcidid': x})


if __name__ == '__main__':
    run()