from ADSOrcid.models import ClaimsLog
from ADSOrcid import tasks
from collections import defaultdict

app = tasks.app

def run():
    stats = defaultdict(lambda: 0)
    authors = {}
    i = 0
    
    with app.session_scope() as session:
        for r in session.query(ClaimsLog).order_by(ClaimsLog.id.asc()).yield_per(1000):
            stats[r.status] += 1
            if r.orcidid and r.bibcode:
                if r.orcidid not in authors:
                    authors[r.orcidid] = {'claimed': 0, 'forced': 0, '#full-import': 0, 'updated': 0, 'removed': 0, 'unchanged': 0}
                authors[r.orcidid][r.status] += 1
            if i % 100000 == 0:
                print 'read ', i, 'rows'
            i += 1
    
    print 'read', i, 'rows'
    print stats
    print authors
if __name__ == '__main__':
    run()