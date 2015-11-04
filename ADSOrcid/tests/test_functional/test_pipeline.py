"""
Functional test

Loads the ADSOrcid workers. It then injects input onto the RabbitMQ instance. Once
processed it then checks all things were written where they should. 
It then shuts down all of the workers.
"""


import unittest
import time
from ADSOrcid.tests import test_base
from ADSOrcid.pipeline import pworkers, worker
from ADSOrcid import app, models

class TestPipeline(test_base.TestFunctional):
    """
    Class for testing the overall functionality of the ADSOrcid pipeline.
    The interaction between the pipeline workers.
    
    Make sure you have the correct values set in the local_config.py
    These tests will use that config.
    """

    def test_mongodb_worker(self):
        """Check we can write into the mongodb; for this test
        you have to have the 'db' container running: vagrant up db
        """
        
        worker = pworkers.MongoUpdater()
        
        # clean up
        worker.mongodb['authors'].remove({'_id': 'bibcode'})
        worker.mongodb[self.app.config.get('MONGODB_COLL', 'orcid_claims')].remove({'_id': 'bibcode'})
        
        # a test record
        worker.mongodb['authors'].insert({'_id': 'bibcode', 'authors': ['Huchra, J', 'Einstein, A', 'Neumann, John']})
        
        v = worker.process_payload({'bibcode': 'bibcode',
            'orcidid': 'foobar',
            'author_name': 'Neumann, John Von',
            'author': ['Neumann, John Von', 'Neumann, John V', 'Neumann, J V']
            })
        
        self.assertTrue(v)
        
        v = worker.mongodb[self.app.config.get('MONGODB_COLL', 'orcid_claims')].find_one({'_id': 'bibcode'})
        self.assertEquals(v['unverified'], [u'-', u'-', u'foobar'])
        
        v = worker.process_payload({'bibcode': 'bibcode',
            'orcidid': 'foobaz',
            'author_name': 'Huchra',
            'author': ['Huchra', 'Huchra, Jonathan']
            })
        v = worker.mongodb[self.app.config.get('MONGODB_COLL', 'orcid_claims')].find_one({'_id': 'bibcode'})
        self.assertEquals(v['unverified'], [u'foobaz', u'-', u'foobar'])


    def test_functionality_on_new_claim(self):
        """
        Main test, it pretends we have received claims from the 
        ADSWS
        
        For this, you need to have 'db' and 'rabbitmq' containers running.
        :return: no return
        """
        
        # clean the slate (production: 0000-0003-3041-2092, staging: 0000-0001-8178-9506) 
        with app.session_scope() as session:
            session.query(models.AuthorInfo).filter_by(orcidid='0000-0003-3041-2092').delete()
            session.query(models.ClaimsLog).filter_by(orcidid='0000-0003-3041-2092').delete()
            session.query(models.Records).filter_by(bibcode='2015ASPC..495..401C').delete()
            
        # setup/check the MongoDB has the proper data for authors
        mworker = pworkers.MongoUpdater()
        mworker.mongodb[self.app.config.get('MONGODB_COLL', 'orcid_claims')].remove({'_id': '2015ASPC..495..401C'})
        r = mworker.mongodb['authors'].find_one({'_id': '2015ASPC..495..401C'})
        if not r or 'authors' not in r:
            mworker.mongodb['authors'].insert({
                "_id" : "2015ASPC..495..401C",
                "authors" : [
                    "Chyla, R",
                    "Accomazzi, A",
                    "Holachek, A",
                    "Grant, C",
                    "Elliott, J",
                    "Henneken, E",
                    "Thompson, D",
                    "Kurtz, M",
                    "Murray, S",
                    "Sudilovsky, V"
                ]
            })

        
        
        
        test_worker = worker.RabbitMQWorker(params={
                            'publish': 'ads.orcid.fresh-claims',
                            'exchange': 'ads-orcid-test'
                        })
        test_worker.connect(self.TM.rabbitmq_url)
        
        # send a test claim
        test_worker.publish({'orcidid': '0000-0003-3041-2092', 'bibcode': '2015ASPC..495..401C'})
        
        time.sleep(2)
        
        # check results
        claim = mworker.mongodb[self.app.config.get('MONGODB_COLL', 'orcid_claims')].find_one({'_id': '2015ASPC..495..401C'})
        self.assertEquals(claim['unverified'],
                          ['0000-0003-3041-2092', '-','-','-','-','-','-','-','-','-', ] 
                          )

if __name__ == '__main__':
    unittest.main()