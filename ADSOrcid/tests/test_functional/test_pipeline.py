"""
Functional test

Loads the ADSOrcid workers. It then injects input onto the RabbitMQ instance. Once
processed it then checks all things were written where they should. 
It then shuts down all of the workers.
"""


import unittest
from ADSOrcid.tests import test_base
from ADSOrcid.pipeline import pworkers

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
        worker.mongodb['authors'].insert({'_id': 'bibcode', 'author': ['Huchra, J', 'Einstein, A', 'Neumann, John']})
        
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

    def xtest_functionality_on_new_claim(self):
        """
        Main test, it pretends we have received claims from the 
        ADSWS

        :return: no return
        """

        pass

if __name__ == '__main__':
    unittest.main()