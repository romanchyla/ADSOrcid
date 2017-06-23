import sys
import os
import json

from mock import patch, PropertyMock
import unittest
import adsputils as utils
from adsmsg import OrcidClaims
from ADSOrcid import app
from ADSOrcid import tasks
from ADSOrcid.models import Base


class TestWorkers(unittest.TestCase):
    
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.proj_home = os.path.join(os.path.dirname(__file__), '../..')
        self._app = tasks.app
        self.app = app.ADSOrcidCelery('test', local_config=\
            {
            'SQLALCHEMY_URL': 'sqlite:///',
            'SQLALCHEMY_ECHO': False
            })
        tasks.app = self.app # monkey-path the app object
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
    
    
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()
        tasks.app = self._app


    def test_task_index_orcid_profile(self):
        
        with patch.object(self.app, 'get_claims') as get_claims, \
            patch.object(self.app, 'insert_claims') as insert_claims, \
            patch.object(tasks.task_index_orcid_profile, 'apply_async') as task_index_orcid_profile, \
            patch.object(tasks.task_ingest_claim, 'delay') as next_task:
            
            self.assertFalse(next_task.called)
            
            get_claims.return_value = (
                {
                 'bibcode1': ('Bibcode1', utils.get_date('2017-01-01'), 'provenance'),
                 'bibcode2': ('Bibcode2', utils.get_date('2017-01-01'), 'provenance'),
                 'bibcode3': ('Bibcode3', utils.get_date('2017-01-01'), 'provenance'),
                },
                {
                 'bibcode1': ('Bibcode1', utils.get_date('2017-01-01')),
                 'bibcode4': ('Bibcode4', utils.get_date('2017-01-01')), # we have, but orcid no more
                 },
                {
                 'bibcode2': ('Bibcode2', utils.get_date('2017-01-01')),
                 })
            insert_claims.return_value = [
                {'status': u'#full-import', 'bibcode': u'', 'created': '2017-05-26T21:29:22.726506+00:00', 'provenance': u'OrcidImporter', 'orcidid': '0000-0003-3041-2092', 'id': None},
                {'status': u'claimed', 'bibcode': 'Bibcode2', 'created': '2017-01-01T00:00:00+00:00', 'provenance': u'provenance', 'orcidid': '0000-0003-3041-2092', 'id': None},
                {'status': u'claimed', 'bibcode': 'Bibcode3', 'created': '2017-01-01T00:00:00+00:00', 'provenance': u'provenance', 'orcidid': '0000-0003-3041-2092', 'id': None},
                {'status': u'removed', 'bibcode': 'Bibcode4', 'created': '2017-05-26T21:29:22.728368+00:00', 'provenance': u'OrcidImporter', 'orcidid': '0000-0003-3041-2092', 'id': None},
                {'status': u'unchanged', 'bibcode': 'Bibcode1', 'created': '2017-01-01T00:00:00+00:00', 'provenance': u'OrcidImporter', 'orcidid': '0000-0003-3041-2092', 'id': None},
              ]
            
            
            tasks.task_index_orcid_profile({'orcidid': '0000-0003-3041-2092'})
            
            self.assertTrue(next_task.called)
            self.assertEqual(next_task.call_count, 4)
            
            self.assertEqual([(x.bibcode, x.status) for x in insert_claims.call_args[0][0]],
                             [(u'', u'#full-import'), ('Bibcode2', u'claimed'), ('Bibcode3', u'claimed'), ('Bibcode4', u'removed'), ('Bibcode1', u'unchanged')])
            
            self.assertEqual([(x[0][0]['bibcode'], x[0][0]['status']) for x in next_task.call_args_list],
                             [('Bibcode2', u'claimed'), ('Bibcode3', u'claimed'), ('Bibcode4', u'removed'), ('Bibcode1', u'unchanged')]
                             )


    def test_task_ingest_claim(self):
        
        with patch.object(self.app, 'retrieve_orcid') as retrieve_orcid, \
            patch.object(self.app, 'retrieve_metadata') as retrieve_metadata, \
            patch.object(tasks.task_match_claim, 'delay') as next_task:
            
            self.assertFalse(next_task.called)
            retrieve_metadata.return_value = {'bibcode': 'BIBCODE22'}
            retrieve_orcid.return_value = {'status': None, 
                                           'name': u'Stern, D K', 
                                           'facts': {u'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'], u'orcid_name': [u'Stern, Daniel'], u'author_norm': [u'Stern, D'], u'name': u'Stern, D K'}, 
                                           'orcidid': u'0000-0003-2686-9241', 
                                           'id': 1, 
                                           'account_id': None,
                                           'updated': utils.get_date('2017-01-01')
                                           }
            
            tasks.task_ingest_claim({'status': u'claimed', 
                                     'bibcode': 'foo Bibcode2xxxxxxxxxxx bar', 
                                     'created': '2017-01-01T00:00:00+00:00', 
                                     'provenance': u'provenance', 
                                     'orcidid': '0000-0003-3041-2092'
                                    })
            
            self.assertEqual('Bibcode2xxxxxxxxxxx', retrieve_metadata.call_args[0][0])
            self.assertDictContainsSubset(
                             {'status': u'claimed', 'bibcode': 'BIBCODE22', 
                              u'name': u'Stern, D K', 
                              'provenance': u'provenance', u'orcid_name': [u'Stern, Daniel'], 
                              u'author_norm': [u'Stern, D'], u'author_status': None, 
                              'orcidid': '0000-0003-3041-2092', 
                              u'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'],  
                              u'author_id': 1, u'account_id': None},
                             next_task.call_args[0][0],
                             )
            
            
            
            # check authors can be skipped
            retrieve_orcid.return_value = {'status': 'blacklisted', 
                                           'name': u'Stern, D K', 
                                           'facts': {u'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'], u'orcid_name': [u'Stern, Daniel'], u'author_norm': [u'Stern, D'], u'name': u'Stern, D K'}, 
                                           'orcidid': u'0000-0003-2686-9241', 
                                           'id': 1, 
                                           'account_id': None,
                                           'updated': utils.get_date('2017-01-01')
                                           }
            tasks.task_ingest_claim({'status': u'claimed', 
                                     'bibcode': 'foo Bibcode2xxxxxxxxxxx bar', 
                                     'created': '2017-01-01T00:00:00+00:00', 
                                     'provenance': u'provenance', 
                                     'orcidid': '0000-0003-3041-2092'
                                    })
            assert len(next_task.call_args_list) == 1
            
            
            
    def test_task_match_claim(self):
        
        with patch.object(self.app, 'retrieve_record') as retrieve_record, \
            patch.object(self.app, 'record_claims') as record_claims, \
            patch.object(tasks.task_output_results, 'delay') as next_task:
            
            retrieve_record.return_value = {'bibcode': 'BIBCODE22',
                                            'authors': ['Einstein, A', 'Socrates', 'Stern, D K', 'Munger, C'],
                                            'claims': {'verified': ['-', '-', '-', '-'],
                                                       'unverified': ['-', '-', '-', '-']}}
            
            self.assertFalse(next_task.called)
            tasks.task_match_claim({'status': u'claimed', 'bibcode': 'BIBCODE22', 
                              u'name': u'Stern, D K', 
                              'provenance': u'provenance', u'orcid_name': [u'Stern, Daniel'], 
                              u'author_norm': [u'Stern, D'], u'author_status': None, 
                              'orcidid': '0000-0003-3041-2092', 
                              u'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'],  
                              u'author_id': 1, u'account_id': None})
            
            self.assertEqual(('BIBCODE22', 
                              {'verified': ['-', '-', '-', '-'], 'unverified': ['-', '-', '0000-0003-3041-2092', '-']}, 
                              ['Einstein, A', 'Socrates', 'Stern, D K', 'Munger, C']),
                             record_claims.call_args[0])
            
            self.assertEqual({'bibcode': 'BIBCODE22',
                              'authors': ['Einstein, A', 'Socrates', 'Stern, D K', 'Munger, C'],
                              'verified': ['-', '-', '-', '-'], 
                              'unverified': ['-', '-', '0000-0003-3041-2092', '-']}, 
                             next_task.call_args[0][0].toJSON()
                             )
            

    def test_task_check_orcid_updates(self):
        
        with patch.object(tasks.requests, 'get') as get, \
            patch.object(tasks.task_index_orcid_profile, 'delay') as next_task, \
            patch.object(tasks.task_check_orcid_updates, 'apply_async') as recheck_task:
            
            #data = open(os.path.join(self.proj_home, 'ADSOrcid/tests/stub_data', '0000-0003-3041-2092.orcid-updates.json'), 'r').read()
            data = [{'orcid_id': '0000-0003-3041-2092', 'updated': str(utils.get_date())}, 
                    {'orcid_id': '0000-0003-3041-2093', 'updated': str(utils.get_date())}]
            r = PropertyMock()
            r.text = str(data)
            r.json = lambda: data
            r.status_code = 200
            get.return_value = r
            
            tasks.task_check_orcid_updates({}) # could be anything
            
            self.assertEqual(next_task.call_args_list[0][0][0]['orcidid'], '0000-0003-3041-2092')
            self.assertEqual(next_task.call_args_list[1][0][0]['orcidid'], '0000-0003-3041-2093')
            self.assertEqual(str(recheck_task.call_args_list[0]), "call({u'errcount': 0}, countdown=300)")
            
            

if __name__ == '__main__':
    unittest.main()