#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit tests of the project. Each function related to the workers individual tools
are tested in this suite. There is no communication.
"""


import sys
import os

import unittest
import json
import re
import os
import math
import httpretty
import mock
from mock import patch
from io import BytesIO
from datetime import datetime
import adsputils as utils
from ADSOrcid import app
from ADSOrcid.models import ClaimsLog, Records, AuthorInfo, Base, ChangeLog

class TestAdsOrcidCelery(unittest.TestCase):
    """
    Tests the appliction's methods
    """
    def setUp(self):
        unittest.TestCase.setUp(self)
        proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        self.app = app.create_app('test',
            {
            'SQLALCHEMY_URL': 'sqlite:///',
            'SQLALCHEMY_ECHO': False,
            'PROJ_HOME' : proj_home,
            'TEST_DIR' : os.path.join(proj_home, 'ADSOrcid/tests'),
            })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
    
    
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()

    
    def test_app(self):
        assert self.app._config.get('SQLALCHEMY_URL') == 'sqlite:///'
        assert self.app.conf.get('SQLALCHEMY_URL') == 'sqlite:///'

    def test_create_claim(self):
        c = self.app.create_claim(bibcode='b123456789123456789', 
                                          orcidid='0000-0000-0000-0001', 
                                          status='removed')
        assert isinstance(c, ClaimsLog)
        assert c.bibcode == 'b123456789123456789'
        self.assertTrue(len(self.app._session.query(ClaimsLog)
                            .filter_by(bibcode='b123456789123456789').all()) == 0)
        
        # test what happens when the claim already exists
        self.app._session.add(c)
        self.app._session.commit()
        cid = c.id
        
        c = self.app.create_claim(bibcode='b123456789123456789', 
                                          orcidid='0000-0000-0000-0001', 
                                          status='claimed',
                                          date=c.created,
                                          force_new=False)
        assert c.status == 'claimed'
        assert c.id == cid

    
    def test_insert_claims(self):
        """
        It should be able to create a series of claims
        """
        r = self.app.insert_claims([
                    {'bibcode': 'b123456789123456789',
                     'orcidid': '0000-0000-0000-0001',
                     'provenance' : 'ads test'},
                    {'bibcode': 'b123456789123456789',
                     'orcidid': '0000-0000-0000-0001',
                     'status' : 'updated'},
                    self.app.create_claim(bibcode='b123456789123456789', 
                                          orcidid='0000-0000-0000-0001', 
                                          status='removed')
                ])
        self.assertEquals(len(r), 3)
        self.assertTrue(len(self.app._session.query(ClaimsLog)
                            .filter_by(bibcode='b123456789123456789').all()) == 3)


    def test_import_recs(self):
        """It should know how to import bibcode:orcidid pairs
        :return None
        """
        
        fake_file = BytesIO("\n".join([
                                 "b123456789123456789\t0000-0000-0000-0001",
                                 "b123456789123456789\t0000-0000-0000-0002\tarxiv",
                                 "b123456789123456789\t0000-0000-0000-0003\tarxiv\tclaimed",
                                 "b123456789123456789\t0000-0000-0000-0004\tfoo        \tclaimed\t2008-09-03T20:56:35.450686Z",
                                 "b123456789123456789\t0000-0000-0000-0005",
                                 "b123456789123456789\t0000-0000-0000-0006",
                                 "b123456789123456789\t0000-0000-0000-0004\tfoo        \tupdated\t2009-09-03T20:56:35.450686Z",
                                ]))
        with mock.patch('ADSOrcid.app.open', return_value=fake_file, create=True
                ) as context:
            self.app.import_recs(__file__)
            self.assertTrue(len(self.app._session.query(ClaimsLog).all()) == 7)

        fake_file = BytesIO('\n'.join([
                                "b123456789123456789\t0000-0000-0000-0001",
                                "b123456789123456789\t0000-0000-0000-0002\tarxiv"]))

        with mock.patch('ADSOrcid.app.open', return_value=fake_file, create=True
                ) as context:
            c = []
            self.app.import_recs(__file__, collector=c)
            self.assertTrue(len(c) == 2)
    
    
    @httpretty.activate
    def test_harvest_author_info(self):
        """
        We have to be able to verify orcid against orcid api
        and also collect data from SOLR (author names)
        """
        app = self.app
        orcidid = '0000-0003-2686-9241'
        
        httpretty.register_uri(
            httpretty.GET, self.app.conf['API_ORCID_PROFILE_ENDPOINT'] % orcidid,
            content_type='application/json',
            body=open(os.path.join(self.app.conf['TEST_DIR'], 'stub_data', orcidid + '.orcid.json')).read())
        httpretty.register_uri(
            httpretty.GET, self.app.conf['API_ORCID_EXPORT_PROFILE'] % orcidid,
            content_type='application/json',
            body=open(os.path.join(self.app.conf['TEST_DIR'], 'stub_data', orcidid + '.ads.json')).read())
        httpretty.register_uri(
            httpretty.GET, self.app.conf['API_SOLR_QUERY_ENDPOINT'],
            content_type='application/json',
            body=open(os.path.join(self.app.conf['TEST_DIR'], 'stub_data', orcidid + '.solr.json')).read())
        
        data = app.harvest_author_info(orcidid)
        self.assertDictEqual(data, {'orcid_name': [u'Stern, Daniel'],
                                    'author': [u'Stern, A D',
                                               u'Stern, Andrew D',
                                               u'Stern, D', 
                                               u'Stern, D K', 
                                               u'Stern, Daniel'
                                               ],
                                    'authorized': True,
                                    'author_norm': [u'Stern, D'],
                                    'current_affiliation': u'ADS',
                                    'name': u'Stern, D K',
                                    'short_name': ['Stern, A', 'Stern, A D', 'Stern, D', 'Stern, D K']
                                    })


    def test_update_author(self):
        """Has to update AuthorInfo and also create a log of events about the changes."""
        
        # bootstrap the db with already existing author info
        with self.app.session_scope() as session:
            ainfo = AuthorInfo(orcidid='0000-0003-2686-9241',
                               facts=json.dumps({'orcid_name': [u'Stern, Daniel'],
                                    'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'],
                                    'author_norm': [u'Stern, D'],
                                    'name': u'Stern, D K'
                                    }),
                               )
            session.add(ainfo)
            session.commit()
        
        with self.app.session_scope() as session:
            ainfo = session.query(AuthorInfo).filter_by(orcidid='0000-0003-2686-9241').first()
            with patch.object(self.app, 'harvest_author_info', return_value= {'orcid_name': [u'Sternx, Daniel'],
                                        'author': [u'Stern, D', u'Stern, D K', u'Sternx, Daniel'],
                                        'author_norm': [u'Stern, D'],
                                        'name': u'Sternx, D K'
                                        }
                    ) as _:
                app.clear_caches()
                author = self.app.retrieve_orcid('0000-0003-2686-9241')
                self.assertDictContainsSubset({'status': None, 
                                               'name': u'Sternx, D K', 
                                               'facts': {u'author': [u'Stern, D', u'Stern, D K', u'Sternx, Daniel'], u'orcid_name': [u'Sternx, Daniel'], u'author_norm': [u'Stern, D'], u'name': u'Sternx, D K'}, 
                                               'orcidid': u'0000-0003-2686-9241', 
                                               'id': 1, 
                                               'account_id': None}, 
                                              author)
                self.assertDictContainsSubset({'oldvalue': json.dumps([u'Stern, Daniel']),
                                               'newvalue': json.dumps([u'Sternx, Daniel'])},
                                              session.query(ChangeLog).filter_by(key='0000-0003-2686-9241:update:orcid_name').first().toJSON())
                self.assertDictContainsSubset({'oldvalue': json.dumps(u'Stern, D K'),
                                               'newvalue': json.dumps(u'Sternx, D K')},
                                              session.query(ChangeLog).filter_by(key='0000-0003-2686-9241:update:name').first().toJSON())
                self.assertDictContainsSubset({'oldvalue': json.dumps([u'Stern, D', u'Stern, D K', u'Stern, Daniel']),
                                               'newvalue': json.dumps([u'Stern, D', u'Stern, D K', u'Sternx, Daniel'])},
                                              session.query(ChangeLog).filter_by(key='0000-0003-2686-9241:update:author').first().toJSON())
        
        with self.app.session_scope() as session:
            ainfo = session.query(AuthorInfo).filter_by(orcidid='0000-0003-2686-9241').first()
            with mock.patch.object(self.app, 'harvest_author_info', return_value= {
                                        'name': u'Sternx, D K',
                                        'authorized': True
                                        }
                    ) as _:
                app.clear_caches()
                author = self.app.retrieve_orcid('0000-0003-2686-9241')
                self.assertDictContainsSubset({'status': None, 
                                               'name': u'Sternx, D K', 
                                               'facts': {u'authorized': True, u'name': u'Sternx, D K'}, 
                                               'orcidid': u'0000-0003-2686-9241', 
                                               'id': 1, 
                                               'account_id': 1}, 
                                              author)
                self.assertDictContainsSubset({'oldvalue': json.dumps([u'Stern, Daniel']),
                                               'newvalue': json.dumps([u'Sternx, Daniel'])},
                                              session.query(ChangeLog).filter_by(key='0000-0003-2686-9241:update:orcid_name').first().toJSON())
                self.assertDictContainsSubset({'oldvalue': json.dumps(u'Stern, D K'),
                                               'newvalue': json.dumps(u'Sternx, D K')},
                                              session.query(ChangeLog).filter_by(key='0000-0003-2686-9241:update:name').first().toJSON())
                self.assertDictContainsSubset({'oldvalue': json.dumps([u'Stern, D', u'Stern, D K', u'Stern, Daniel']),
                                               'newvalue': json.dumps([u'Stern, D', u'Stern, D K', u'Sternx, Daniel'])},
                                              session.query(ChangeLog).filter_by(key='0000-0003-2686-9241:update:author').first().toJSON())
 

    def test_create_orcid(self):
        """Has to create AuthorInfo and populate it, but not add to database"""
        with mock.patch.object(self.app, 'harvest_author_info', return_value= {'orcid_name': [u'Stern, Daniel'],
                                    'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'],
                                    'author_norm': [u'Stern, D'],
                                    'name': u'Stern, D K'
                                    }
                ) as _:
            res = self.app.create_orcid('0000-0003-2686-9241')
            self.assertIsInstance(res, AuthorInfo)
            self.assertEqual(res.name, 'Stern, D K')
            self.assertEqual(res.orcidid, '0000-0003-2686-9241')
            self.assertEqual(res.facts, '{"orcid_name": ["Stern, Daniel"], "author_norm": ["Stern, D"], "name": "Stern, D K", "author": ["Stern, D", "Stern, D K", "Stern, Daniel"]}')
            
            self.assertTrue(self.app._session.query(AuthorInfo).first() is None)


    def test_retrive_orcid(self):
        """Has to find and load/or create ORCID data"""
        with mock.patch.object(self.app, 'harvest_author_info', return_value= {'orcid_name': [u'Stern, Daniel'],
                                    'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'],
                                    'author_norm': [u'Stern, D'],
                                    'name': u'Stern, D K'
                                    }
                ) as _:
            author = self.app.retrieve_orcid('0000-0003-2686-9241')
            self.assertDictContainsSubset({'status': None, 
                                           'name': u'Stern, D K', 
                                           'facts': {u'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'], u'orcid_name': [u'Stern, Daniel'], u'author_norm': [u'Stern, D'], u'name': u'Stern, D K'}, 
                                           'orcidid': u'0000-0003-2686-9241', 
                                           'id': 1, 
                                           'account_id': None}, 
                                          author)
        
            self.assertTrue(self.app._session.query(AuthorInfo).first().orcidid, '0000-0003-2686-9241')
            

 
    def test_update_database(self):
        """Inserts a record (of claims) into the database"""
        self.app.record_claims('bibcode', {'verified': ['foo', '-', 'bar'], 'unverified': ['-', '-', '-']})
        with self.app.session_scope() as session:
            r = session.query(Records).filter_by(bibcode='bibcode').first()
            self.assertEquals(json.loads(r.claims), {'verified': ['foo', '-', 'bar'], 'unverified': ['-', '-', '-']})
            self.assertTrue(r.created == r.updated)
            self.assertFalse(r.processed)
            
        self.app.record_claims('bibcode', {'verified': ['foo', 'zet', 'bar'], 'unverified': ['-', '-', '-']})
        with self.app.session_scope() as session:
            r = session.query(Records).filter_by(bibcode='bibcode').first()
            self.assertEquals(json.loads(r.claims), {'verified': ['foo', 'zet', 'bar'], 'unverified': ['-', '-', '-']})
            self.assertTrue(r.created != r.updated)
            self.assertFalse(r.processed)
        
        self.app.mark_processed('bibcode')
        with self.app.session_scope() as session:
            r = session.query(Records).filter_by(bibcode='bibcode').first()
            self.assertTrue(r.processed)
            
            

    def test_get_claims(self):
        """Check the correct logic for discovering difference in the orcid profile."""
        
        orcidid = '0000-0003-3041-2092'
        def side_effect(x, search_identifiers=False):
            return {'bibcode': x}
        with mock.patch.object(self.app, 'retrieve_orcid', 
                return_value={'status': None, 'updated': None, 'name': None, 'created': '2009-09-03T20:56:35.450686+00:00', 
                              'facts': {}, 'orcidid': orcidid, 'id': 1, 'account_id': None} ) as harvest_author_info, \
            mock.patch.object(self.app, '_get_ads_orcid_profile',
                return_value=json.loads(open(os.path.join(self.app.conf['TEST_DIR'], 'stub_data', orcidid + '.ads.json')).read())) as _, \
            mock.patch.object(self.app, 'retrieve_metadata', side_effect=side_effect) as retrieve_metadata:
            
            
            orcid_present, updated, removed = self.app.get_claims(orcidid,
                         self.app.conf.get('API_TOKEN'), 
                         self.app.conf.get('API_ORCID_EXPORT_PROFILE') % orcidid,
                         force=False,
                         orcid_identifiers_order=self.app.conf.get('ORCID_IDENTIFIERS_ORDER', {'bibcode': 9, '*': -1})
                         )
            assert len(orcid_present) == 7 and len(updated) == 0 and len(removed) == 0
            
            # pretend that we have already ran the import
            cdate = utils.get_date('2015-11-05 16:37:33.381000+00:00') # this is the latest moddate from the orcid profile
            self.app.insert_claims([self.app.create_claim(bibcode='', 
                              orcidid=orcidid, 
                              provenance='OrcidImporter', 
                              status='#full-import',
                              date=cdate
                              )])
            
            # it should ignore the next call
            orcid_present, updated, removed = self.app.get_claims(orcidid,
                         self.app.conf.get('API_TOKEN'), 
                         self.app.conf.get('API_ORCID_EXPORT_PROFILE') % orcidid,
                         force=False,
                         orcid_identifiers_order=self.app.conf.get('ORCID_IDENTIFIERS_ORDER', {'bibcode': 9, '*': -1})
                         )
            assert len(orcid_present) == 0 and len(updated) == 0 and len(removed) == 0
            
            # but if we force it, it must not ignore use...
            orcid_present, updated, removed = self.app.get_claims(orcidid,
                         self.app.conf.get('API_TOKEN'), 
                         self.app.conf.get('API_ORCID_EXPORT_PROFILE') % orcidid,
                         force=True,
                         orcid_identifiers_order=self.app.conf.get('ORCID_IDENTIFIERS_ORDER', {'bibcode': 9, '*': -1})
                         )
            #print len(orcid_present), len(updated), len(removed)
            assert len(orcid_present) == 7 and len(updated) == 0 and len(removed) == 0
        
    
if __name__ == '__main__':
    unittest.main()
