#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit tests of the project. Each function related to the workers individual tools
are tested in this suite. There is no communication.

        config = utils.load_config()
        
        #update PROJ_HOME since normally it is run from higher leve
        config['PROJ_HOME'] = os.path.abspath(config['PROJ_HOME'] + '/..')
        
        config['TEST_UNIT_DIR'] = os.path.join(config['PROJ_HOME'],
                         'ADSOrcid/tests/test_unit')
        config['TEST_INTGR_DIR'] = os.path.join(config['PROJ_HOME'],
                         'ADSOrcid/tests/test_integration')
        config['TEST_FUNC_DIR'] = os.path.join(config['PROJ_HOME'],
                         'ADSOrcid/tests/test_functional')

        self.app = self.create_app()
        self.app.config.update(config)
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
from io import BytesIO
from datetime import datetime
from ADSOrcid import app
from ADSOrcid.models import ClaimsLog, Records, AuthorInfo, Base, ChangeLog

class TestMatcherUpdater(unittest.TestCase):
    """
    Tests the worker's methods
    """
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.app = app.create_app('test',
            {
            'SQLALCHEMY_URL': 'sqlite:///',
            'SQLALCHEMY_ECHO': False
            })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
    
    
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()
    
   


    @httpretty.activate
    def test_harvest_author_info(self):
        """
        We have to be able to verify orcid against orcid api
        and also collect data from SOLR (author names)
        """
        app = self.app
        orcidid = '0000-0003-2686-9241'
        
        httpretty.register_uri(
            httpretty.GET, self.app.config['API_ORCID_PROFILE_ENDPOINT'] % orcidid,
            content_type='application/json',
            body=open(os.path.join(self.app.config['TEST_UNIT_DIR'], 'stub_data', orcidid + '.orcid.json')).read())
        httpretty.register_uri(
            httpretty.GET, self.app.config['API_ORCID_EXPORT_PROFILE'] % orcidid,
            content_type='application/json',
            body=open(os.path.join(self.app.config['TEST_UNIT_DIR'], 'stub_data', orcidid + '.ads.json')).read())
        httpretty.register_uri(
            httpretty.GET, self.app.config['API_SOLR_QUERY_ENDPOINT'],
            content_type='application/json',
            body=open(os.path.join(self.app.config['TEST_UNIT_DIR'], 'stub_data', orcidid + '.solr.json')).read())
        
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
        
    
    def test_create_orcid(self):
        """Has to create AuthorInfo and populate it, but not add to database"""
        app = self.app
        with mock.patch('ADSOrcid.matcher.harvest_author_info', return_value= {'orcid_name': [u'Stern, Daniel'],
                                    'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'],
                                    'author_norm': [u'Stern, D'],
                                    'name': u'Stern, D K'
                                    }
                ) as context:
            res = app.create_orcid('0000-0003-2686-9241')
            self.assertIsInstance(res, AuthorInfo)
            self.assertEqual(res.name, 'Stern, D K')
            self.assertEqual(res.orcidid, '0000-0003-2686-9241')
            self.assertEqual(res.facts, '{"orcid_name": ["Stern, Daniel"], "author_norm": ["Stern, D"], "name": "Stern, D K", "author": ["Stern, D", "Stern, D K", "Stern, Daniel"]}')
            
            self.assertTrue(self.app.session.query(AuthorInfo).first() is None)

 
    def test_retrive_orcid(self):
        """Has to find and load/or create ORCID data"""
        app = self.app
        with mock.patch('ADSOrcid.matcher.harvest_author_info', return_value= {'orcid_name': [u'Stern, Daniel'],
                                    'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'],
                                    'author_norm': [u'Stern, D'],
                                    'name': u'Stern, D K'
                                    }
                ) as context:
            author = app.retrieve_orcid('0000-0003-2686-9241')
            self.assertDictContainsSubset({'status': None, 
                                           'name': u'Stern, D K', 
                                           'facts': {u'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'], u'orcid_name': [u'Stern, Daniel'], u'author_norm': [u'Stern, D'], u'name': u'Stern, D K'}, 
                                           'orcidid': u'0000-0003-2686-9241', 
                                           'id': 1, 
                                           'account_id': None}, 
                                          author)
        
            self.assertTrue(self.app.session.query(AuthorInfo).first().orcidid, '0000-0003-2686-9241')
    
    
    def test_update_author(self):
        """Has to update AuthorInfo and also create a log of events about the changes."""
        app = self.app
        # bootstrap the db with already existing author info
        with app.session_scope() as session:
            ainfo = AuthorInfo(orcidid='0000-0003-2686-9241',
                               facts=json.dumps({'orcid_name': [u'Stern, Daniel'],
                                    'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'],
                                    'author_norm': [u'Stern, D'],
                                    'name': u'Stern, D K'
                                    }),
                               )
            session.add(ainfo)
            session.commit()
        
        with app.session_scope() as session:
            ainfo = session.query(AuthorInfo).filter_by(orcidid='0000-0003-2686-9241').first()
            with mock.patch('ADSOrcid.matcher.harvest_author_info', return_value= {'orcid_name': [u'Sternx, Daniel'],
                                        'author': [u'Stern, D', u'Stern, D K', u'Sternx, Daniel'],
                                        'author_norm': [u'Stern, D'],
                                        'name': u'Sternx, D K'
                                        }
                    ) as context:
                app.cache.clear()
                app.orcid_cache.clear()
                app.ads_cache.clear()
                author = app.retrieve_orcid('0000-0003-2686-9241')
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
        
        with app.session_scope() as session:
            ainfo = session.query(AuthorInfo).filter_by(orcidid='0000-0003-2686-9241').first()
            with mock.patch('ADSOrcid.matcher.harvest_author_info', return_value= {
                                        'name': u'Sternx, D K',
                                        'authorized': True
                                        }
                    ) as context:
                app.cache.clear()
                app.orcid_cache.clear()
                app.ads_cache.clear()
                author = app.retrieve_orcid('0000-0003-2686-9241')
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
    

    def test_update_record(self):
        """
        Update ADS document with the claim info

        :return: no return
        """
        app = self.app
        doc = {
            'bibcode': '2015ApJ...799..123B', 
            'authors': [
              "Barrière, Nicolas M.",
              "Krivonos, Roman",
              "Tomsick, John A.",
              "Bachetti, Matteo",
              "Boggs, Steven E.",
              "Chakrabarty, Deepto",
              "Christensen, Finn E.",
              "Craig, William W.",
              "Hailey, Charles J.",
              "Harrison, Fiona A.",
              "Hong, Jaesub",
              "Mori, Kaya",
              "Stern, Daniel",
              "Zhang, William W."
            ],
            'claims': {}
        }
        r = app.update_record(
          doc,
          {
           'bibcode': '2015ApJ...799..123B', 
           'orcidid': '0000-0003-2686-9241',
           'account_id': '1',
           'orcid_name': [u'Stern, Daniel'],
           'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'],
           'author_norm': [u'Stern, D'],
           'name': u'Stern, D K' 
          }                          
        )
        self.assertEqual(r, ('verified', 12))
        self.assertEqual(doc['claims']['verified'], 
            ['-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '0000-0003-2686-9241', '-'])
        
        app.update_record(
          doc,
          {
           'bibcode': '2015ApJ...799..123B', 
           'orcidid': '0000-0003-2686-9241',
           'account_id': '1',
           'orcid_name': [u'Stern, Daniel'],
           'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'],
           'author_norm': [u'Stern, D'],
           'name': u'Stern, D K',
           'status': 'removed'
          }                          
        )
        self.assertEqual(doc['claims']['verified'], 
            ['-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-'])
        
        # the size differs
        doc['claims']['verified'] = ['-']
        r = app.update_record(
          doc,
          {
           'bibcode': '2015ApJ...799..123B', 
           'orcidid': '0000-0003-2686-9241',
           'account_id': '1',
           'orcid_name': [u'Stern, Daniel'],
           'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'],
           'author_norm': [u'Stern, D'],
           'name': u'Stern, D K' 
          }                          
        )
        self.assertEqual(r, ('verified', 12))
        self.assertEqual(doc['claims']['verified'], 
            ['-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '0000-0003-2686-9241', '-'])
        
        self.assertEqual(14, len(doc['claims']['verified']))
        

    def test_find_author_position(self):
        """
        Given the ORCID ID, and information about author name, 
        we have to identify the position of the author from
        the list of supplied names

        :return: no return
        """
        app = self.app
        res = app.find_orcid_position([
              "Barrière, Nicolas M.",
              "Krivonos, Roman",
              "Tomsick, John A.",
              "Bachetti, Matteo",
              "Boggs, Steven E.",
              "Chakrabarty, Deepto",
              "Christensen, Finn E.",
              "Craig, William W.",
              "Hailey, Charles J.",
              "Harrison, Fiona A.",
              "Hong, Jaesub",
              "Mori, Kaya",
              "Stern, Daniel",
              "Zhang, William W."
            ],
          ['Stern, D.', 'Stern, Daniel']                          
        )
        self.assertEqual(res, 12)
        
        # check that the author cannot claim what doesn't look like their 
        # own paper
        
        res = app.find_orcid_position([
               "Erdmann, Christopher",
               "Frey, Katie"
               ], 
              ["Accomazzi, Alberto"]);
        self.assertEqual(res, -1)
        
        # check boundaries
        res = app.find_orcid_position([
               "Erdmann, Christopher",
               "Frey, Katie"
               ], 
              ["Erdmann, C"]);
        self.assertEqual(res, 0)
        res = app.find_orcid_position([
               "Erdmann, Christopher",
               "Cote, Ann",
               "Frey, Katie"
               ], 
              ["Frey, Katie"]);
        self.assertEqual(res, 2)

    def test_update_database(self):
        """Inserts a record (of claims) into the database"""
        app = self.app
        app.record_claims('bibcode', {'verified': ['foo', '-', 'bar'], 'unverified': ['-', '-', '-']})
        with app.session_scope() as session:
            r = session.query(Records).filter_by(bibcode='bibcode').first()
            self.assertEquals(json.loads(r.claims), {'verified': ['foo', '-', 'bar'], 'unverified': ['-', '-', '-']})
            self.assertTrue(r.created == r.updated)
            self.assertFalse(r.processed)
            
        app.record_claims('bibcode', {'verified': ['foo', 'zet', 'bar'], 'unverified': ['-', '-', '-']})
        with app.session_scope() as session:
            r = session.query(Records).filter_by(bibcode='bibcode').first()
            self.assertEquals(json.loads(r.claims), {'verified': ['foo', 'zet', 'bar'], 'unverified': ['-', '-', '-']})
            self.assertTrue(r.created != r.updated)
            self.assertFalse(r.processed)
        
        app.mark_processed('bibcode')
        with app.session_scope() as session:
            r = session.query(Records).filter_by(bibcode='bibcode').first()
            self.assertTrue(r.processed)
            
            
    def test_importer_import_recs(self):
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
        with mock.patch('ADSOrcid.importer.open', return_value=fake_file, create=True
                ) as context:
            importer.import_recs(__file__)
            self.assertTrue(len(self.app.session.query(ClaimsLog).all()) == 7)

        fake_file = BytesIO('\n'.join([
                                "b123456789123456789\t0000-0000-0000-0001",
                                "b123456789123456789\t0000-0000-0000-0002\tarxiv"]))

        with mock.patch('ADSOrcid.importer.open', return_value=fake_file, create=True
                ) as context:
            c = []
            importer.import_recs(__file__, collector=c)
            self.assertTrue(len(c) == 2)
        
    
    def test_insert_claim(self):
        """
        It should be able to create a series of claims
        """
        r = importer.insert_claims([
                    {'bibcode': 'b123456789123456789',
                     'orcidid': '0000-0000-0000-0001',
                     'provenance' : 'ads test'},
                    {'bibcode': 'b123456789123456789',
                     'orcidid': '0000-0000-0000-0001',
                     'status' : 'updated'},
                    importer.create_claim(bibcode='b123456789123456789', 
                                          orcidid='0000-0000-0000-0001', 
                                          status='removed')
                ])
        self.assertEquals(len(r), 3)
        
        self.assertTrue(len(self.app.session.query(ClaimsLog)
                            .filter_by(bibcode='b123456789123456789').all()) == 3)
        
    
if __name__ == '__main__':
    unittest.main()
