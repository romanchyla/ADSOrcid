#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit tests of the project. Each function related to the workers individual tools
are tested in this suite. There is no communication.
"""


import json
import re
import httpretty
import mock
import os
import unittest
import datetime
from dateutil import parser
from mock import patch

from ADSOrcid.tests import test_base
from ADSOrcid import app, importer
from ADSOrcid.pipeline import workers, ClaimsImporter
from ADSOrcid.models import AuthorInfo, ClaimsLog, Records, Base, KeyValue

class TestWorkers(test_base.TestUnit):
    """
    Tests the GenericWorker's methods
    """
    
    def tearDown(self):
        test_base.TestUnit.tearDown(self)
        Base.metadata.drop_all()
        app.close_app()
    
    def create_app(self):
        app.init_app({
            'SQLALCHEMY_URL': 'sqlite:///',
            'SQLALCHEMY_ECHO': False,
            'ORCID_CHECK_FOR_CHANGES': 0
        })
        Base.metadata.bind = app.session.get_bind()
        Base.metadata.create_all()
        return app
    
    @patch('ADSOrcid.pipeline.OutputHandler.OutputHandler.forward', return_value=None)
    def test_output_handler(self, *args):
        """Check it is sending bibcodes"""
        worker = workers.OutputHandler.OutputHandler()
        worker.process_payload({
                                u'bibcode': u'2014ATel.6427....1V', 
                                u'unverified': [u'0000-0003-3455-5082', u'-', u'-', u'-', u'0000-0001-6347-0649', u'0000-0002-6082-5384', u'-', u'-', u'-', u'-', u'-', u'0000-0003-4666-119X', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'0000-0002-4590-0040', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-', u'-']})
        worker.forward.assert_called_with([u'2014ATel.6427....1V'], topic='SolrUpdateRoute')
    
    
    @httpretty.activate
    def test_ingester_logic(self):
        """Has to be able to diff orcid profile against the 
        existing log in a database"""
        #self.maxDiff = None
        orcidid = '0000-0003-3041-2092'
        
        httpretty.register_uri(
            httpretty.GET, self.app.config['API_ORCID_EXPORT_PROFILE'] % orcidid,
            content_type='application/json',
            body=open(os.path.join(self.app.config['TEST_UNIT_DIR'], 'stub_data', orcidid + '.orcid-profile.json')).read())
        httpretty.register_uri(
            httpretty.GET, re.compile(self.app.config['API_ORCID_UPDATES_ENDPOINT'] % '.*'),
            content_type='application/json',
            body=open(os.path.join(self.app.config['TEST_UNIT_DIR'], 'stub_data', orcidid + '.orcid-updates.json')).read())
        
        worker = ClaimsImporter.ClaimsImporter()
        worker.check_orcid_updates()
        
        with app.session_scope() as session:
            self.assertEquals('2015-11-05T11:37:36.381000+00:00', session.query(KeyValue).filter(KeyValue.key == 'last.check').first().value)
            recs = []
            for x in session.query(ClaimsLog).all():
                recs.append(x.toJSON())
            self.assertEqual(recs, [
                {'status': u'#full-import', 'bibcode': u'', 'created': '2015-11-05T16:37:33.381000+00:00', 'provenance': u'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 1},
                {'status': u'claimed', 'bibcode': u'2015arXiv150304194A', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 2},
                {'status': u'claimed', 'bibcode': u'2015AAS...22533655A', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 3},
                {'status': u'claimed', 'bibcode': u'2014arXiv1406.4542H', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 4},
                {'status': u'claimed', 'bibcode': u'2015arXiv150305881C', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'Roman Chyla', 'orcidid': u'0000-0003-3041-2092', 'id': 5},
                {'status': u'claimed', 'bibcode': u'2015ASPC..492..150T', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 6},
                {'status': u'claimed', 'bibcode': u'2015ASPC..492..208G', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 7},
                {'status': u'claimed', 'bibcode': u'2014AAS...22325503A', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 8}
            ])
            kv = session.query(KeyValue).filter(KeyValue.key == 'last.check').first()
            kv.value = ''
            session.commit()
        
        # do the same stuff again (it should not bother with new recs)
        worker.check_orcid_updates()
        with app.session_scope() as session:
            self.assertEquals(len(session.query(ClaimsLog).all()), 8)
            new_value = parser.parse(session.query(KeyValue).filter(KeyValue.key == 'last.check').first().value)
            self.assertEquals('2015-11-05T11:37:36.381000+00:00', session.query(KeyValue).filter(KeyValue.key == 'last.check').first().value)
            
            # now change the date of the #full-import (this will force the logic to re-evaluate the batch against the 
            # existing claims)
            c = session.query(ClaimsLog).filter(ClaimsLog.status == '#full-import').first()
            c.created = c.created + datetime.timedelta(microseconds=1000)
            
        worker.check_orcid_updates()
        
        with app.session_scope() as session:
            recs = []
            for x in session.query(ClaimsLog).all():
                recs.append(x.toJSON())
            self.assertEqual(recs,
                [{'status': u'#full-import', 'bibcode': u'', 'created': '2015-11-05T16:37:33.382000+00:00', 'provenance': u'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 1}, 
                {'status': u'claimed', 'bibcode': u'2015arXiv150304194A', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 2}, 
                {'status': u'claimed', 'bibcode': u'2015AAS...22533655A', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 3}, 
                {'status': u'claimed', 'bibcode': u'2014arXiv1406.4542H', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 4}, 
                {'status': u'claimed', 'bibcode': u'2015arXiv150305881C', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'Roman Chyla', 'orcidid': u'0000-0003-3041-2092', 'id': 5}, 
                {'status': u'claimed', 'bibcode': u'2015ASPC..492..150T', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 6}, 
                {'status': u'claimed', 'bibcode': u'2015ASPC..492..208G', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 7}, 
                {'status': u'claimed', 'bibcode': u'2014AAS...22325503A', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 8}, 
                {'status': u'#full-import', 'bibcode': u'', 'created': '2015-11-05T16:37:33.381000+00:00', 'provenance': u'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 9}, 
                {'status': u'unchanged', 'bibcode': u'2015arXiv150304194A', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 10}, 
                {'status': u'unchanged', 'bibcode': u'2015AAS...22533655A', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 11}, 
                {'status': u'unchanged', 'bibcode': u'2014arXiv1406.4542H', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 12}, 
                {'status': u'unchanged', 'bibcode': u'2015arXiv150305881C', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 13}, 
                {'status': u'unchanged', 'bibcode': u'2015ASPC..492..150T', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 14}, 
                {'status': u'unchanged', 'bibcode': u'2015ASPC..492..208G', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 15}, 
                {'status': u'unchanged', 'bibcode': u'2014AAS...22325503A', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 16} 
                ])
            
        # now let's pretend that we have one extra claim and there was one deletion
        with app.session_scope() as session:
            session.query(ClaimsLog).filter(ClaimsLog.id > 8).delete() # clean up
            session.query(ClaimsLog).filter_by(id=5).delete()
            importer.insert_claims([importer.create_claim(bibcode='2014AAS...22325503A', 
                                                          orcidid=orcidid, status='removed',
                                                          date='2015-11-05 11:37:33.381000+00:00')])
            
        
        worker.check_orcid_updates()
        
        with app.session_scope() as session:
            recs = []
            for x in session.query(ClaimsLog).all():
                recs.append(x.toJSON())
            self.assertEqual(recs,
                [{'status': u'#full-import', 'bibcode': u'', 'created': '2015-11-05T16:37:33.382000+00:00', 'provenance': u'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 1},
                {'status': u'claimed', 'bibcode': u'2015arXiv150304194A', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 2},
                {'status': u'claimed', 'bibcode': u'2015AAS...22533655A', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 3},
                {'status': u'claimed', 'bibcode': u'2014arXiv1406.4542H', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 4},
                {'status': u'claimed', 'bibcode': u'2015ASPC..492..150T', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 6},
                {'status': u'claimed', 'bibcode': u'2015ASPC..492..208G', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 7},
                {'status': u'claimed', 'bibcode': u'2014AAS...22325503A', 'created': '2015-09-16T10:59:01.721000+00:00', 'provenance': u'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 8},
                {'status': u'removed', 'bibcode': u'2014AAS...22325503A', 'created': '2015-11-05T11:37:33.381000+00:00', 'provenance': u'None', 'orcidid': u'0000-0003-3041-2092', 'id': 9},
                {'status': u'#full-import', 'bibcode': u'', 'created': '2015-11-05T16:37:33.381000+00:00', 'provenance': u'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 10},
                {'status': u'claimed', 'bibcode': u'2015arXiv150305881C', 'created': '2015-09-16T10:59:01.721000+00:00', u'provenance': 'Roman Chyla', 'orcidid': u'0000-0003-3041-2092', 'id': 11},
                {'status': u'claimed', 'bibcode': u'2014AAS...22325503A', 'created': '2015-09-16T10:59:01.721000+00:00', u'provenance': 'NASA ADS', 'orcidid': u'0000-0003-3041-2092', 'id': 12},
                {'status': u'unchanged', 'bibcode': u'2014arXiv1406.4542H', 'created': '2015-09-16T10:59:01.721000+00:00', u'provenance': 'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 13},
                {'status': u'unchanged', 'bibcode': u'2015ASPC..492..150T', 'created': '2015-09-16T10:59:01.721000+00:00', u'provenance': 'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 14},
                {'status': u'unchanged', 'bibcode': u'2015ASPC..492..208G', 'created': '2015-09-16T10:59:01.721000+00:00', u'provenance': 'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 15},
                {'status': u'unchanged', 'bibcode': u'2015arXiv150304194A', 'created': '2015-09-16T10:59:01.721000+00:00', u'provenance': 'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 16},
                {'status': u'unchanged', 'bibcode': u'2015AAS...22533655A', 'created': '2015-09-16T10:59:01.721000+00:00', u'provenance': 'ClaimsImporter', 'orcidid': u'0000-0003-3041-2092', 'id': 17}
                ])


if __name__ == '__main__':
    unittest.main()        
        