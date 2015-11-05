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

from ADSOrcid.tests import test_base
from ADSOrcid import app
from ADSOrcid.pipeline.pworkers import ClaimsImporter
from ADSOrcid.models import AuthorInfo, ClaimsLog, Records, Base

class TestMatcherUpdater(test_base.TestUnit):
    """
    Tests the worker's methods
    """
    
    def tearDown(self):
        test_base.TestUnit.tearDown(self)
        Base.metadata.drop_all()
    
    def create_app(self):
        app.init_app({
            'SQLALCHEMY_URL': 'sqlite:///',
            'SQLALCHEMY_ECHO': True,
            'ORCID_CHECK_FOR_CHANGES': 0
        })
        Base.metadata.bind = app.session.get_bind()
        Base.metadata.create_all()
        return app
    
    @httpretty.activate
    def test_ingester_logic(self):
        """Has to be able to diff orcid profile against the 
        existing log in a database"""
        
        orcidid = '0000-0003-3041-2092'
        
        httpretty.register_uri(
            httpretty.GET, self.app.config['API_ORCID_EXPORT_PROFILE'] % orcidid,
            content_type='application/json',
            body=open(os.path.join(self.app.config['TEST_UNIT_DIR'], 'stub_data', orcidid + '.orcid-profile.json')).read())
        httpretty.register_uri(
            httpretty.GET, re.compile(self.app.config['API_ORCID_UPDATES_ENDPOINT'] % '.*'),
            content_type='application/json',
            body=open(os.path.join(self.app.config['TEST_UNIT_DIR'], 'stub_data', orcidid + '.orcid-updates.json')).read())
        
        worker = ClaimsImporter()
        worker.check_orcid_updates()

if __name__ == '__main__':
    unittest.main()        
        