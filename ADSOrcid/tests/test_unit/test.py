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

from ADSOrcid.tests import test_base
from ADSOrcid import matcher, app, updater
from ADSOrcid.models import AuthorInfo, ClaimsLog, Records, Base

class TestMatcher(test_base.TestUnit):
    """
    Tests the worker's methods
    """
    
    def create_app(self):
        app.init_app({
            'SQLALCHEMY_URL': 'sqlite:///'
        })
        Base.metadata.bind = app.session.get_bind()
        Base.metadata.create_all()
        return app
    
    def test_retrive_orcid(self):
        """Has to find and load/or create ORCID data"""
        
        author = matcher.retrieve_orcid('0123-4567')
        self.assertIsInstance(author, AuthorInfo)
        
        AuthorInfo()
        

    def test_find_author_position(self):
        """
        Given the ORCID ID, and information about author name, 
        we have to identify the position of the author from
        the list of supplied names

        :return: no return
        """
        
        res = updater.find_orcid_position([
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


if __name__ == '__main__':
    unittest.main()
