"""
Unit tests of the project. Each function related to the workers individual tools
are tested in this suite. There is no communication.
"""


import sys
import os

import unittest
import utils
import json
import re
import os
import math

from settings import PROJ_HOME, config, CONSTANTS, META_CONTENT
from lib import CheckIfExtract as check
from lib import StandardFileExtract as std_extract
from lib import WriteMetaFile as writer
from lib import test_base
from requests.exceptions import HTTPError

from ADSOrcid import matcher
from ADSOrcid import models

class Test(test_base.TestUnit):
    """
    Tests the worker's methods
    """
    
    def test_retrive_orcid(self):
        """Has to find and load/or create ORCID data"""
        
        author = matcher.retrieve_orcid('0123-4567')
        self.assertIsInstance(author, models.AuthorInfo)
        
        
        

    def test_find_author_position(self):
        """
        Given the ORCID ID, and information about author name, 
        we have to identify the position of the author from
        the list of supplied names

        :return: no return
        """
        
        matcher.find_orcid_position('Doe, J', '')
        self.assertEqual(exists, False)


if __name__ == '__main__':
    unittest.main()
