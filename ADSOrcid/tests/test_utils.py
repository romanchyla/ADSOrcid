#!/usr/bin/env python
# -*- coding: utf-8 -*-


import unittest
from ADSOrcid import utils 

class Test(unittest.TestCase):
    def test_get_date(self):
        """Check we always work with UTC dates"""
        
        d = utils.get_date()
        self.assertTrue(d.tzname() == 'UTC')
        
        d1 = utils.get_date('2009-09-04T01:56:35.450686Z')
        self.assertTrue(d1.tzname() == 'UTC')
        self.assertEqual(d1.isoformat(), '2009-09-04T01:56:35.450686+00:00')
        
        d2 = utils.get_date('2009-09-03T20:56:35.450686-05:00')
        self.assertTrue(d2.tzname() == 'UTC')
        self.assertEqual(d2.isoformat(), '2009-09-04T01:56:35.450686+00:00')
        
        d3 = utils.get_date('2009-09-03T20:56:35.450686')
        self.assertTrue(d3.tzname() == 'UTC')
        self.assertEqual(d3.isoformat(), '2009-09-03T20:56:35.450686+00:00')
        
if __name__ == '__main__':
    unittest.main()        