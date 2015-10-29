"""
Functional test

Loads the ADSOrcid workers. It then injects input onto the RabbitMQ instance. Once
processed it then checks all things were written where they should. 
It then shuts down all of the workers.
"""


import sys
import os


import run
import os
import time
import subprocess
import string
import unittest
from .. import test_base


class TestExtractWorker(test_base.TestFunctional):
    """
    Class for testing the overall functionality of the ADSOrcid pipeline.
    The interaction between the pipeline workers.
    """


    def test_functionality_on_new_claim(self):
        """
        Main test, it pretends we have received claims from the 
        ADSWS

        :return: no return
        """

        time.sleep(1)
        
        # create an orcid claim

if __name__ == '__main__':
    unittest.main()