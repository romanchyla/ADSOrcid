"""
Test base class to be used in all of the tests. Contains helper functions and
other common utilities that are used.
"""


import sys
import os

import unittest
import time
import json
from ADSOrcid import utils
from ..pipeline import psettings, pstart
from ..pipeline import worker



class TestUnit(unittest.TestCase):
    """
    Default unit test class. It sets up the stub data required
    """
    def setUp(self):
        config = utils.load_config()
        
        #update PROJ_HOME since normally it is run from higher leve
        config['PROJ_HOME'] = os.path.abspath(config['PROJ_HOME'] + '/..')
        
        config['TEST_UNIT_DIR'] = os.path.join(config['PROJ_HOME'],
                         'ADSOrcid/tests/test_unit')
        config['TEST_INTGR_DIR'] = os.path.join(config['PROJ_HOME'],
                         'ADSOrcid/tests/test_integration')
        config['TEST_FUNC_DIR'] = os.path.join(config['PROJ_HOME'],
                         'ADSOrcid/tests/test_functional')
        self.config = config
        self.app = self.create_app()


class TestFunctional(unittest.TestCase):
    """
    Generic test class. Used as the primary class that implements a standard
    integration test. Also contains a range of helper functions, and the correct
    tearDown method when interacting with RabbitMQ.
    """

    def setUp(self):
        """
        Sets up the parameters for the RabbitMQ workers, and also the workers
        themselves. Generates all the queues that should be in place for testing
        the RabbitMQ workers.

        :return: no return
        """
        

        # Queues and routes are switched on so that they can allow workers
        # to connect
        TM = pstart.TaskMaster(psettings.RABBITMQ_URL,
                        'ads-orcid-test',
                        psettings.QUEUES,
                        psettings.WORKERS)
        TM.initialize_rabbitmq()

        self.connect_publisher()
        self.purge_all_queues()

    def connect_publisher(self):
        """
        Makes a connection between the worker and the RabbitMQ instance, and
        sets up an attribute as a channel.

        :return: no return
        """

        self.publish_worker = worker.RabbitMQWorker()
        self.ret_queue = self.publish_worker.connect(psettings.RABBITMQ_URL)

    def purge_all_queues(self):
        """
        Purges all the content from all the queues existing in psettings.py.

        :return: no return
        """
        for queue in psettings.QUEUES:
            _q = queue['queue']
            self.publish_worker.channel.queue_purge(queue=_q)

    def tearDown(self):
        """
        General tearDown of the class. Purges the queues and then sleeps so that
        there is no contaminating the next set of tests.

        :return: no return
        """

        self.purge_all_queues()
        time.sleep(5)




