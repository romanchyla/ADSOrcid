"""
Test base class to be used in all of the tests. Contains helper functions and
other common utilities that are used.
"""


import sys
import os

import unittest
import time
import json
import pika
from ADSOrcid import utils, app
from ..pipeline import pstart, workers, GenericWorker



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

        self.app = self.create_app()
        self.app.config.update(config)


class TestFunctional(TestUnit):
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
        
        super(TestFunctional, self).setUp()
        
        # Queues and routes are switched on so that they can allow workers
        # to connect
        app = self.app
        self.TM = pstart.TaskMaster(app.config.get('RABBITMQ_URL'),
                        'ads-orcid-test',
                        app.config.get('QUEUES'),
                        app.config.get('WORKERS'))
        self.connect_publisher()
        #self.TM.start_workers(verbose=True)
        
        
    def create_app(self):
        """Does not mess with a db, it expects it to exist"""
        app.init_app()
        return app
    

    def connect_publisher(self):
        """
        Makes a connection between the GenericWorker and the RabbitMQ instance, and
        sets up an attribute as a channel.

        :return: no return
        """

        self.publish_worker = GenericWorker.RabbitMQWorker()
        self.ret_queue = self.publish_worker.connect(self.app.config.get('RABBITMQ_URL'))

        
    def purge_all_queues(self):
        """
        Purges all the content from all the queues

        :return: no return
        """
        for worker, wconfig in self.app.config.get('WORKERS').iteritems():
            for x in ('publish', 'subscribe'):
                if x in wconfig and wconfig[x]:
                    try:
                        self.publish_worker.channel.queue_delete(queue=wconfig[x])
                    except pika.exceptions.ChannelClosed, e:
                        pass
        self.publish_worker.channel.exchange_delete(self.TM.exchange, if_unused=True)

    def tearDown(self):
        """
        General tearDown of the class. Purges the queues and then sleeps so that
        there is no contaminating the next set of tests.

        :return: no return
        """

        self.purge_all_queues()
        self.TM.stop_workers()
        self.TM = None
        




