#!/usr/bin/env python

"""
Pipeline to extract full text documents. It carries out the following:

  - Initialises the queues to be used in RabbitMQ
  - Starts the workers and connects them to the queue
"""


import sys
import os


import psettings
import multiprocessing
import threading
import time
import signal
import sys
from ADSOrcid.pipeline import pworkers as workers
from ADSOrcid.pipeline.worker import RabbitMQWorker
from ADSOrcid.utils import setup_logging
from copy import deepcopy


logger = setup_logging(__file__, __name__)


class Singleton(object):
    """
    Singleton type class. Collates a list of the class instances.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = \
                super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class TaskMaster(Singleton):
    """
    Class that starts, stops, and controls the workers that connect to the
    RabbitMQ instance running
    """

    def __init__(self, rabbitmq_url, exchange, rabbitmq_routes, workers):
        """
        Initialisation function (constructor) of the class

        :param rabbitmq_url: URI of the RabbitMQ instance
        :param exchange: the name of the main exchange
        :param rabbitmq_routes: list of routes that should exist
        :param workers: list of workers that should be started
        :return: no return
        """
        self.rabbitmq_url = rabbitmq_url
        self.exchange = exchange
        self.rabbitmq_routes = deepcopy(rabbitmq_routes)
        self.workers = deepcopy(workers)
        self.running = False

    def quit(self, os_signal, frame):
        """
        Stops all RabbitMQ workers when it receives a SIGTERM or other type of
        SIGKILL from the OS

        :param os_signal: signal received from the OS, e.g., SIGTERM
        :param frame: packet information
        :return:
        """

        try:
            logger.info(
                'Got SIGTERM to stop workers, attempt graceful shutdown.')

            self.stop_workers()

        except Exception as err:
            logger.warning('Workers not stopped gracefully: {0}'.format(err))

        finally:
            self.running = False
            sys.exit(0)

    def initialize_rabbitmq(self):
        """
        Sets up the correct routes, exchanges, and bindings on the RabbitMQ
        instance

        :return: no return
        """

        w = RabbitMQWorker()
        w.connect(self.rabbitmq_url)
        
        # make sure the exchange is there
        w.channel.exchange_declare(
                        exchange=self.exchange,
                        passive=False,
                        durable=True,
                        internal=False,
                        type='topic')
        
        # make sure queues exists
        if self.rabbitmq_routes:
            for qname, qvals in self.rabbitmq_routes.items():
                w.channel.queue_declare(
                            queue=qname, 
                            passive=False, 
                            durable=qvals.has_key('durable') and qvals['durable'], 
                            exclusive=False, 
                            auto_delete=False)
                # make sure messages are properly routed
                w.channel.queue_bind(
                    queue=qname, 
                    exchange=self.exchange, 
                    routing_key=qvals['routing_key'])
            
        for worker in self.workers:
            if 'subscribe' in worker:
                w.channel.queue_declare(
                            queue=worker['subscribe'], 
                            passive=False,  
                            exclusive=False, 
                            auto_delete=False)
            if 'publish' in worker:
                w.channel.queue_declare(
                            queue=worker['publish'], 
                            passive=False,  
                            exclusive=False,
                            durable=worker.has_key('durable') and worker['durable'], 
                            auto_delete=False)
        
        w.connection.close()


    def poll_loop(self, poll_interval=60, ttl=7200,
                  extra_params=False):
        """
        Starts all of the workers connecting and consuming to the queue. It then
        continually polls the workers to ensure that the correct number exists,
        in case one has died.

        :param poll_interval: how often to poll
        :param ttl: time to live, how long before it tries to restart workers
        :param extra_params: other parameters
        :return: no return
        """

        while self.running:

            time.sleep(poll_interval)
            for worker, params in self.workers.iteritems():
                for active in params['active']:
                    if not active['proc'].is_alive():

                        logger.debug('{0} is not alive, restarting: {1}'.format(
                            active['proc'], worker))
                        if hasattr(active['proc'], 'terminate'):
                            active['proc'].terminate()
                        active['proc'].join()
                        if not active['proc'].is_alive():
                            params['active'].remove(active)
                        continue
                    if ttl:
                        if time.time()-active['start'] > ttl:
                            logger.debug('time to live reached')
                            if hasattr(active['proc'], 'terminate'):
                                active['proc'].terminate()
                            active['proc'].join()
                            active['proc'].is_alive()
                            params['active'].remove(active)

            self.start_workers(verbose=False, extra_params=extra_params)

    def start_workers(self, verbose=True, extra_params=False):
        """
        Starts the workers and the relevant number of them wanted by the user,
        which is defined with the pipeline settings module, psettings.py.

        :param verbose: if the messages should be verbose
        :param extra_params: other parameters
        :return: no return
        """

        for worker, params in self.workers.iteritems():
            logger.debug('Starting worker: {0}'.format(worker))
            params['active'] = params.get('active', [])
            params['RABBITMQ_URL'] = self.rabbitmq_url
            params['exchange'] = self.exchange

            for par in extra_params:
                logger.debug('Adding extra content: [{0}]: {1}'.format(
                    par, extra_params[par]))

                params[par] = extra_params[par]
            
            conc = params.get('concurrency', 1)
            while len(params['active']) < conc:
                w = eval('workers.{0}'.format(worker))(params)
                
                # decide if we want to run it multiprocessing
                if conc > 1:
                    process = multiprocessing.Process(target=w.run)
                else:
                    process = threading.Thread(target=w.run, args=())
                
                process.daemon = True
                process.start()

                if verbose:
                    logger.debug('Started {0}-{1}'.format(worker, process.name))

                params['active'].append({
                    'proc': process,
                    'start': time.time(),
                })

            logger.debug('Successfully started: {0}'.format(
                len(params['active'])))

        self.running = True

    def stop_workers(self):
        """
        Stops the workers. Currently it does nothing as closing the main process
        should gracefully clean up each daemon process.

        :return: no return
        """
        pass


def start_pipeline(params_dictionary=False):
    """
    Starts the TaskMaster that starts the queues needed for full text
    extraction. Defines how the system can be stopped, and begins the polling
    of the workers. This is the main application of ADSfulltext.

    :param params_dictionary: parameters needed for the workers and polling
    :return: no return
    """

    task_master = TaskMaster(psettings.RABBITMQ_URL,
                    psettings.EXCHANGE,
                    'QUEUES' in psettings and psettings.QUEUES or None,
                    psettings.WORKERS)

    task_master.initialize_rabbitmq()
    task_master.start_workers(extra_params=params_dictionary)

    # Define the SIGTERM handler
    signal.signal(signal.SIGTERM, task_master.quit)

    # Start the main process in a loop
    task_master.poll_loop(extra_params=params_dictionary, 
                          poll_interval=psettings.POLL_INTERVAL)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Process user input.')

    parser.add_argument('--no-consume-queue',
                        dest='test_run',
                        action='store_true',
                        help='Worker will exit the queue after consuming a '
                             'single message.')

    parser.add_argument('--consume-queue',
                        dest='test_run',
                        action='store_false',
                        help='Worker will sit on the queue, continuously '
                             'consuming.')


    parser.set_defaults(test_run=False)
    args = parser.parse_args()

    params_dictionary = {'TEST_RUN': args.test_run}
    start_pipeline(params_dictionary)


if __name__ == '__main__':
    main()