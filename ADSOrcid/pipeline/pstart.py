#!/usr/bin/env python

"""
Pipeline to extract full text documents. It carries out the following:

  - Initialises the queues to be used in RabbitMQ
  - Starts the workers and connects them to the queue
"""


import sys
import os


import multiprocessing
import threading
import time
import signal
import sys
import os
from ADSOrcid import app
from ADSOrcid.pipeline import workers, GenericWorker
from ADSOrcid.utils import setup_logging
from copy import deepcopy
from threading import Thread


logger = setup_logging(os.path.abspath(os.path.join(__file__, '..')), __name__)


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
        self.initialize_rabbitmq()

    def quit(self, os_signal=None, frame=None):
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

        w = GenericWorker.RabbitMQWorker()
        w.connect(self.rabbitmq_url)
        
        # make sure the exchange is there
        w.channel.exchange_declare(
                        exchange=self.exchange,
                        passive=False,
                        durable=True,
                        internal=False,
                        type='topic')
        
        # make sure queues exists
        queues = {}
        if self.rabbitmq_routes:
            for qname, qvals in self.rabbitmq_routes.items():
                if qname not in queues:
                    queues[qname] = qvals.has_key('durable') and qvals['durable']
                w.channel.queue_declare(
                            queue=qname, 
                            passive=False, 
                            durable=queues[qname], 
                            exclusive=False, 
                            auto_delete=False)
                # make sure messages are properly routed
                w.channel.queue_bind(
                    queue=qname, 
                    exchange=self.exchange, 
                    routing_key=qvals['routing_key'] or qname)
            
        for worker in self.workers.values():
            if worker.get('subscribe', None):
                qname = worker['subscribe']
                if qname not in queues:
                    queues[qname] = worker.has_key('durable') and worker['durable']
                    
                w.channel.queue_declare(
                            queue=worker['subscribe'],
                            durable = queues[qname], 
                            passive=False,  
                            exclusive=False, 
                            auto_delete=False)
                
                # make sure messages are properly routed
                w.channel.queue_bind(
                    queue=qname, 
                    exchange=self.exchange, 
                    routing_key=qname)
                
            if worker.get('publish', None):
                qname = worker['publish']
                if qname not in queues:
                    queues[qname] = worker.has_key('durable') and worker['durable']

                w.channel.queue_declare(
                            queue=worker['publish'], 
                            passive=False,  
                            exclusive=False,
                            durable=queues[qname], 
                            auto_delete=False)
                
                # make sure messages are properly routed
                w.channel.queue_bind(
                    queue=qname, 
                    exchange=self.exchange, 
                    routing_key=qname)
        
        w.connection.close()


    def poll_loop(self, poll_interval=60, ttl=7200,
                  extra_params=False, verbose=False):
        """
        Starts all of the workers connecting and consuming to the queue. It then
        continually polls the workers to ensure that the correct number exists,
        in case one has died.

        :param poll_interval: how often to poll
        :param ttl: time to live, how long before it tries to restart workers
        :param extra_params: other parameters
        :return: no return
        """
        self = self # to make pylint happy
        self.running = True
        
        def check_pool():
            while self.running:
                self.start_workers(verbose=verbose, extra_params=extra_params)
                
                for worker, params in self.workers.iteritems():
                    for active in params['active']:
                        if not active['proc'].is_alive():
                            logger.debug('{0} is not alive, restarting: {1}'.format(
                                active['proc'], worker))
                            if hasattr(active['proc'], 'terminate'):
                                active['proc'].terminate()
                            active['proc'].join(3)
                            if not active['proc'].is_alive():
                                params['active'].remove(active)
                            continue
                        if ttl:
                            if time.time()-active['start'] > ttl:
                                logger.debug('time to live reached')
                                if hasattr(active['proc'], 'terminate'):
                                    active['proc'].terminate()
                                active['proc'].join(3)
                                active['proc'].is_alive()
                                params['active'].remove(active)
                
                time.sleep(poll_interval)

        def quit():
            self.quit()
            
        # Define the SIGTERM handler
        if isinstance(threading.current_thread(), threading._MainThread):
            signal.signal(signal.SIGTERM, quit)
        poll_thread = Thread(target=check_pool)
        poll_thread.start()
        poll_thread.join()
        
        
        
        


    def start_workers(self, verbose=True, extra_params=False):
        """
        Starts the workers and the relevant number of them wanted by the user,
        which is defined with the app config

        :param verbose: if the messages should be verbose
        :param extra_params: other parameters
        :return: no return
        """
        
        class WorkerThread(threading.Thread):
            def __init__(self, worker, *args, **kwargs):
                threading.Thread.__init__(self, *args, **kwargs)
                self._worker = worker
            def run(self):
                self._worker.run()
            def terminate(self):
                self._worker.terminate()
            
        for worker, params in self.workers.iteritems():
            
            conc = params.get('concurrency', 1)
            if len(params.get('active', [])) >= conc:
                continue
            
            logger.debug('Starting worker: {0}'.format(worker))
            params['active'] = params.get('active', [])
            params['RABBITMQ_URL'] = self.rabbitmq_url
            params['exchange'] = self.exchange
            
            if isinstance(extra_params, dict):
                for par in extra_params:
                    logger.debug('Adding extra content: [{0}]: {1}'.format(
                        par, extra_params[par]))
    
                    params[par] = extra_params[par]
            
            while len(params['active']) < conc:
                w = eval('workers.{0}.{0}'.format(worker))(params)
                
                # decide if we want to run it multiprocessing
                if conc > 1:
                    process = multiprocessing.Process(target=w.run)
                else:
                    process = WorkerThread(w)
                
                #process.daemon = True
                process.start()

                if verbose:
                    logger.debug('Started {0}-{1}'.format(worker, process.name))

                params['active'].append({
                    'proc': process,
                    'start': time.time(),
                })

            logger.debug('Successfully started: {0}'.format(
                len(params['active'])))


    def stop_workers(self, waittime=2):
        """
        Stops the running workers.

        :return: no return
        """
        self.running = False
        for worker, params in self.workers.iteritems():
            if 'active' in params:
                for active in params['active']:
                    if active['proc'].is_alive():
                        logger.debug('{0} is alive, stopping: {1}'.format(
                            active['proc'], worker))
                        if hasattr(active['proc'], 'terminate'):
                            active['proc'].terminate()
        time.sleep(waittime)
        for worker, params in self.workers.iteritems():
            for active in params.get('active', []):
                active['proc'].join(0.01)
                if active['proc'].is_alive():
                    logger.error('Process is still alive (despite termination): {}'.format(active))
                params['active'].remove(active)


def start_pipeline(params_dictionary=False, application=None):
    """
    Starts the TaskMaster that starts the queues needed for full text
    extraction. Defines how the system can be stopped, and begins the polling
    of the workers. This is the main application of ADSfulltext.

    :param params_dictionary: parameters needed for the workers and polling
    :return: no return
    """
    
    app = application or app
    
    task_master = TaskMaster(app.config.get('RABBITMQ_URL'),
                    app.config.get('EXCHANGE'),
                    app.config.get('QUEUES', None),
                    app.config.get('WORKERS'))

    # Start the main process in a loop
    task_master.poll_loop(extra_params=params_dictionary, 
                          poll_interval=app.config.get('POLL_INTERVAL', 15))


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
    
    app.init_app()
    start_pipeline(params_dictionary, app)


if __name__ == '__main__':
    main()