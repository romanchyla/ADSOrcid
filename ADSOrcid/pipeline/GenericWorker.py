from .. import utils
import pika
import sys
import json
import traceback

class RabbitMQWorker(object):
    """
    Base worker class. Defines the plumbing to communicate with rabbitMQ
    """

    def __init__(self, params=None):
        """
        Initialisation function (constructor) of the class

        :param params: dictionary of parameters
        :return: no return
        """
        params = params or {}
        
        self.params = params
        self.logger = self.setup_logging()
        self.connection = None
        self.results = None
        self.logger.debug('Initialized')
        self.exchange = params.get('exchange', None)
        self.publish_topic = None
        self.channel = None
        self.fwd_topic = None
        self.fwd_exchange = None
        if 'publish' in self.params and self.params['publish']:
            self.publish_topic = self.params['publish']
        

    def setup_logging(self, level='DEBUG'):
        """
        Sets up the generic logging of the worker

        :param level: level of the logging, INFO, DEBUG, WARN
        :return: no return
        """

        return utils.setup_logging(__file__, self.__class__.__name__)

    def connect(self, url, confirm_delivery=False):
        """
        Connect to RabbitMQ on <url>, and confirm the delivery

        :param url: URI of the RabbitMQ instance
        :param confirm_delivery: should the worker confirm delivery of packets
        :return: no return
        """

        try:
            self.connection = pika.BlockingConnection(pika.URLParameters(url))
            self.channel = self.connection.channel()
            if confirm_delivery:
                self.channel.confirm_delivery()
            self.channel.basic_qos(prefetch_count=1)
            
            for x in ('publish', 'subscribe'):
                if x in self.params and self.params[x]:
                    self.channel.queue_declare(queue=self.params[x], passive=True)
                    
            if self.params.get('forwarding'):
                fwd = self.params.get('forwarding')
                self.fwd_connection = pika.BlockingConnection(pika.URLParameters(fwd.get('url', url)))
                self.fwd_channel = self.fwd_connection.channel()
                if fwd.get('confirm_delivery', confirm_delivery):
                    self.fwd_channel.confirm_delivery()
                    self.fwd_channel.basic_qos(prefetch_count=1)
                if fwd.get('publish'):
                    self.fwd_channel.queue_declare(queue=fwd['publish'], passive=True)
                    self.fwd_topic = fwd['publish']
                if fwd.get('exchange'):
                    self.fwd_exchange = fwd['exchange']
                else:
                    raise Exception('exchange must be specified for forwarding')
                    
            return True
        except:
            self.logger.error(sys.exc_info())
            raise Exception(sys.exc_info())


    def publish_to_error_queue(self, message, exchange=None, routing_key=None,
                               **kwargs):
        """
        Publishes messages to the error queue. Prefixes the message with the
        worker that passed on the message.

        :param message: message received from the queue
        :param exchange: name of the exchange that contains the error queue
        :param routing_key: routing key for the error queue
        :param kwargs: extra keywords that may be needed
        :return: no return
        """
        if not exchange:
            exchange = self.params.get('exchange', 'ads-orcid')

        if not routing_key:
            routing_key = self.params.get('error', 'ads.orcid.error')

        self.publish(message, topic=routing_key, properties=kwargs['header_frame'])


    def forward(self, message, topic=None, **kwargs):
        """
        Forwards the message to another server/exchange/queue
        """
        
        if not (topic or self.fwd_topic):
            self.logger.error('Whaaaat? No forwarding topic/queue configured!')
            return
        
        if not self.fwd_channel:
            raise Exception('You must connect to a channel before caling forward()')
        
        self.logger.debug('Publish to {0} using topic {1}'.format(
                                    self.fwd_exchange,
                                    topic or self.fwd_topic))
        
        if not isinstance(message, basestring):
            message = json.dumps(message)
        self.fwd_channel.basic_publish(exchange=self.fwd_exchange,
                                   routing_key=topic or self.fwd_topic,
                                   body=message)
        
        
    def publish(self, message, topic=None, **kwargs):
        """
        Publishes messages to the queue. Uses the generic template for the
        relevant worker, which is defined in the pipeline settings module.

        :param message: message to be publishes
        :param topic: String (the routing key) - overrides this worker's 
               routing key
        :param kwargs: extra keywords that may be needed
        :return: no return
        """

        if not (topic or self.publish_topic):
            self.logger.error('Whaaaat? No topic/queue configured!')
            return
        
        if not self.channel:
            self.logger.error('You must connect to a channel before caling publish()')
            return
        
        self.logger.debug('Publish to {0} using topic {1}'.format(
                                    self.exchange,
                                    topic or self.publish_topic))
        
        if not isinstance(message, basestring):
            message = json.dumps(message)
        self.channel.basic_publish(exchange=self.exchange,
                                   routing_key=topic or self.publish_topic,
                                   body=message)
        

    def subscribe(self, callback, **kwargs):
        """
        Starts the worker consuming from the relevant queue defined in the
        pipeline settings module, for that worker.

        :param callback: the function called by the worker when it consumes
        :param kwargs: extra keyword arguments
        :return: no return
        """

        if 'subscribe' in self.params and self.params['subscribe']:
            self.logger.debug('Subscribing to: {0}'.format(self.params['subscribe']))
            self.channel.basic_consume(callback, queue=self.params['subscribe'], **kwargs)

            if not self.params.get('TEST_RUN', False):
                self.logger.debug('Worker consuming from queue: {0}'.format(
                    self.params['subscribe']))
                self.channel.start_consuming()

    
    def process_payload(self, payload, 
                        channel=None, 
                        method_frame=None, 
                        header_frame=None):
        """Please provide your own implementation"""
        raise NotImplementedError("Missing impl of process_payload")
        

    def on_message(self, channel, method_frame, header_frame, body):
        """
        Default skeleton for processing data (you have to provide
        process_payload method)

        :param channel: the channel instance for the connected queue
        :param method_frame: contains delivery information of the packet
        :param header_frame: contains header information of the packet
        :param body: contains the message inside the packet
        :return: no return
        """

        message = json.loads(body)
        try:
            self.logger.debug('Running on message')
            self.results = self.process_payload(message, 
                                                channel=channel, 
                                                method_frame=method_frame, 
                                                header_frame=header_frame)
        except Exception, e:
            self.results = 'Offloading to ErrorWorker due to exception:' \
                           ' {0}'.format(e.message)

            self.logger.warning('Offloading to ErrorWorker due to exception: '
                                '{0} ({1})'.format(e.message,
                                                   traceback.format_exc()))

            self.publish_to_error_queue(json.dumps(
                {self.__class__.__name__: message}),
                header_frame=header_frame
            )

        # Send delivery acknowledgement
        self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)


    def run(self):
        """
        Wrapper function that both connects the worker to the RabbitMQ instance
        and starts it consuming messages.
        :return: no return
        """

        self.connect(self.params['RABBITMQ_URL'])
        self.subscribe(self.on_message)