from . import GenericWorker
        
class ErrorHandler(GenericWorker.RabbitMQWorker):
    def process_payload(self, msg, *args, **kwargs):
        self.logger.warn(u'\nmsg={}\nargs={}\nkwargs={}'.format(msg, args, kwargs))
