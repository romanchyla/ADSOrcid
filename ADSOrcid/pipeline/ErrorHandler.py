from . import GenericWorker
        
class ErrorHandler(GenericWorker.RabbitMQWorker):
    def process_payload(self, msg):
        pass
