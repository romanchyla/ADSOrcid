"""
Settings for the rabbitMQ/ADSOrcid
"""


# Travis-CI uses guest:guest
# Max message size = 500kb
RABBITMQ_URL = 'amqp://guest:guest@localhost:8072/?' \
               'socket_timeout=10&backpressure_detection=t'
               
POLL_INTERVAL = 15  # per-worker poll interval (to check health) in seconds.

EXCHANGE = 'ads-orcid'

WORKERS = {
    'ClaimsIngestWorker': {
        'concurrency': 1,
        'subscribe': 'ClaimsQueue',
        'publish': 'ads.orcid.claims',
        'error': 'ads.orcid.error',
        'durable': True
    },
    'ErrorHandler': {
        'subscribe': None,
        'exchange': None,
        'publish': None,
        'durable' : False
    }
}

QUEUES = {
    'ImportQueue': {
            'routing_key': 'ads.orcid.import',
            'durable': True
        },
    'ClaimsQueue': {
            'routing_key': 'ads.orcid.claims',
            'durable': True
        }
}


# For production/testing environment
try:
    from local_psettings import *

except ImportError as e:
    pass
