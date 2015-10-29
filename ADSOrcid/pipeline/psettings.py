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
    'ClaimsImporter': {
        'concurrency': 1,
        'subscribe': 'ads.orcid.fresh-claims',
        'publish': 'ads.orcid.claims',
        'error': 'ads.orcid.error',
        'durable': True
    },
    'ClaimsIngester': {
        'concurrency': 1,
        'subscribe': 'ads.orcid.claims',
        'publish': 'ads.orcid.updates',
        'error': 'ads.orcid.error',
        'durable': True
    },
    'MongoUpdater': {
        'concurrency': 1,
        'subscribe': 'ads.orcid.updates',
        'publish': None,
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



# For production/testing environment
try:
    from local_psettings import *

except ImportError as e:
    pass
