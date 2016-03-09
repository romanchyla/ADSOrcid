# Connection to the database where we save orcid-claims (this database
# serves as a running log of claims and storage of author-related
# information). It is not consumed by others (ie. we 'push' results) 
# SQLALCHEMY_URL = 'postgres://docker:docker@localhost:6432/docker'
SQLALCHEMY_URL = 'sqlite:///'
SQLALCHEMY_ECHO = False

# One of the workers is (currently) writing orcid claims into MongoDB
# this is the same db as for ADSClassic<->adsdata synchronization
MONGODB_URL = 'mongodb://localhost:37017/adsdata'


# Configuration of the pipeline; if you start 'vagrant up rabbitmq' 
# container, the port is localhost:8072 - but for production, you 
# want to point to the ADSImport pipeline 
RABBITMQ_URL = 'amqp://guest:guest@localhost:6672/?' \
               'socket_timeout=10&backpressure_detection=t'
               


# URLs to get data from our own API, the token must give us
# access to the orcid microservice + access to the info about
# a user (highly privileged access, so make sure you are not
# exposing it!)
API_ENDPOINT = 'https://api.adsabs.harvard.edu'
API_SOLR_QUERY_ENDPOINT = API_ENDPOINT + '/v1/search/query/'
API_ORCID_EXPORT_PROFILE = API_ENDPOINT + '/v1/orcid/get-profile/%s'
API_ORCID_UPDATES_ENDPOINT = API_ENDPOINT + '/v1/orcid/export/%s'
API_TOKEN = 'fixme'

# The ORCID API public endpoint
API_ORCID_PROFILE_ENDPOINT = 'http://pub.orcid.org/v1.2/%s/orcid-bio'

# Levenshtein.ration() to compute similarity between two strings; if
# lower than this, we refuse to match names, eg.
# Levenshtein.ratio('Neumann, John', 'Neuman, J')
# > Out[2]: 0.8181818181818182
MIN_LEVENSHTEIN_RATIO = 0.8


# possible values: WARN, INFO, DEBUG
LOGGING_LEVEL = 'DEBUG'

               
POLL_INTERVAL = 15  # per-worker poll interval (to check health) in seconds.

# All work we do is concentrated into one exchange (the queues are marked
# by topics, e.g. ads.orcid.claims); The queues will be created automatically
# based on the workers' definition. If 'durable' = True, it means that the 
# queue is created as permanent *AND* the worker will publish 'permanent'
# messages. Ie. if rabbitmq goes down/restarted, the uncomsumed messages will
# still be there 
EXCHANGE = 'ads-orcid'

WORKERS = {
    'OrcidImporter': {
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
    'ClaimsRecorder': {
        'concurrency': 1,
        'subscribe': 'ads.orcid.updates',
        'publish': 'ads.orcid.output',
        'error': 'ads.orcid.error',
        'durable': True
    },
    'OutputHandler': {
        'subscribe': 'ads.orcid.output',
        'publish': None,
        'durable': True,
        'forwarding': {
            'exchange': 'MergerPipelineExchange',
            'publish': 'SolrUpdateQueue'
        }
    },
    'ErrorHandler': {
        'subscribe': None,
        'exchange': None,
        'publish': None,
        'durable' : False
    },
    
}