import os

# possible values: WARN, INFO, DEBUG
LOGGING_LEVEL = 'DEBUG'

# Connection to the database where we save orcid-claims (this database
# serves as a running log of claims and storage of author-related
# information). It is not consumed by others (ie. we 'push' results) 
# SQLALCHEMY_URL = 'postgres://docker:docker@localhost:6432/docker'
SQLALCHEMY_URL = 'sqlite:///'
SQLALCHEMY_ECHO = False


# Celery related configuration
# All work we do is concentrated into one exchange (the queues are marked
# by topics, e.g. ads.orcid.claims); The queues will be created automatically
# based on the workers' definition. If 'durable' = True, it means that the 
# queue is created as permanent *AND* the worker will publish 'permanent'
# messages. Ie. if rabbitmq goes down/restarted, the uncomsumed messages will
# still be there 


#CELERY_DEFAULT_EXCHANGE = 'orcid_pipeline'
#CELERY_DEFAULT_EXCHANGE_TYPE = "topic"
CELERY_INCLUDE = ['ADSOrcid.tasks']
ACKS_LATE=True
PREFETCH_MULTIPLIER=1
CELERYD_TASK_SOFT_TIME_LIMIT = 60
CELERY_BROKER = 'pyamqp://'


# Where to send results (of our processing); since we rely on Celery, we have
# to specify the task id - which is the worker's module on the remote side
# that will be handling the message. This is a limitation of the current setup.
# TODO: find a way to send a queue to the remote queue and let Celery deliver
# it to the appropriate worker without having to specify it's name
OUTPUT_CELERY_BROKER = 'pyamqp://guest:guest@localhost:6672/master_pipeline'
OUTPUT_TASKNAME = 'adsmp.tasks.task_update_record'
#OUTPUT_EXCHANGE = 'master_pipeline'
OUTPUT_QUEUE = 'update-record'

               


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
# Experimental results show 0.69 to be the best value.
MIN_LEVENSHTEIN_RATIO = 0.69



# order in which the identifiers (inside an orcid profile) will be tested
# to retrieve a canonical bibcode; first match will stop the process. Higher number
# means 'higher priority'
# the '*' will be used for no-match, if this number is <0, the identifier will be skipped
ORCID_IDENTIFIERS_ORDER = {'bibcode': 9, 'doi': 8, 'arxiv': 7, '*': 0}