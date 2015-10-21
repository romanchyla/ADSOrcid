
# Connection to the database where we save our data
SQLALCHEMY_URL = 'sqlite:///'
SQLALCHEMY_ECHO = False

# URLs to get data from our own API
API_ENDPOINT = 'https://api.adsabs.harvard.edu'
API_SOLR_QUERY_ENDPOINT = API_ENDPOINT + '/v1/search/query/'
API_ORCID_EXPORT_PROFILE = API_ENDPOINT + '/v1/orcid/get-profile/%s'

# Token that only this service has (it must have necessary scopes)
API_TOKEN = 'fixme'

# The ORCID API public endpoint
API_ORCID_PROFILE_ENDPOINT = 'http://pub.orcid.org/v1.2/%s/orcid-bio'

# Ratio under which a name is considered as a poor match; it is using
# Levenshtein.ration() to compute similarity between two strings; if
# lower than this, we refuse to use the ORCID in the record
MIN_LEVENSHTEIN_RATIO = 0.6


# WARN, INFO, DEBUG
LOGGING_LEVEL = 'DEBUG'