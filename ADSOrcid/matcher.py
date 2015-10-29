from app import session_scope, config
from . import app
from .models import AuthorInfo
import requests
import json
import cachetools
import time

cache = cachetools.TTLCache(maxsize=1024, ttl=3600, timer=time.time, missing=None, getsizeof=None)

    
@cachetools.cached(cache)  
def retrieve_orcid(orcid):
    """
    Finds (or creates and returns) model of ORCID
    from the dbase
    
    :param orcid - String (orcid id)
    :return - OrcidModel datastructure
    """
    with session_scope() as session:
        u = session.query(AuthorInfo).filter_by(orcidid=orcid).first()
        if u is not None:
            return u
        u = create_orcid(orcid)
        session.add(u)
        session.commit()
        
        return session.query(AuthorInfo).filter_by(orcidid=orcid).first().toJSON()

   
def create_orcid(orcid, name=None, facts=None):
    """
    Creates an ORCID object and populates it with data
    (this endpoint will query the API to discover
    information about the author; so it is potentially
    expensive)
    
    :param: orcid - String, ORCID ID
    :param: name - String, name of the author (optional)
    :param: facts - dictionary of other facts we want to
        know/store (about the author)
    
    :return: AuthorInfo object
    """
    name = cleanup_name(name)
    
    # retrieve profile from our own orcid microservice
    if not name or not facts:
        profile = harvest_author_info(orcid, name, facts)
        name = name or profile['name']
        facts = profile

    return AuthorInfo(orcidid=orcid, name=name, facts=json.dumps(facts))


def harvest_author_info(orcidid, name=None, facts=None):
    """
    Does the hard job of querying public and private 
    API's for whatever information we want to collect
    about the ORCID ID;
    
    At this stage, we want to mainly retrieve author
    names (ie. variations of the author name)
    
    :param: orcidid - String
    :param: name - String, name of the author (optional)
    :param: facts - dict, info about the author
    
    :return: dict with various keys: name, author, author_norm, orcid_name
            (if available)
    """
    
    author_data = {}
    
    # first verify the public ORCID profile
    r = requests.get(config.get('API_ORCID_PROFILE_ENDPOINT') % orcidid,
                 headers={'Accept': 'application/json'})
    if r.status_code != 200:
        app.logger.error('We cant verify public profile of: http://orcid.org/%s' % orcidid)
    else:
        # we don't trust (the ugly) ORCID profiles too much
        # j['orcid-profile']['orcid-bio']['personal-details']['family-name']
        j = r.json()
        if 'orcid-profile' in j and 'orcid-bio' in j['orcid-profile'] \
            and 'personal-details' in j['orcid-profile']['orcid-bio'] and \
            'family-name' in j['orcid-profile']['orcid-bio']['personal-details'] and \
            'given-names' in j['orcid-profile']['orcid-bio']['personal-details']:
            
            author_data['orcid_name'] = ['%s, %s' % \
                (j['orcid-profile']['orcid-bio']['personal-details']['family-name']['value'],
                 j['orcid-profile']['orcid-bio']['personal-details']['given-names']['value'])]
                
                
    # search for the orcidid in our database (but only the publisher populated fiels)
    # we can't trust other fiels to bootstrap our database
    r = requests.get(
                '%(endpoint)s?q=%(query)s&fl=author,author_norm,orcid&rows=100&sort=pubdate+desc' % \
                {
                 'endpoint': config.get('API_SOLR_QUERY_ENDPOINT'),
                 'query' : 'orcid_publisher:%s' % cleanup_orcidid(orcidid),
                },
                headers={'Authorization': 'Bearer:%s' % config.get('API_TOKEN')})
    
    if r.status_code != 200:
        app.logger.error('Failed getting data from our own API! (err: %s)' % r.status_code)
        raise Exception(r.text)
    
    
    # go through the documents and collect all the names that correspond to the ORCID
    master_set = {}
    for doc in r.json()['response']['docs']:
        for k,v in _extract_names(orcidid, doc).items():
            if v:
                master_set.setdefault(k, {})
                n = cleanup_name(v)
                if not master_set[k].has_key(n):
                    master_set[k][n] = 0
                master_set[k][n] += 1
    
    # elect the most frequent name to become the 'author name'
    # TODO: this will choose the normalized names (as that is shorter)
    # maybe we should choose the longest (but it is not too important
    # because the matcher will be checking all name variants during
    # record update)
    mx = 0
    for k,v in master_set.items():
        author_data[k] = sorted(list(v.keys()))
        for name, freq in v.items():
            if freq > mx:
                author_data['name'] = name
    
    return author_data
    

def _extract_names(orcidid, doc):
    o = cleanup_orcidid(orcidid)
    r = {}
    if 'orcid' not in doc:
        raise Exception('Solr doc is missing orcid field')
    
    orcids = [cleanup_orcidid(x) for x in doc['orcid']]
    idx = None
    try:
        idx = orcids.index(o)
    except ValueError:
        raise Exception('Orcid %s is not present in the response for: %s' % (orcidid, doc))
    
    for f in 'author', 'author_norm':
        if f in doc:
            try:
                r[f] = doc[f][idx]
            except IndexError:
                raise Exception('The orcid %s should be at index: %s (but it wasnt)\n%s'
                                 % (orcidid, idx, doc))
    return r

    
def cleanup_orcidid(orcid):
    return orcid.replace('-', '').lower()

        
def cleanup_name(name):
    """
    Removes some unnecessary characters from the name
    """
    if not name:
        return ''
    name = name.replace('.', '')
    name = ' '.join(name.split())
    return name 
        
    