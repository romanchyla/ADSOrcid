from app import session_scope, config
from . import app
from .models import AuthorInfo, ChangeLog
import requests
import json
import cachetools
import time
from copy import deepcopy

"""
Tools for enhancing our knowledge about orcid ids (authors).
"""

cache = cachetools.TTLCache(maxsize=1024, ttl=3600, timer=time.time, missing=None, getsizeof=None)
orcid_cache = cachetools.TTLCache(maxsize=1024, ttl=3600, timer=time.time, missing=None, getsizeof=None)
ads_cache = cachetools.TTLCache(maxsize=1024, ttl=3600, timer=time.time, missing=None, getsizeof=None)
    
@cachetools.cached(cache)  
def retrieve_orcid(orcid):
    """
    Finds (or creates and returns) model of ORCID
    from the dbase. It will automatically update our
    knowledge about the author every time it gets
    called.
    
    :param orcid - String (orcid id)
    :return - OrcidModel datastructure
    """
    with session_scope() as session:
        u = session.query(AuthorInfo).filter_by(orcidid=orcid).first()
        if u is not None:
            return update_author(u)
        u = create_orcid(orcid)
        session.add(u)
        session.commit()
        
        return session.query(AuthorInfo).filter_by(orcidid=orcid).first().toJSON()

@cachetools.cached(orcid_cache)
def get_public_orcid_profile(orcidid):
    r = requests.get(config.get('API_ORCID_PROFILE_ENDPOINT') % orcidid,
                 headers={'Accept': 'application/json'})
    if r.status_code != 200:
        return None
    else:
        return r.json()

@cachetools.cached(ads_cache)
def get_ads_orcid_profile(orcidid):
    r = requests.get(config.get('API_ORCID_EXPORT_PROFILE') % orcidid,
                 headers={'Accept': 'application/json', 'Authorization': 'Bearer:%s' % config.get('API_TOKEN')})
    if r.status_code != 200:
        return None
    else:
        return r.json()


def update_author(author):
    """Updates existing AuthorInfo records. 
    
    It will check for new information. If there is a difference,
    updates the record and also records the old values.
    
    :param: author - AuthorInfo instance
    
    :return: AuthorInfo object
    
    :sideeffect: Will insert new records (ChangeLog) and also update
     the author instance
    """
    try:
        new_facts = harvest_author_info(author.orcidid)
    except:
        return author.toJSON()
    
    info = author.toJSON()
    with session_scope() as session:
        old_facts = info['facts']
        attrs = set(new_facts.keys())
        attrs = attrs.union(old_facts.keys())
        is_dirty = False
        
        for attname in attrs:
            if old_facts.get(attname, None) != new_facts.get(attname, None):
                session.add(ChangeLog(key='{0}:update:{1}'.format(author.orcidid, attname), 
                           oldvalue=json.dumps(old_facts.get(attname, None)),
                           newvalue=json.dumps(new_facts.get(attname, None))))
                is_dirty = True
        
        if bool(author.account_id) != bool(new_facts.get('authorized', False)):
            author.account_id = new_facts.get('authorized', False) and 1 or None 
        
        if is_dirty:
            author.facts = json.dumps(new_facts)
            author.name = new_facts.get('name', author.name)
            aid=author.id
            session.commit()
            return session.query(AuthorInfo).filter_by(id=aid).first().toJSON()
        else:
            return info
    
   
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

    return AuthorInfo(orcidid=orcid, name=name, facts=json.dumps(facts), account_id=facts.get('authorized', None))


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
    j = get_public_orcid_profile(orcidid)
    if j is None:
        app.logger.error('We cant verify public profile of: http://orcid.org/%s' % orcidid)
    else:
        # we don't trust (the ugly) ORCID profiles too much
        # j['orcid-profile']['orcid-bio']['personal-details']['family-name']
        if 'orcid-profile' in j and 'orcid-bio' in j['orcid-profile'] \
            and 'personal-details' in j['orcid-profile']['orcid-bio'] and \
            'family-name' in j['orcid-profile']['orcid-bio']['personal-details'] and \
            'given-names' in j['orcid-profile']['orcid-bio']['personal-details']:
            
            fname = j['orcid-profile']['orcid-bio']['personal-details'].get('family-name', {}).get('value', None)
            gname = j['orcid-profile']['orcid-bio']['personal-details'].get('given-names', {}).get('value', None)
            
            if fname and gname:
                author_data['orcid_name'] = ['%s, %s' % (fname, gname)]
                author_data['name'] = author_data['orcid_name'][0]
            
                
    # search for the orcidid in our database (but only the publisher populated fiels)
    # we can't trust other fiels to bootstrap our database
    r = requests.get(
                '%(endpoint)s?q=%(query)s&fl=author,author_norm,orcid_pub&rows=100&sort=pubdate+desc' % \
                {
                 'endpoint': config.get('API_SOLR_QUERY_ENDPOINT'),
                 'query' : 'orcid_pub:%s' % cleanup_orcidid(orcidid),
                },
                headers={'Authorization': 'Bearer %s' % config.get('API_TOKEN')})
    
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
    
    # get ADS data about the user
    # 0000-0003-3052-0819 | {"authorizedUser": true, "currentAffiliation": "Australian Astronomical Observatory", "nameVariations": ["Green, Andrew W.", "Green, Andy", "Green, Andy W."]}

    r = get_ads_orcid_profile(orcidid)
    if r:
        _author = r
        _info = _author.get('info', {}) or {}
        if _info.get('authorizedUser', False):
            author_data['authorized'] = True
        if _info.get('currentAffiliation', False):
            author_data['current_affiliation'] = _info['currentAffiliation']
        _vars = _info.get('nameVariations', None)
        if _vars:
            master_set.setdefault('author', {})
            for x in _vars:
                x = cleanup_name(x)
                v = master_set['author'].get(x, 1)
                master_set['author'][x] = v
    
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
    
    # automatically add the short names, because they make us find
    # more matches
    short_names = set()
    for x in ('author', 'orcid_name', 'author_norm'):
        if x in author_data and author_data[x]:
            for name in author_data[x]:
                for variant in _build_short_forms(name):
                    short_names.add(variant)
    if len(short_names):
        author_data['short_name'] = sorted(list(short_names))
    
    return author_data
    

def _build_short_forms(orig_name):
    orig_name = cleanup_name(orig_name)
    if ',' not in orig_name:
        return [] # refuse to do anything
    surname, other_names = orig_name.split(',', 1)
    ret = set()
    parts = filter(lambda x: len(x), other_names.split(' '))
    if len(parts) == 1 and len(parts[0]) == 1:
        return []
    for i in range(len(parts)):
        w_parts = deepcopy(parts)
        x = w_parts[i]
        if len(x) > 1:
            w_parts[i] = x[0]
            ret.add('{0}, {1}'.format(surname, ' '.join(w_parts)))
    w_parts = [x[0] for x in parts]
    while len(w_parts) > 0:
        ret.add('{0}, {1}'.format(surname, ' '.join(w_parts)))
        w_parts.pop()

    return list(ret)
    
        
    
def _extract_names(orcidid, doc):
    o = cleanup_orcidid(orcidid)
    r = {}
    if 'orcid_pub' not in doc:
        raise Exception('Solr doc is missing orcid field')
    
    orcids = [cleanup_orcidid(x) for x in doc['orcid_pub']]
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
    Removes some unnecessary characters from the name; 
    always returns a unicode
    """
    if not name:
        return u''
    if not isinstance(name, unicode):
        name = name.decode('utf8') # assumption, but ok...
    name = name.replace(u'.', u'')
    name = u' '.join(name.split())
    return name 
        
    