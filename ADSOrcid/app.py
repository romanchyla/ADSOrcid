

from .models import ClaimsLog, Records, AuthorInfo, ChangeLog
from adsputils import get_date, setup_logging, load_config, ADSCelery
from ADSOrcid import names
from ADSOrcid.exceptions import IgnorableException
from celery import Celery
from contextlib import contextmanager
from dateutil.tz import tzutc
from sqlalchemy import and_, or_
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
import cachetools
import datetime
import json
import os
import random
import requests
import time
import traceback
from ADSOrcid.models import AuthorInfo
import adsputils


# global objects; we could make them belong to the app object but it doesn't seem necessary
# unless two apps with a different endpint/config live along; TODO: move if necessary
cache = cachetools.TTLCache(maxsize=1024, ttl=3600, timer=time.time, missing=None, getsizeof=None)
orcid_cache = cachetools.TTLCache(maxsize=1024, ttl=3600, timer=time.time, missing=None, getsizeof=None)
ads_cache = cachetools.TTLCache(maxsize=1024, ttl=3600, timer=time.time, missing=None, getsizeof=None)
bibcode_cache = cachetools.TTLCache(maxsize=2048, ttl=3600, timer=time.time, missing=None, getsizeof=None)

ALLOWED_STATUS = set(['claimed', 'updated', 'removed', 'unchanged', 'forced', '#full-import'])



def clear_caches():
    """Clears all the module caches."""
    cache.clear()
    orcid_cache.clear()
    ads_cache.clear()
    bibcode_cache.clear()


class ADSOrcidCelery(ADSCelery):
    
    
    def insert_claims(self, claims):
        """
        Build a batch of claims and saves them into a database
        
        :param: claims - list of json values, with claims
                       - or list of claims (ClaimLog) instances
        :return number of claims that were successfuly added
                to the database
        """
        res = []
        with self.session_scope() as session:
            for c in claims:
                if isinstance(c, ClaimsLog):
                    claim = c
                else:
                    claim = self.create_claim(**c)
                if claim:
                    session.add(claim)
                    res.append(claim)
            session.commit()
            res = [x.toJSON() for x in res]
        return res
    
    def create_claim(self, 
                 bibcode=None, 
                 orcidid=None, 
                 provenance=None, 
                 status=None, 
                 date=None, 
                 force_new=True,
                 **kwargs):
        """
        Inserts (or updates) ClaimLog entry.
        
        :return: ClaimsLog instance (however this is only for reading, you should
            not try to do anything with it; the session will have been closed already)
        """
        assert(orcidid)
        if isinstance(date, basestring):
            date = get_date(date)
        if status and status.lower() not in ALLOWED_STATUS:
            raise Exception('Unknown status %s' % status)
        
        if not date or force_new is True: # we don't need to verify the record exists
            return ClaimsLog(bibcode=bibcode, 
                      orcidid=orcidid,
                      provenance=provenance, 
                      status=status,
                      created=date or get_date())
        else:
            with self.session_scope() as session:
                f = session.query(ClaimsLog).filter_by(created=date).first()
                if f and f.bibcode == bibcode and f.orcidid == orcidid:
                    f.provenance = provenance
                    f.status = status
                    session.expunge(f)
                    return f
                else:
                    return ClaimsLog(bibcode=bibcode, 
                      orcidid=orcidid,
                      provenance=provenance, 
                      status=status,
                      created=date)


    def import_recs(self, input_file, default_provenance=None, 
                default_status='claimed', collector=None):
        """
        Imports (creates log records) of claims from
        :param: input_file - String, path to the file with the following 
                information (tab delimited):
                    bibcode
                    orcid_id
                    provenance - optional
                    status - optional
                    date - optional
        :param: default_provenance - String, this will be used if the records
                don't provide provenance
        :param: default_status - String, used when status is not supplied
        :param: collector - if passed in, the results will be inserted
                into it
        :type: array
        """
        
        if not os.path.exists(input_file):
            raise Exception('{file} does not exist'.format(
                               file=input_file
                               ))
        if collector is not None:
            assert(isinstance(collector, list))
            
        if default_provenance is None:
            default_provenance = os.path.abspath(input_file)
            
        def rec_builder(bibcode=None, orcidid=None, provenance=None, status=None, date=None):
            assert(bibcode and orcidid)
            return ClaimsLog(bibcode=bibcode, 
                          orcidid=orcidid,
                          provenance=provenance or default_provenance, 
                          status=status or default_status,
                          created=date and get_date(date) or get_date())
            
        i = 0
        with open(input_file, 'r') as fi:
            with self.session_scope() as session:
                for line in fi:
                    i += 1
                    l = line.strip()
                    if len(l) == 0 or l[0] == '#':
                        continue
                    parts = l.split('\t')
                    try:
                        rec = rec_builder(*parts)
                        session.add(rec)
                        if collector is not None:
                            collector.append(rec.toJSON())
                    except Exception, e:
                        self.logger.error('Error importing line %s (%s) - %s' % (i, l, e))
                    if i % 1000 == 0:
                        session.commit()
                session.commit()


    def _get_ads_orcid_profile(self, orcidid, api_token, api_url):
        r = requests.get(api_url,
                 params={'reload': True},
                 headers={'Accept': 'application/json', 'Authorization': 'Bearer:%s' % api_token})
        if r.status_code == 200:
            return r.json()
        else:
            self.logger.warning('Missing profile for: {0}'.format(orcidid))
            self.logger.warning(r.text)
            return {}

    def get_claims(self, orcidid, api_token, api_url, force=False, 
                      orcid_identifiers_order=None):
        """
        Fetch a fresh profile from the orcid-service and compare
        it against the state of the storage (diff). Return the docs
        that need updating, and removal
        
    
        :param orcidid
            - string, orcid identifier
        :param: api_token
            - string, OAuth token to access ADS API
        :param: api_url:
            - string, URL for getting the orcid profiles; in the
              config it is 'API_ORCID_EXPORT_PROFILE'
        :param: force
            - bool, when True it forces claims to be counted
                as new (even if we have already indexed them)
        :param: orcid_identifiers_order
            - dict, helps to sort claims by their identifies.
                (e.g. to say that bibcodes have higher priority than
                dois)
        :return: 
            - updated: dict of bibcodes that were updated
                - keys are lowercased bibcodes
                - values are (bibcode, timestamp)
            - removed: set of bibcodes that were removed
                - keys are lowercased bibcodes
                - values are (bibcode, timestamp)
        """
        
        
        
    
        # make sure the author is there (even if without documents) 
        author = self.retrieve_orcid(orcidid) # @UnusedVariable
        data = self._get_ads_orcid_profile(orcidid, api_token, api_url)
        
        if data is None:
            return {}, {}, {} #TODO: remove all existing claims?
        
        profile = data.get('profile', {})
        if not profile:
            return {}, {}, {} #TODO: remove all existing claims?
        
    
        with self.session_scope() as session:
              
            # orcid is THE ugliest datastructure of today!
            try:
                works = profile['orcid-profile']['orcid-activities']['orcid-works']['orcid-work']
            except:
                self.logger.warning('Nothing to do for: '
                    '{0} ({1})'.format(orcidid,
                                       traceback.format_exc()))
                return {}, {}, {}
    
            # check we haven't seen this very profile already
            try:
                updt = str(profile['orcid-profile']['orcid-history']['last-modified-date']['value'])
                updt = float('%s.%s' % (updt[0:10], updt[10:]))
                updt = datetime.datetime.fromtimestamp(updt, tzutc())
                updt = get_date(updt.isoformat())
            except KeyError:
                updt = get_date()
                                    
            # find the most recent #full-import record
            last_update = session.query(ClaimsLog).filter(
                and_(ClaimsLog.status == '#full-import', ClaimsLog.orcidid == orcidid)
                ).order_by(ClaimsLog.id.desc()).first()
                
            if last_update is None:
                q = session.query(ClaimsLog).filter_by(orcidid=orcidid).order_by(ClaimsLog.id.asc())
            else:
                if get_date(last_update.created) == updt:
                    if force:
                        self.logger.info("Profile {0} unchanged, but forced update in effect.".format(orcidid))
                    else:
                        self.logger.info("Skipping {0} (profile unchanged)".format(orcidid))
                        return {}, {}, {}
                q = session.query(ClaimsLog).filter(
                    and_(ClaimsLog.orcidid == orcidid, ClaimsLog.id > last_update.id)) \
                    .order_by(ClaimsLog.id.asc())
                        
            
            # now get info about each record #TODO: enhance the matching (and refactor)
            # we'll try to match identifiers against our own API; if a document is found
            # it will be added to the `orcid_present` with corresponding timestamp (cdate)
            orcid_present = {}
            for w in works:
                bibc = None
                try:
                    ids =  w['work-external-identifiers']['work-external-identifier']
                    seek_ids = []
                    
                    # painstakingly check ids (start from a bibcode) if we can find it
                    # we'll send it through (but start from bibcodes, then dois, arxiv...)
                    fmap = orcid_identifiers_order
                    for x in ids:
                        xtype = x.get('work-external-identifier-type', None)
                        if xtype:
                            seek_ids.append((fmap.get(xtype.lower().strip(), fmap.get('*', -1)), 
                                             x['work-external-identifier-id']['value']))
                    
                    if len(seek_ids) == 0:
                        continue
                    
                    seek_ids = sorted(seek_ids, key=lambda x: x[0], reverse=True)
                    for _priority, fvalue in seek_ids:
                        try:
                            time.sleep(1.0/random.randint(1, 10)) # be nice to the api
                            metadata = self.retrieve_metadata(fvalue, search_identifiers=True)
                            if metadata and metadata.get('bibcode', None):
                                bibc = metadata.get('bibcode')
                                self.logger.info('Match found {0} -> {1}'.format(fvalue, bibc))
                                break
                        except Exception, e:
                            self.logger.error('Exception while searching for matching bibcode for: {}'.format(fvalue))
                            self.logger.warning(e.message)
                            
                    
                    if bibc:
                        # would you believe that orcid doesn't return floats?
                        ts = str(w['last-modified-date']['value'])
                        ts = float('%s.%s' % (ts[0:10], ts[10:]))
                        ts = datetime.datetime.fromtimestamp(ts, tzutc())
                        try:
                            provenance = w['source']['source-name']['value']
                        except KeyError:
                            provenance = 'orcid-profile'
                        orcid_present[bibc.lower().strip()] = (bibc.strip(), get_date(ts.isoformat()), provenance)
                    else:
                        self.logger.warning('Found no bibcode for: {orcidid}. {ids}'.format(ids=json.dumps(ids), orcidid=orcidid))
                        
                except KeyError, e:
                    self.logger.warning('Error processing a record: '
                        '{0} ({1})'.format(w,
                                           traceback.format_exc()))
                    continue
                except TypeError, e:
                    self.logger.warning('Error processing a record: '
                        '{0} ({1})'.format(w,
                                           traceback.format_exc()))
                    continue
    
            
            # find all records we have processed at some point
            updated = {}
            removed = {}
            
            for cl in q.all():
                if not cl.bibcode:
                    continue
                bibc = cl.bibcode.lower()
                if cl.status == 'removed':
                    removed[bibc] = (cl.bibcode, get_date(cl.created))
                    if bibc in updated:
                        del updated[bibc]
                else: #elif cl.status in ('claimed', 'updated', 'forced', 'unchanged'):
                    updated[bibc] = (cl.bibcode, get_date(cl.created))
                    if bibc in removed:
                        del removed[bibc]
            
            return orcid_present, updated, removed
        
    
        
    @cachetools.cached(cache)  
    def retrieve_orcid(self, orcid):
        """
        Finds (or creates and returns) model of ORCID
        from the dbase. It will automatically update our
        knowledge about the author every time it gets
        called.
        
        :param orcid - String (orcid id)
        :return - OrcidModel datastructure
        """
        with self.session_scope() as session:
            u = session.query(AuthorInfo).filter_by(orcidid=orcid).first()
            if u is not None:
                return self.update_author(u)
            u = self.create_orcid(orcid)
            session.add(u)
            session.commit()
            
            return session.query(AuthorInfo).filter_by(orcidid=orcid).first().toJSON()
    
    @cachetools.cached(orcid_cache)
    def get_public_orcid_profile(self, orcidid):
        r = requests.get(self._config.get('API_ORCID_PROFILE_ENDPOINT') % orcidid,
                     headers={'Accept': 'application/json'})
        if r.status_code != 200:
            return None
        else:
            return r.json()
    
    @cachetools.cached(ads_cache)
    def get_ads_orcid_profile(self, orcidid):
        r = requests.get(self._config.get('API_ORCID_EXPORT_PROFILE') % orcidid,
                     headers={'Accept': 'application/json', 'Authorization': 'Bearer:%s' % self._config.get('API_TOKEN')})
        if r.status_code != 200:
            return None
        else:
            return r.json()
    
    
    def update_author(self, author):
        """Updates existing AuthorInfo records. 
        
        It will check for new information. If there is a difference,
        updates the record and also records the old values.
        
        :param: author - AuthorInfo instance
        
        :return: AuthorInfo object
        
        :sideeffect: Will insert new records (ChangeLog) and also update
         the author instance
        """
        try:
            new_facts = self.harvest_author_info(author.orcidid)
        except:
            return author.toJSON()
        
        info = author.toJSON()
        with self.session_scope() as session:
            old_facts = info['facts']
            attrs = set(new_facts.keys())
            attrs = attrs.union(old_facts.keys())
            is_dirty = False
            
            for attname in attrs:
                if old_facts.get(attname, None) != new_facts.get(attname, None):
                    session.add(ChangeLog(key=u'{0}:update:{1}'.format(author.orcidid, attname), 
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


    def create_orcid(self, orcid, name=None, facts=None):
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
        name = names.cleanup_name(name)
        
        # retrieve profile from our own orcid microservice
        if not name or not facts:
            profile = self.harvest_author_info(orcid, name, facts)
            name = name or profile.get('name', None)
            if not name:
                raise IgnorableException('Cant find an author name for orcid-id: {}'.format(orcid))
            facts = profile
    
        return AuthorInfo(orcidid=orcid, name=name, facts=json.dumps(facts), account_id=facts.get('authorized', None) and 1 or None)
    
    
    def harvest_author_info(self, orcidid, name=None, facts=None):
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
        j = self.get_public_orcid_profile(orcidid)
        if j is None:
            self.logger.error('We cant verify public profile of: http://orcid.org/%s' % orcidid)
        else:
            # we don't trust (the ugly) ORCID profiles too much
            # j['orcid-profile']['orcid-bio']['personal-details']['family-name']
            if 'orcid-profile' in j and 'orcid-bio' in j['orcid-profile'] \
                and 'personal-details' in j['orcid-profile']['orcid-bio'] and \
                'family-name' in j['orcid-profile']['orcid-bio']['personal-details'] and \
                'given-names' in j['orcid-profile']['orcid-bio']['personal-details']:
                
                fname = (j['orcid-profile']['orcid-bio']['personal-details'].get('family-name', {}) or {}).get('value', None)
                gname = (j['orcid-profile']['orcid-bio']['personal-details'].get('given-names', {}) or {}).get('value', None)
                
                if fname and gname:
                    author_data['orcid_name'] = ['%s, %s' % (fname, gname)]
                    author_data['name'] = author_data['orcid_name'][0]
                
                    
        # search for the orcidid in our database (but only the publisher populated fiels)
        # we can't trust other fiels to bootstrap our database
        r = requests.get(
                    '%(endpoint)s?q=%(query)s&fl=author,author_norm,orcid_pub&rows=100&sort=pubdate+desc' % \
                    {
                     'endpoint': self._config.get('API_SOLR_QUERY_ENDPOINT'),
                     'query' : 'orcid_pub:%s' % names.cleanup_orcidid(orcidid),
                    },
                    headers={'Authorization': 'Bearer %s' % self._config.get('API_TOKEN')})
        
        if r.status_code != 200:
            self.logger.error('Failed getting data from our own API! (err: %s)' % r.status_code)
            raise Exception(r.text)
        
        
        # go through the documents and collect all the names that correspond to the ORCID
        master_set = {}
        for doc in r.json()['response']['docs']:
            for k,v in names.extract_names(orcidid, doc).items():
                if v:
                    master_set.setdefault(k, {})
                    n = names.cleanup_name(v)
                    if not master_set[k].has_key(n):
                        master_set[k][n] = 0
                    master_set[k][n] += 1
        
        # get ADS data about the user
        # 0000-0003-3052-0819 | {"authorizedUser": true, "currentAffiliation": "Australian Astronomical Observatory", "nameVariations": ["Green, Andrew W.", "Green, Andy", "Green, Andy W."]}
    
        r = self.get_ads_orcid_profile(orcidid)
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
                    x = names.cleanup_name(x)
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
                    for variant in names.build_short_forms(name):
                        short_names.add(variant)
        if len(short_names):
            author_data['short_name'] = sorted(list(short_names))
        
        return author_data
    
    
    @cachetools.cached(bibcode_cache) 
    def retrieve_metadata(self, bibcode, search_identifiers=False):
        """
        From the API retrieve the set of metadata we want to know about the record.
        """
        params={
                'q': search_identifiers and 'identifier:"{0}"'.format(bibcode) or 'bibcode:"{0}"'.format(bibcode),
                'fl': 'author,bibcode,identifier'
                }
        r = requests.get(self._config.get('API_SOLR_QUERY_ENDPOINT'),
             params=params,
             headers={'Accept': 'application/json', 'Authorization': 'Bearer:%s' % self._config.get('API_TOKEN')})
        if r.status_code != 200:
            raise Exception('{}\n{}\n{}'.format(r.status_code, params, r.text))
        else:
            data = r.json().get('response', {})
            if data.get('numFound') == 1:
                docs = data.get('docs', [])
                return docs[0]
            elif data.get('numFound') == 0:
                if search_identifiers:
                    bibcode_cache.setdefault(bibcode, {}) # insert to prevent failed retrievals
                    raise IgnorableException(u'No metadata found for identifier:{0}'.format(bibcode))
                else:
                    return self.retrieve_metadata(bibcode, search_identifiers=True)
            else:
                if data.get('numFound') > 10:
                    raise IgnorableException(u'Insane num of results for {0} ({1})'.format(bibcode, data.get('numFound')))
                docs = data.get('docs', [])
                for d in docs:
                    for ir in d.get('identifier', []):
                        if ir.lower().strip() == bibcode.lower().strip():
                            return d
                raise IgnorableException(u'More than one document found for {0}'.format(bibcode))
        
    
    
    def retrieve_record(self, bibcode):
        """
        Gets a record from the database (creates one if necessary)
        """
        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is None:
                r = Records(bibcode=bibcode)
                session.add(r)
            out = r.toJSON()
            
            metadata = self.retrieve_metadata(bibcode)
            authors = metadata.get('author', [])
            
            if out.get('authors') != authors:
                r.authors = json.dumps(authors)
                out['authors'] = authors
            
            session.commit()
            return out
        
        
    def record_claims(self, bibcode, claims, authors=None):
        """
        Stores results of the processing in the database.
        
        :param: bibcode
        :type: string
        :param: claims
        :type: dict
        """
        
        if not isinstance(claims, basestring):
            claims = json.dumps(claims)
        if authors and not isinstance(authors, basestring):
            authors = json.dumps(authors)
            
        with self.session_scope() as session:
            if not isinstance(claims, basestring):
                claims = json.dumps(claims)
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is None:
                t = get_date()
                r = Records(bibcode=bibcode, 
                            claims=claims, 
                            created=t,
                            updated=t,
                            authors=authors
                            )
                session.add(r)
                self.logger.debug('Inserting record %s', r.toJSON())
            else:
                r.updated = get_date()
                r.claims = claims
                if authors:
                    r.authors = authors
                session.merge(r)
                self.logger.debug('Updating record %s', r.toJSON())
            session.commit()


    def mark_processed(self, bibcode):
        """Updates the date on which the record has been processed (i.e.
        something has consumed it
        
        :param: bibcode
        :type: str
        
        :return: None
        """
        
        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is None:
                raise IgnorableException('Nonexistant record for {0}'.format(bibcode))
            r.processed = get_date()
            session.commit()
            return True


    def touch_author(self, orcidid, timestamp=None):
        """Update 'visited' timestamp for a given author (orcidid)
        :param orcidid: - string, orcid id
        :keyword timestamp: - optional, time that should be inserted into the record
        :return: True if author found and updates, False otherwise
        """
        
        with self.session_scope() as session:
            r = session.query(AuthorInfo).filter_by(AuthorInfo.orcidid == orcidid).first()
            if r is None:
                return False
            r.visited = timestamp or adsputils.get_date()
            session.commit()
            return True
        
    def get_untouched_authors(self, timestamp):
        """Return all the orcidids that have been visited
        before timestamp T.
        """
        with self.session_scope() as session:
            for r in session.query(AuthorInfo).filter_by(or_(AuthorInfo.visited < timestamp, AuthorInfo.visited == None)).yield_per(100):
                yield r.orcidid