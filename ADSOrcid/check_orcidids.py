
from app import session_scope, config
from . import app
from .models import AuthorInfo
from sqlalchemy.orm import load_only
import requests
import json
from .utils import get_date
import datetime
from pipeline import GenericWorker


"""
This script will re-check the remove orcid-service
- fetching all orcidids (ever recorded)
- and triggers reingestion/updates
"""

def run(start_date='1974-11-09T22:56:52.518001Z'):
    
    app.logger.info("Checking for orcid updates since: %s", start_date)
    start_date = get_date(start_date) # RFC 3339 format
    
    # collect orcidids from orcid-service
    orcidids = set()     
    while True:
        authors = get_orcidids(start_date)
        if len(authors) == 0:
            break
        for author in authors:
            orcidids.add(author['orcid_id'])
            last_update = author['updated']
        start_date = get_date(last_update) + datetime.timedelta(microseconds=1)
    
    # collect known authors (these were already processed)
    authors = set()
    with app.session_scope() as session:
        for a in session.query(AuthorInfo).options(load_only('orcidid', 'updated')).all():
            authors.add(a.orcidid)
            
    # find missing ones
    missing = orcidids.difference(authors)
    
    if len(missing) > 0:
        worker = GenericWorker.RabbitMQWorker()
        worker.connect(app.config.get('RABBITMQ_URL'))
        for orcidid in missing:
            worker.publish(orcidid, topic='ads.orcid.reindex')


def get_orcidids(since_date):
    r = requests.get(app.config.get('API_ORCID_UPDATES_ENDPOINT') % since_date.isoformat(),
                 params={'fields': ['updated', 'orcid_id']},
                 headers = {'Authorization': 'Bearer {0}'.format(app.config.get('API_TOKEN'))})
    if r.status_code != 200:
        app.logger.error('Failed getting {0}\n{1}'.format(
                                app.config.get('API_ORCID_UPDATES_ENDPOINT') % since_date.isoformat(),
                                r.text))
        return []
    return r.json()

