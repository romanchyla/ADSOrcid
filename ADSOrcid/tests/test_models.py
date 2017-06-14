#!/usr/bin/env python
# -*- coding: utf-8 -*-


from datetime import datetime
import unittest
import adsputils as utils
from ADSOrcid.models import ClaimsLog, Records, AuthorInfo, Base
from ADSOrcid import app

class Test(unittest.TestCase):
    
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.app = app.create_app('test',
            {
            'SQLALCHEMY_URL': 'sqlite:///',
            'SQLALCHEMY_ECHO': False
            })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
    
    
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()
        
    
    def test_models(self):
        """Check serialization into JSON"""
        
        app = self.app
        claim = ClaimsLog(bibcode='foo', orcidid='bar',
                          created='2009-09-03T20:56:35.450686Z')
        self.assertDictEqual(claim.toJSON(),
             {'status': None, 'bibcode': 'foo', 'created': '2009-09-03T20:56:35.450686+00:00', 'provenance': 'None', 'orcidid': 'bar', 'id': None})
        
        ainfo = AuthorInfo(orcidid='bar',
                          created='2009-09-03T20:56:35.450686Z')
        
        self.assertDictEqual(ainfo.toJSON(),
             {'status': None, 'updated': None, 'name': None, 'created': '2009-09-03T20:56:35.450686+00:00', 'facts': {}, 'orcidid': 'bar', 'id': None, 'account_id': None})
        
        rec = Records(bibcode='foo', created='2009-09-03T20:56:35.450686Z')

        self.assertDictEqual(rec.toJSON(),
             {'bibcode': 'foo', 'created': '2009-09-03T20:56:35.450686+00:00', 'updated': None, 'processed': None, 'claims': {}, 'id': None, 'authors': []})
        
        with self.assertRaisesRegexp(Exception, 'IntegrityError'):
            with app.session_scope() as session:
                c = ClaimsLog(bibcode='foo', orcidid='bar', status='hey')
                session.add(c)
                session.commit()
        
        for s in ['blacklisted', 'postponed']:
            with app.session_scope() as session:
                session.add(AuthorInfo(orcidid='bar' + s, status=s))
                session.commit()
        
        with self.assertRaisesRegexp(Exception, 'IntegrityError'):
            with app.session_scope() as session:
                c = AuthorInfo(orcidid='bar', status='hey')
                session.add(c)
                session.commit()
        
        for s in ['claimed', 'updated', 'removed', 'unchanged', '#full-import']:
            with app.session_scope() as session:
                session.add(ClaimsLog(bibcode='foo'+s, orcidid='bar', status=s))
                session.commit()
                
    
    def test_dates(self):
        '''We want to use only UTC dates'''
        app = self.app
        with self.assertRaisesRegexp(Exception, 'ValueError'):
            with app.session_scope() as session:
                rec = Records(bibcode='foo', created='2009-09-03T20:56:35.450686Z')
                session.add(rec)
                rec.updated = datetime.now()
                session.commit()

        with app.session_scope() as session:
            rec = Records(bibcode='foo', created='2009-09-03T20:56:35.450686Z')
            session.add(rec)
            rec.updated = utils.get_date()
            session.commit()

            
if __name__ == '__main__':
    unittest.main()            