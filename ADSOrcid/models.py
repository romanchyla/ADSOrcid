# -*- coding: utf-8 -*-

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import synonym
from sqlalchemy import Column, Integer, String, Text, TIMESTAMP

Base = declarative_base()



class AuthorInfo(Base):
    __tablename__ = 'authors'
    id = Column(Integer, primary_key=True)
    orcid_id = Column(String(19), unique=True)
    name = Column(String(255))
    info = Column(Text)
    status = Column(String(255))
    account_id = Column(Integer)
    
    
class ClaimsLog(Base):
    __tablename__ = 'claims'
    orcid_id = Column(String(19))
    bibcode = Column(String(19))
    status = Column(String(255))
    provenance = Column(String(255))
    date = Column(TIMESTAMP)

    
class Records(Base):
    __tablename__ = 'records'
    orcid_id = Column(String(19))
    bibcode = Column(String(19))
    date_updated = Column(TIMESTAMP)
    date_processed = Column(TIMESTAMP)
    status = Column(String(255))
    
