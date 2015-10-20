# -*- coding: utf-8 -*-

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, TIMESTAMP
import datetime

Base = declarative_base()

class AuthorInfo(Base):
    __tablename__ = 'authors'
    id = Column(Integer, primary_key=True)
    orcidid = Column(String(19), unique=True)
    name = Column(String(255))
    facts = Column(Text)
    status = Column(String(255))
    account_id = Column(Integer)
    created = Column(TIMESTAMP, default=datetime.datetime.utcnow)
    updated = Column(TIMESTAMP)
    
    
class ClaimsLog(Base):
    __tablename__ = 'claims'
    id = Column(Integer, primary_key=True)
    orcidid = Column(String(19))
    bibcode = Column(String(19))
    status = Column(String(255))
    provenance = Column(String(255))
    created = Column(TIMESTAMP)

    
class Records(Base):
    __tablename__ = 'records'
    id = Column(Integer, primary_key=True)
    orcidid = Column(String(19))
    bibcode = Column(String(19))
    created = Column(TIMESTAMP)
    processed = Column(TIMESTAMP)
    status = Column(String(255))
    
