"""
The main application object (it has to be loaded by any worker/script)
in order to initialize the database and get a working configuration.
"""

from contextlib import contextmanager
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from . import utils
import os
import sys

config = {}
session = None
logger = None


def init_app(local_config=None):
    """This function must be called before you start working with the application
    (or worker, script etc)
    
    :return None
    """
    

    config.update(utils.load_config())
    if local_config:
        config.update(local_config)
    
    global logger
    logger = utils.setup_logging(__file__, 'app', config['LOGGING_LEVEL'])
    
    engine = create_engine(config.get('SQLALCHEMY_URL', 'sqlite:///'))
    session_factory = sessionmaker()
    global session
    session = scoped_session(session_factory)
    
    session.configure(bind=engine)

    
@contextmanager
def session_scope():
    """Provides a transactional session - ie. the session for the 
    current thread/work of unit. The application has to be properly
    initialized before you use method. See :object: 
    `ADSOrcid.app.session`
    
    Use as:
    
        with session_scope() as session:
            o = AuthorInfo(...)
            session.add(o)
    """

    if session is None:
        raise Exception('init_app() must be called before you can use the session')
    
    # create local session (optional step)
    s = session()
    
    try:
        yield s
        s.commit()
    except:
        s.rollback()
        raise
    finally:
        s.close()        
    

