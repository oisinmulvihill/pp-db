"""
Common database module
"""
import uuid

# This is the instance all declaratives must use or 
from setup import Base


def session():
    """
    Create a session based on the one set up by the dbsetup.init.

    :returns: An instance of the Session class.
    
    """
    from setup import Session
    assert Session, "Please setup the database before attempting to use the session"
    return Session()

def metadata():
    """
    Return SQLAlchemy metadata, used for introspecting table definitions
    """
    from setup import metadata
    assert metadata, "Please setup the database before attempting to use the metadata"
    return metadata

def engine():
    """
    Return SQLAlchemy engine, used for introspecting table definitions
    """
    from setup import engine
    assert engine, "Please setup the database before attempting to use the engine"
    return engine

def tables():
    """
    Return all our table definitions.
    """
    from setup import tables
    assert tables, "Please setup the database before attempting to use the tables"
    return tables

def guid():
    """ 
    Returns a database GUID.
    :rtype:   String
    :return:  A string usable by database tables requiring a GUID for their ID. 
    """
    return str(uuid.uuid1()).upper()

class BaseMapper(object):
    """
    Base class for table mappers
    """
