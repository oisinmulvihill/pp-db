"""
Common Database initialisation Tools

Edward Easton, Oisin Mulvihill

"""
import os
import logging

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

def get_log():
    return logging.getLogger("pp.common.db.setup")


# SQLAlchemy database set up by the init() function:
engine = None
Session = None

Base = declarative_base()

# Table lookup for our baseclasses (as they are not showing up in metadata.tables)
bases = {}
tables = {}

# This fixed problems with postgresql which prevents db reset from working.
#
# http://blog.pythonisito.com/2008/01/cascading-drop-table-with-sqlalchemy.html
#
# EDE 04/06/11: This no longer withs with SA @ v0.7.2: commenting out for now
"""
from sqlalchemy.databases import postgres
class PGCascadeSchemaDropper(postgres.PGSchemaDropper):
     def visit_table(self, table):
        for column in table.columns:
            if column.default is not None:
                self.traverse_single(column.default)
        self.append("\nDROP TABLE " +
                    self.preparer.format_table(table) +
                    " CASCADE")
        self.execute()

    postgres.dialect.schemadropper = PGCascadeSchemaDropper
"""




# Global list of database modules we know about
__modules = []
__mappers = []

def setup( modules = [], mappers = [] ):
    """ 
      * init(tables): Create a set of mappers using the given lookup of tables

     This can safely be called multiple times.
    """
    global __modules, __mappers
    __modules.extend(modules)
    __mappers.extend(mappers)


def init(uri, pool_size=5, pool_max_overflow=10, pool_timeout=30, pool_recycle=-1):
    """Called to do the initial metadata set up for all the database modules
       passed in via the :meth:`setup` method.

    :type uri:                  String
    :param uri:                 This is the database access URI.
    :type pool_size:            Integer
    :param pool_size:           Connection pool size
    :type pool_max_overflow:    Integer
    :param pool_max_overflow:   Connection pool max overflow size
    :type pool_timeout:         Integer
    :param pool_timeout:        Connection pool timeout in seconds
    :type pool_recycle:         Integer
    :param pool_recycle:        Connection pool recycle time in seconds

    """
    get_log().info("init: starting project wide setup...")

    global engine, Session, Base

    bind = sqlalchemy.create_engine(
        uri,
        poolclass=sqlalchemy.pool.QueuePool, 
        pool_size=pool_size, 
        max_overflow=pool_max_overflow, 
        pool_recycle=pool_recycle,
        echo=False,
        echo_pool=False,
    )

    session = scoped_session(sessionmaker(bind=bind))
    session.configure(bind=bind)
    Session = session
    engine = bind
    Base.metadata.bind = bind

    # Pass the engine to all the tables so that they can bind it to their meta data instances.
    for mod in __modules:
        mod.init(Base)
        for t in mod.TABLES:
            tables[t.__tablename__] = t

    # Pass the tables into all the mappers
    for mod in __mappers :
        mod.init(tables)
        

def init_with_session(bind, session):
    """ As above but with a pre-existing session, eg from a multithreaded webserver """
    global Session, tables, engine, Base, bases, __mappers
    get_log().info("init: starting project wide setup given bind and session: %s %s" % \
        (bind, session))
    Session = session
    engine = bind
    Base.metadata.bind = bind

    for mod in __modules:
        # Pass the base class into all the mappers
        mod_bases, mod_tables, mod_mappers = mod.init(Base)
        for b in mod_bases:
            bases[b.__tablename__] = b
        for t in mod_tables:
            tables[t.__tablename__] = t
            # TODO add entries for tables from bases as well
        __mappers.extend(mod_mappers)

def create():
    """Called to create all the tables required for the modules 
       passed in via the :meth:`setup` method.
    """
    get_log().info("create: starting project wide create...")
    Base.metadata.create_all()
    for mod in __modules:
        # Call module create hook if it's there
        if hasattr(mod, 'create'):
            mod.create()
    get_log().info("create: done.")


def destroy():
    """Called to destroy all the tables required for the modules 
       passed in via the :meth:`setup` method.
    """
    get_log().warn("destroy: starting project wide destroy...")
    Base.metadata.drop_all()
    for mod in __modules:
        # Call module destroy hook if it's there
        if hasattr(mod, 'destroy'):
            mod.destroy()
    get_log().info("destroy: done.")


def dump(output_dir=None):
    """
    TODO
    """
    pass


def load(input_dir):
    """
    TODO
    """
    pass
