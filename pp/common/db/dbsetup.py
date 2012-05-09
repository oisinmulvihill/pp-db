# -*- coding: utf-8 -*-
"""
Common Database initialisation Tools

Edward Easton, Oisin Mulvihill

"""
import logging
import importlib

import sqlalchemy
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
__mapper_modules = []


def modules_from_config(settings, prefix='commondb.'):
    """
    Returns the list of `pp.common.db` modules from a given config dict
    """
    def modules():
        return settings.get('%smodules' % prefix, '').split('\n')

    module_list = [i.strip() for i in modules() if i.strip()]

    try:
        return map(importlib.import_module, module_list)

    except Exception:
        # Give some hint to what may have gone wrong i.e. comments or some
        # order non module / rubbish in the indented list.
        #
        get_log().exception(
            "modules_from_config error: module_list <%s>  - " % module_list
        )
        raise


def setup(modules=[], mappers=[]):
    """
    Adds modules and mappers to the commondb registry of known schema items.
    :param modules: list of modules to initialise. If any of these items have a
                    '''commondb_setup()''' method on them, it will call this method
                    to recover a dict of '''{'modules':modules, 'mappers':mappers}'''
                    items instead. Use this to shortcut importing a whole package
                    worth of modules with one call to this method.
    :param mappers: list of mappers to initialise.
    """
    global __modules, __mapper_modules

    for m in modules:
        if hasattr(m, 'commondb_setup'):
            cfg = m.commondb_setup()
            __modules.extend(cfg['modules'])
            __mapper_modules.extend(cfg['mappers'])
        else:
            __modules.append(m)
    __mapper_modules.extend(mappers)
    if __modules:
        get_log().info("Found db modules:")
        map(get_log().info, __modules)
    else:
        get_log().info("No db modules configured")
    if __mapper_modules:
        get_log().info("Found mapper modules:")
        map(get_log().info, __mapper_modules)
    else:
        get_log().info("No mappers configured")


def init(uri, pool_size=5, pool_max_overflow=10, pool_timeout=30, pool_recycle=-1,
         use_transaction=True):
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

    engine = sqlalchemy.create_engine(
        uri,
        poolclass=sqlalchemy.pool.QueuePool,
        pool_size=pool_size,
        max_overflow=pool_max_overflow,
        pool_recycle=pool_recycle,
        echo=False,
        echo_pool=False,
    )
    if use_transaction:
        from zope.sqlalchemy import ZopeTransactionExtension
        Session = scoped_session(sessionmaker(bind=engine, extension=ZopeTransactionExtension()))
    else:
        Session = scoped_session(sessionmaker(bind=engine))
    Base.metadata.bind = engine
    init_modules()


def init_with_session(bind, session):
    """ As above but with a pre-existing session, eg from a multithreaded webserver """
    global Session, engine, Base
    get_log().info("init: starting project wide setup given bind and session: %s %s" % \
        (bind, session))
    Session = session
    engine = bind
    Base.metadata.bind = bind
    init_modules()


def init_from_config(settings, prefix='sqlalchemy.', use_transaction=True):
    """ As above but use a settings dict, eg from a Pyramid config
    """
    global Session, engine, Base
    engine = sqlalchemy.engine_from_config(settings, prefix)
    if use_transaction:
        from zope.sqlalchemy import ZopeTransactionExtension
        Session = scoped_session(sessionmaker(bind=engine, extension=ZopeTransactionExtension()))
    else:
        Session = scoped_session(sessionmaker(bind=engine))
    Base.metadata.bind = engine
    init_modules()


def init_modules():
    """ Go through all our modules configured in `setup` and run their
        init methods. Fills out the global mappers, tables and bases lookups.
    """
    global tables, bases
    for mod in __modules:
        if not hasattr(mod, 'init'):
            raise ValueError("Module %r has no 'init' method, is this a database module?" % mod)
        mod_bases, mod_tables, mod_mappers = mod.init()
        # TODO: do we need to do anything with the mod_mappers?
        for b in mod_bases:
            bases[b.__tablename__] = b
        for t in mod_tables:
            tables[t.__tablename__] = t
            # TODO add entries for tables from bases as well

    # Pass the tables into all the mappers
    for mod in __mapper_modules:
        mod.init(tables)


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
