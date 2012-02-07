# -*- coding: utf-8 -*-
"""
:mod:`utils` --- Utility module
==================================================================================

.. module:: utils
   :synopsis:
.. moduleauthor:: Edward Easton<edward.easton@foldingsoftware.com>
.. sectionauthor::  Edward Easton<edward.easton@foldingsoftware.com>

.. versionadded::

The :mod:`commondb.utils` module contains some commonly used methods and functions.
"""
import logging

from sqlalchemy.orm import eagerload
from sqlalchemy.sql import select, func, and_

from pp.common.db import session, Base
from pp.common.db import tables as AllTables

def get_log():
    return logging.getLogger('nelsonapi.utils')

def generic_constructor(db_item, properties=[], **kwargs) :
    """
    Convert from a db result instance into a generic container instance.
    :param db_item: This is a sqlalchemy instance from the db.
    :param properties: List of property names.
    """
    res  = Container()
    res.properties.update(dict(list((p,None) for p in properties)))
    for p in properties:
        setattr(res, p, getattr(db_item, p))
    return res


def paginate_find(table_or_query, constructor=generic_constructor, filter={}, sort_by=None, sort_order='asc',
                  count=False, offset=None, limit=None, aggregate=False, query=None, sess=None, relations=[], **kw):
    """
    Generic paginating database find method.
    Called to find one, all or selection of database data entries.

    :param table_or_query:   Declaritive base table instance to operate on, or a dict query descriptor
    :param constructor:   Return object constructor method.
    :param filter:  Filter dict, eg {'colname':'value'}
    :param count:   Return total number of results in a tuple: (count, results)
    :param offset:  Return results offset by this number of records
    :param limit:   Limit results to this many rows
    :param sort_by: Sort by this column. Use 'table.column' for related entries
    :param query:   Optional running database query. If this is specified then no session
                    handling or query generation will be done.
    :param relations:   List of relations to expand
    :para aggregate: True/False results are passed into their constructor as a list of items

    :param sess:    Use in unit testing to override the internal session creation.


    :returns: If count was set: a tuple of (total, results), otherwise just the results.
              The results are a list of objects representing entries in the table or an
              empty list indicating nothing was found.
    """
    #print """
    #paginate_find:
    #table_or_query: %r
    #constructor: %r
    #filter: %r,
    #sort_by: %r
    #sort_order: %r
    #count: %r
    #offset: %r
    #limit: %r
    #""" % (table_or_query, constructor, filter, sort_by, sort_order, count, offset, limit )
    if not query:
        if not sess:
            s = session()
        else:
            s = sess

    try:
        # All our table refs
        all_tables  = AllTables()
        # Aliased columns
        aliases = {}
        # Are we using the dict query language?
        dict_query = False

        if query:
            # Use existing Query object. Only pagination options will be honoured.
            q = query
        else :
            if type(table_or_query) == dict :
                # Using a query defintion dictionary
                dict_query = True

                # Example of what this construct is doing :
                #from nelsonapi.db.flight import FlightDB
                #from nelsonapi.db.registration import RegistrationDB
                #q = s.query(ect(columns=[FlightDB.id, FlightDB.flight_no,FlightDB.date_time,RegistrationDB.registration_no]).alias('q1'))

                columns = []
                properties = []
                tables = []
                for table_name, column_names  in table_or_query.items():
                    # Skip calculated tables
                    if table_name in ['counts','isEqual'] : continue
                    tables.append(all_tables[table_name])
                    columns.extend(list( getattr(all_tables[table_name], col) for col in column_names))
                    properties.extend(column_names)


                # Calculations and subqueries
                subqueries = []

                # Count(column alias, table1.col, table2.col) === count(select table1.col from table1 where table1.col == table2.col)
                for alias, c1, c2 in table_or_query.get('counts',[]) :
                    c1_table_name, c1_column_name = c1.split('.')
                    c2_table_name, c2_column_name = c2.split('.')
                    t1 = all_tables[c1_table_name]
                    t2 = all_tables[c2_table_name]
                    col1 = getattr(t1, c1_column_name)
                    col2 = getattr(t2, c2_column_name)

                    stmt = s.query(col1, func.count('*').label(alias)).group_by(col1).subquery()
                    stmt_join = getattr(stmt.c, c1_column_name)
                    stmt_alias = getattr(stmt.c, alias)
                    aliases[alias] = stmt_alias
                    properties.append(alias)
                    subqueries.append((stmt, col2, stmt_join, stmt_alias))

                    #count_query = select([func.count(getattr(t1, c1_column_name))],
                    #                      getattr(t1,c1_column_name)==getattr(t2,c2_column_name)).label(alias)
                    #columns.append(count_query)

                # Add subquery columns to the select
                for stmt, left, right, alias in subqueries :
                    columns.append(alias)

                q = s.query(*columns).join(*tables[1:])

                # Join subqueries with main one
                for stmt, left, right, alias in subqueries :
                    q = q.outerjoin((stmt, left==right))

                # TODO - generalised table.column dereferencing

                # Add == clauses
                for col, val in table_or_query.get('isEqual',[]) :
                    table_name, column_name = col.split('.')
                    t1 = all_tables[table_name]
                    col1 = getattr(t1, column_name)
                    q = q.filter(col1==val)

            else:
                # Using a single table instance or mapper (or something that is potentially wrong...)
                q = s.query(table_or_query)
                properties = []
                # Attempt to introspect object for its properties in case we're using the generic constructor
                if hasattr(table_or_query, 'properties') :
                    properties = table_or_query.properties.keys()

        for r in relations:
            q = q.options(eagerload(r))

        if filter:
            q = q.filter_by(**filter)

        if count:
            total = q.count()


        if sort_by != None :
            get_log().info("sort by: %r" % sort_by)
	    if sort_by == 'random' :
	 	q = q.order_by(func.random())

            else:
		if dict_query :
		    # Query dict must specify table.column
		    if '.' in sort_by:
			table_name, column_name  = sort_by.split('.')
			col = getattr(all_tables[table_name],column_name)
		    else:
			# Check the aliases as well
			col = aliases.get(sort_by,sort_by)
		else :
		    # For ORM searches sort_by must match the columns in that table
		    col = getattr(table_or_query, sort_by)

		q = q.order_by(getattr(col, sort_order)())

        if offset != None:
            q = q.offset(offset)

        if limit != None:
            q = q.limit(limit)

        print q
        returned = q.all()

        if aggregate:
            if returned:
                returned = [constructor(returned,relations=relations,properties=properties)]
        else:
            returned = [constructor(i,relations=relations,properties=properties) for i in returned]

    finally:
        if not sess and not query:
            s.close()

    if count:
        return (total, returned)

    return returned


# -------------- Generic CRUD Methods ---------------- #

class DBGetError(Exception):
    """
    Raised when get can not recover a requested object.
    """

class DBAddError(Exception):
    """
    Raised when add could not add a new object.
    """


class DBUpdateError(Exception):
    """
    Raised when an update could work on a given object.
    """


class DBRemoveError(Exception):
    """
    Raised when an remove could work on a given object.
    """


def generic_has(obj, id_attr = 'id'):
    """
    Returns a generic 'has' DB method.
    """
    def has(item):
        s = session()
        query = s.query(obj)
        key = getattr(item, id_attr, item)
        query = query.filter_by(**{id_attr : key})
        if query.count():
            return True
        return False
    return has

def generic_get(obj, id_attr='id'):
    """
    Returns a generic 'get' DB method
    """
    def get(item):
        s = session()
        query = s.query(obj)
        key = getattr(item, id_attr, item)
        query = query.filter_by(**{id_attr : key})
        if not query.count():
            raise DBGetError("The %s '%s' was not found!" % (obj,item))
        return query.first()
    return get


def generic_find(obj):
    """Returns a generic 'find' DB method.

    :param obj: This is the SQLAlchemy Mapper / Declaritive base class to use.

    :returns: A generic find function which allows filtering by
    provided key work arguments. A list of found items is returned
    or an empty list.

    """
    def find(**kwargs):
        """Filter for %s by keyword arguments.""" % obj
        s = session()
        query = s.query(obj)
        query = query.filter_by(**kwargs)
        return query.all()
    return find


def generic_update(obj, id_attr = 'id'):
    """
    Returns a generic 'update' DB method
    """
    def update(item, **kwargs):
        s = session()
        # TODO: check for instance, re-add to session?
        query = s.query(obj)
        key = getattr(item, id_attr, item)
        query = query.filter_by(**{id_attr : key})
        if not query.count():
            raise DBUpdateError("The %s '%s' was not found!" % (obj,item))
        db_item = query.first()
        [ setattr(db_item, k, v) for k,v in kwargs.items() ]
        s.commit()
    return update


def generic_add(obj):
    """
    Returns a generic 'add' DB method
    """
    def add(**kwargs):
        s = session()
        item = obj(**kwargs)
        [ setattr(item, k, v) for k,v in kwargs.items() ]
        s.add(item)
        s.commit()
        return item
    return add


def generic_remove(obj, id_attr='id'):
    """
    Returns a generic 'remove' DB method.
    """
    def remove(item):
        s = session()
        key = getattr(item, id_attr, item)
        query = s.query(obj)
        query = query.filter_by(**{id_attr : key})
        if not query.count():
            raise DBRemoveError("The %s '%s' was not found!" % (obj, item))

        db_item = query.first()
        s.delete(db_item)
        s.commit()
    return remove
