# -*- coding: utf-8 -*-
"""
:mod:`utils` --- Utility module
==================================================================================

.. module:: utils
   :synopsis:
.. moduleauthor:: Edward Easton<edward.easton@foldingsoftware.com>
.. sectionauthor::  Edward Easton<edward.easton@foldingsoftware.com>

.. versionadded::

The :mod:`pp.db.utils` module contains some commonly functions.
"""

import logging

#from sqlalchemy.orm import eagerload
#from sqlalchemy.sql import select, func, and_

from pp.db import session


def get_log():
    return logging.getLogger('pp.db.utils')


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


def generic_has(obj, id_attr='id'):
    """
    Returns a generic 'has' DB method.
    """
    def has(item):
        s = session()
        query = s.query(obj)
        key = getattr(item, id_attr, item)
        query = query.filter_by(**{id_attr: key})
        if query.count():
            return True
        return False
    return has


def generic_get(obj, id_attr='id'):
    """
    Returns a generic 'get' DB method
    """
    def get(item):
        """Recover and exiting %s item from the DB.

        """ % str(obj)
        s = session()
        query = s.query(obj)
        key = getattr(item, id_attr, item)
        query = query.filter_by(**{id_attr: key})
        if not query.count():
            raise DBGetError("The %s '%s' was not found!" % (obj, item))
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


def generic_update(obj, id_attr='id'):
    """
    Returns a generic 'update' DB method
    """
    def update(item, **kwargs):
        """Update an existing %s item in the database.

        kwargs contains: no_commit

        If no_commit is present and True, no commit will be performed. It is
        assumed this is handled elsewhere.

        """ % str(obj)
        no_commit = False
        if "no_commit" in kwargs:
            no_commit = True
            kwargs.pop("no_commit")

        s = session()
        # TODO: check for instance, re-add to session?
        query = s.query(obj)
        key = getattr(item, id_attr, item)
        query = query.filter_by(**{id_attr: key})
        if not query.count():
            raise DBUpdateError("The %s '%s' was not found!" % (obj, item))
        db_item = query.first()
        [setattr(db_item, k, v) for k, v in kwargs.items()]

        if not no_commit:
            s.commit()

    return update


def generic_add(obj):
    """
    Returns a generic 'add' DB method
    """
    def add(**kwargs):
        """Add a new %s item to the database.

        kwargs contains: no_commit

        If no_commit is present and True, no commit will be performed. It is
        assumed this is handled elsewhere.

        """ % str(obj)
        no_commit = False
        if "no_commit" in kwargs:
            no_commit = True
            kwargs.pop("no_commit")

        s = session()
        item = obj(**kwargs)
        [setattr(item, k, v) for k, v in kwargs.items()]
        s.add(item)

        if not no_commit:
            s.commit()

        return item
    return add


def generic_remove(obj, id_attr='id'):
    """
    Returns a generic 'remove' DB method.
    """
    def remove(item, no_commit=False):
        """Remove an %s item from the database.

        :param no_commit: True | False

        If no_commit is present and True, no commit will be performed. It is
        assumed this is handled elsewhere.

        """ % str(obj)
        s = session()
        key = getattr(item, id_attr, item)
        query = s.query(obj)
        query = query.filter_by(**{id_attr: key})
        if not query.count():
            raise DBRemoveError("The %s '%s' was not found!" % (obj, item))

        db_item = query.first()
        s.delete(db_item)

        if not no_commit:
            s.commit()

    return remove
