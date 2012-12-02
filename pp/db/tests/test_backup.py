import tempfile
import shutil
import datetime
import os
import logging
import operator

import mock
import transaction
from path import path
import pytest

from pp.db import dbsetup, session, backup

import backup_test_db

def test_backup_and_restore_sqlite():
    logging.basicConfig(level=logging.DEBUG)
    backup_dir = tempfile.mkdtemp()
    try:
        dbfile = os.path.join(backup_dir, "test.db")
        dbsetup.setup(modules=[backup_test_db])
        dbsetup.init('sqlite:///' + dbfile)
        dbsetup.create()
        
        s = session()
        s.add(backup_test_db.TestTable(id="1", foo="bar"))
        transaction.commit()

        rows = s.query(backup_test_db.TestTable).filter(backup_test_db.TestTable.foo == "bar").all()
        assert rows

        backup.dump_database(s, backup_dir)

        now = datetime.datetime.now()
        dump_file = os.path.join(backup_dir, "test.db.dump.{:%Y%m%d-%H%M}.gz".format(now))

        assert os.path.isfile(dump_file)
    
        dbsetup.destroy()
        dbsetup.create()
        rows = s.query(backup_test_db.TestTable).all()
        assert not rows

        backup.load_database(s, dbsetup.Base.metadata, dump_file)

        rows = s.query(backup_test_db.TestTable).filter(backup_test_db.TestTable.foo == "bar").all()
        assert rows

    finally:
        shutil.rmtree(backup_dir)


def test_api_restore_points():
    backup_dir = path(tempfile.mkdtemp())

    files = ['foo.db.dump.20120101-1200.gz',
             'foo.db.dump.20120102-1200.gz',
             'foo.db.dump.20120103-1200.gz']
    try:
        for f in files:
            (backup_dir / f).touch()
        api = backup.DatabaseBackupAPI(mock.Mock(), mock.Mock(), backup_dir)
        assert sorted(api.restore_points, key=operator.itemgetter('timestamp')) == [
                {'id': "58e5f54867606384bae9c27723c3e621", 
                 'timestamp': "20120101-1200",
                 'path': backup_dir / 'foo.db.dump.20120101-1200.gz',
                 'metadata': {},
                 },
                {'id': "82f815406695e376a5b3ef687c8cdeb6",
                 'timestamp': "20120102-1200",
                 'path': backup_dir / 'foo.db.dump.20120102-1200.gz',
                 'metadata': {},
                 },
                {'id': "a0200c5e84e10a2b80e41c6114686858", 
                 'timestamp': "20120103-1200",
                 'path': backup_dir / 'foo.db.dump.20120103-1200.gz',
                 'metadata': {},
                 },
                ]
    finally:
        shutil.rmtree(backup_dir)


def test_api_load_err():
    backup_dir = path(tempfile.mkdtemp())
    try:
        api = backup.DatabaseBackupAPI(mock.Mock(), mock.Mock(), backup_dir)
        with pytest.raises(backup.BackupError):
            api.load("unknown")
    finally:
        shutil.rmtree(backup_dir)


def test_api_load():
    backup_dir = path(tempfile.mkdtemp())
    files = ['foo.db.dump.20120101-1200.gz',
             'foo.db.dump.20120102-1200.gz',
             'foo.db.dump.20120103-1200.gz']
    try:
        for f in files:
            (backup_dir / f).touch()
        session = mock.Mock()
        metadata = mock.Mock()
        api = backup.DatabaseBackupAPI(session, metadata, backup_dir)
        with mock.patch('pp.db.backup.load_database') as load:
            api.load("82f815406695e376a5b3ef687c8cdeb6")
            load.assert_called_with(session.get_bind(), metadata, backup_dir / 'foo.db.dump.20120102-1200.gz')
    finally:
        shutil.rmtree(backup_dir)


