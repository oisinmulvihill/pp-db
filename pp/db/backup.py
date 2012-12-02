"""
Database backup utils
"""
import subprocess
import datetime
import gzip
import logging
import md5
import json

from path import path
import dateutil.parser

log = logging.getLogger(__name__)


class BackupError(Exception):
    pass

class DatabaseBackupAPI(object):
    """ One-stop-shop for backup and restore of databases
    """
    def __init__(self, session_or_engine, metadata, backup_dir):
        """
        :param session_or_engine:     SQLAlchemy session or engine for your database
        :param metadata:     SQLAlchemy metadata for your database
        :param backup_dir:  Filesystem dir to store backups
        """
        if hasattr(session_or_engine, 'get_bind'):
            self.engine = session_or_engine.get_bind()
        else:
            self.engine = session_or_engine
        self.metadata = metadata
        self.backup_dir = path(backup_dir)
        if not self.backup_dir.isdir():
            self.backup_dir.makedirs_p()

    def _meta_filename(self, dump_file):
            return dump_file.dirname() / (dump_file.basename() + ".meta")

    def dump(self, file_metadata=None):
        """ Dump database to backup directory

            :param file_metadata: Mark this backup with custom metadata which is returned as part of
                                  restore_points
        """
        dump_file = dump_database(self.engine, self.backup_dir)
        if file_metadata:
            meta_file = self._meta_filename(dump_file)
            meta_file.write_text(json.dumps(file_metadata))
        return dump_file

    def get_id(self, dump_file):
        """ Returns restore point ID for a given filepath
        """
        return md5.new(path(dump_file).basename()).hexdigest()

    @property
    def restore_points(self):
        """ Return a list of available restore points
        """
        res = []
        for f in self.backup_dir.files("*.gz") :
            md = {}
            if self._meta_filename(f).isfile():
                md = json.loads(self._meta_filename(f).text())
            res.append({'id': self.get_id(f), 
                        'timestamp':f.basename().split('.')[-2], 
                        'path': f,
                        'metadata': md
            })
        return res

    @property
    def _last_restore_file(self):
        return self.backup_dir / '{0}.last_restore'.format(self.engine.url.database)
    
    @property
    def last_restore(self):
        """ Timestamp of the last time we did a restore
        """
        if self._last_restore_file.isfile():
            return dateutil.parser.parse(self._last_restore_file.text())
        return None
        
    def load(self, restore_point_id):
        """ Load database from given restore point, and save a marker for when we did this
        """
        backup = [i for i in self.restore_points if i['id'] == restore_point_id]
        if not backup:
            raise BackupError("Unknown restore point")
        backup = backup[0]
        load_database(self.engine, self.metadata, backup['path'])
        self._last_restore_file.write_text(datetime.datetime.now().isoformat())


def dump_sqlite(engine, backup_dir):
    """ This is the equivalent of:
        echo '.dump' | sqlite3 dbfile | gzip -c > backup_dir/dbfile.dump.20121004-0300.gz

        :returns:   path to new dump file
    """
    backup_dir = path(backup_dir)
    dbfile = path(engine.url.database)
    dump_name = '{}.dump.{}.gz'.format(dbfile.basename(), datetime.datetime.now().strftime('%Y%m%d-%H%M'))
    dump_file = backup_dir / dump_name
    log.info("Dumping SQLite database to {}".format(dump_file))
    with gzip.open(dump_file, 'wb') as zip_fh:
        cmd = ['sqlite3', dbfile]
        sqlite = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        stdout, _ = sqlite.communicate(".dump\n")
        for line in stdout:
            zip_fh.write(line)
    return dump_file


def load_sqlite(engine, dump_file):
    """ Load a sqlite dump file into the given sqla engine
    """
    dbfile = engine.url.database
    log.warn("Loading SQLite database from {}. This will destroy all existing data".format(dump_file))
    cmd = ['sqlite3', dbfile]
    sqlite = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    with gzip.open(dump_file) as dump_file:
        for line in dump_file:
            log.debug(line)
            sqlite.stdin.write(line)
    sqlite.communicate()


ENGINE_MAP = {
        'sqlite': (dump_sqlite, load_sqlite)
}

def dump_database(session_or_engine, backup_dir):
    """ Backs up a database from the session to a backup dir
    """ 
    if hasattr(session_or_engine, 'get_bind'):
        engine = session_or_engine.get_bind()
    else:
        engine = session_or_engine

    dump, _ = ENGINE_MAP[engine.name]
    return dump(engine, backup_dir)


def load_database(session_or_engine, metadata, dump_file):
    """ Restores a database from the session and dump file
    """ 
    if hasattr(session_or_engine, 'get_bind'):
        engine = session_or_engine.get_bind()
    else:
        engine = session_or_engine
    _, load = ENGINE_MAP[engine.name]
    log.warn("Destroying all existing data in database at {}".format(engine.url.database))
    # TODO: using metadata here isn't the most fullproof method - anything that's not 
    #       referenced in this particular metadata obj won't be dropped
    metadata.drop_all(engine)
    load(engine, dump_file)
