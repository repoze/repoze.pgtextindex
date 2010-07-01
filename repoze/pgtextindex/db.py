
from transaction.interfaces import IDataManager
from zope.interface import implements
import psycopg2.extensions
import transaction

try:
    from hashlib import md5
except ImportError:
    from md5 import new as md5

# disconnected_exceptions contains the exception types that might be
# raised when the connection to the database has been broken.
disconnected_exceptions = (psycopg2.OperationalError, psycopg2.InterfaceError)


class PostgresConnectionManager(object):
    implements(IDataManager)

    def __init__(self, dsn):
        self.dsn = dsn
        self._connection = None
        self._cursor = None
        self._sort_key = md5(self.dsn).hexdigest()

    @property
    def connection(self):
        c = self._connection
        if c is None:
            c = psycopg2.connect(self.dsn)
            c.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
            self._connection = c
        return c

    @property
    def cursor(self):
        u = self._cursor
        if u is None:
            u = self.connection.cursor()
            self._cursor = u
        return u

    def set_changed(self):
        if not self._joined:
            transaction.get().join(self)
            self._joined = True

    def abort(self, transaction):
        try:
            c = self._connection
            if c is not None:
                try:
                    c.rollback()
                except:
                    self._connection = None
                    raise
        finally:
            self._joined = False

    def tpc_begin(self, transaction):
        pass

    def commit(self, transaction):
        pass

    def tpc_vote(self, transaction):
        # ensure connection is open
        self.connection

    def tpc_finish(self, transaction):
        try:
            c = self._connection
            if c is not None:
                try:
                    c.commit()
                except:
                    c.rollback()
                    raise
        finally:
            self._joined = False

    def tpc_abort(self, transaction):
        pass

    def sortKey(self):
        # The DSN might contain a password, so don't expose it.
        return self._sort_key

    def savepoint(self, optimistic=False):
        return NoRollbackSavepoint(self)


class NoRollbackSavepoint:

    def __init__(self, datamanager):
        self.datamanager = datamanager

    def rollback(self):
        pass


def safe_close(obj):
    if obj is not None:
        try:
            obj.close()
        except disconnected_exceptions:
            pass
