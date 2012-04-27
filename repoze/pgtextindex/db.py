
from transaction.interfaces import IDataManager
from zope.interface import implements
import psycopg2.extensions
import transaction

try:  # pragma: no cover
    from hashlib import md5
except ImportError:  # pragma: no cover
    from md5 import new as md5

# disconnected_exceptions contains the exception types that might be
# raised when the connection to the database has been broken.
disconnected_exceptions = (psycopg2.OperationalError, psycopg2.InterfaceError)


class PostgresConnectionManager(object):
    implements(IDataManager)

    def __init__(self, dsn, transaction_manager=transaction.manager,
                 module=psycopg2):
        self.dsn = dsn
        self.transaction_manager = transaction_manager
        self.module = module
        self._connection = None
        self._cursor = None
        self._sort_key = md5(self.dsn).hexdigest()
        self._joined = False

    @property
    def connection(self):
        c = self._connection
        if c is None:
            c = self.module.connect(self.dsn)
            c.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            self._connection = c
        return c

    @property
    def cursor(self):
        u = self._cursor
        if u is None:
            u = self.connection.cursor()
            self._cursor = u

        if not self._joined:
            self.transaction_manager.get().join(self)
            self._joined = True
            # Bring the connection up to date.
            try:
                self.connection.rollback()
                u.execute('SELECT 1')
                u.fetchall()
            except disconnected_exceptions:
                # Try to reopen.
                self.close()
                u = self.connection.cursor()
                self._cursor = u

        return u

    def close(self):
        if self._cursor is not None:
            safe_close(self._cursor)
            self._cursor = None
        if self._connection is not None:
            safe_close(self._connection)
            self._connection = None

    def abort(self, transaction):
        try:
            c = self._connection
            if c is not None:
                try:
                    c.rollback()
                except:
                    self.close()
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
                    try:
                        c.rollback()
                    except (KeyboardInterrupt, SystemExit):  # pragma: no cover
                        self.close()
                        raise
                    except:
                        self.close()
                    raise
        finally:
            self._joined = False

    def tpc_abort(self, transaction):
        self.abort(transaction)

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
