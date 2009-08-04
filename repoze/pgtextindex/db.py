
from transaction.interfaces import IDataManager
from ZODB.POSException import ConnectionStateError
from zope.interface import implements
import psycopg2
import psycopg2.extensions
import transaction

# disconnected_exceptions contains the exception types that might be
# raised when the connection to the database has been broken.
disconnected_exceptions = (psycopg2.OperationalError, psycopg2.InterfaceError)


class PGDatabaseConnector(object):
    """A PostgreSQL database connector that can fit in a ZODB database map.

    Provides the important methods and attributes of ZODB.DB.DB.
    """

    def __init__(self, dsn,
            database_name='pgtextindex-db',
            lock_table='write_lock',
            databases=None):
        self.dsn = dsn
        self.database_name = database_name

        if databases is None:
            databases = {}
        self.databases = databases
        self.database_name = database_name
        if database_name in databases:
            raise ValueError("database_name %r already in databases" %
                             database_name)
        databases[database_name] = self

    def open(self, version=None, transaction_manager=None):
        if version:
            raise ValueError("Versions are not supported by this database.")
        m = ConnectionManager(self, self.database_name)
        m.open(transaction_manager)
        return m

    def open_pg(self):
        # should we pool connections here?
        conn = psycopg2.connect(self.dsn)
        conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        cursor = conn.cursor()
        return conn, cursor

    def close_pg(self, conn, cursor):
        # should we pool connections here?
        safe_close(cursor)
        safe_close(conn)

    def _connectionMap(self, func):
        pass


class PGConnectionManager(object):
    """A data manager for a PostgreSQL database connection.

    Provides the important methods of ZODB.Connection.Connection.
    """
    implements(IDataManager)

    def __init__(self, db, database_name):
        self._db = db
        self.connections = {database_name: self}
        self.transaction_manager = None
        self._conn = None
        self._cursor = None
        self._joined = False

    @property
    def cursor(self):
        if self._conn is None:
            conn, cursor = self._db.open_pg()
            self._conn = conn
            self._cursor = cursor
        return self._cursor

    @property
    def connection(self):
        self.cursor
        return self._conn

    def set_changed(self):
        if not self._joined:
            self.transaction_manager.get().join(self)
            self._joined = True

    def open(self, transaction_manager=None, delegate=None):
        if transaction_manager is None:
            transaction_manager = transaction.manager
        self.transaction_manager = transaction_manager

    def close(self, primary=None):
        if self._joined:
            raise ConnectionStateError("Cannot close a connection joined to "
                                       "a transaction")
        if self._conn is not None:
            self._db.close_pg(self._conn, self._cursor)
            self._cursor = None
            self._conn = None

    def abort(self, transaction):
        pass

    def tpc_begin(self, transaction):
        pass

    def commit(self, transaction):
        pass

    def tpc_vote(self, transaction):
        pass

    def tpc_finish(self, transaction):
        if self._conn is not None:
            self._conn.commit()
        self._joined = False

    def tpc_abort(self, transaction):
        if self._conn is not None:
            self._conn.rollback()
        self._joined = False

    def sortKey(self):
        return self._db.database_name


def safe_close(obj):
    if obj is not None:
        try:
            obj.close()
        except disconnected_exceptions:
            pass


def get_connection_manager(zodb_conn, dsn, database_name):
    if database_name not in zodb_conn.db().databases:
        # install into the database map
        db = DatabaseConnector(dsn, database_name)
        zodb_conn.db().databases[database_name] = db
    return zodb_conn.get_connection(database_name)
