
import unittest

class TestPostgresConnectionManager(unittest.TestCase):

    def setUp(self):
        import transaction
        transaction.abort()

    tearDown = setUp

    def _get_class(self):
        from repoze.pgtextindex.db import PostgresConnectionManager
        return PostgresConnectionManager

    def _make_one(self, dsn="dbname=dummy"):
        return self._get_class()(dsn, module=DummyPsycopg2Module())

    def test_class_conforms_to_IDataManager(self):
        from zope.interface.verify import verifyClass
        from transaction.interfaces import IDataManager
        verifyClass(IDataManager, self._get_class())

    def test_instance_conforms_to_IDataManager(self):
        from zope.interface.verify import verifyObject
        from transaction.interfaces import IDataManager
        verifyObject(IDataManager, self._make_one())

    def test_connection_attr(self):
        cm = self._make_one()
        self.assertEqual(cm.connection.dsn, "dbname=dummy")
        self.assertEqual(cm.connection.isolation_level, 2)

    def test_cursor_attr(self):
        cm = self._make_one()
        self.assertEqual(cm.cursor.connection.dsn, "dbname=dummy")

    def test_set_changed(self):
        cm = self._make_one()
        self.assertFalse(cm._joined)
        cm.set_changed()
        self.assertTrue(cm._joined)

    def test_close_not_open(self):
        cm = self._make_one()
        cm.close()
        self.assertEqual(cm._connection, None)

    def test_close_after_open(self):
        cm = self._make_one()
        conn = cm.connection
        cursor = cm.cursor
        cm.close()
        self.assertEqual(cm._connection, None)
        self.assertEqual(cm._cursor, None)
        self.assertTrue(conn._closed)
        self.assertTrue(cursor._closed)

    def test_abort_success(self):
        cm = self._make_one()
        conn = cm.connection
        cm.set_changed()
        self.assertEqual(conn.rollbacks, 0)
        import transaction
        transaction.abort()
        self.assertEqual(conn.rollbacks, 1)

    def test_abort_fail(self):
        cm = self._make_one()
        conn = cm.connection

        def faulty_rollback():
            raise ValueError()

        conn.rollback = faulty_rollback
        cm.set_changed()
        self.assertEqual(conn.rollbacks, 0)
        import transaction
        self.assertRaises(ValueError, transaction.abort)

    def test_commit_success(self):
        cm = self._make_one()
        conn = cm.connection
        cm.set_changed()
        self.assertEqual(conn.commits, 0)
        import transaction
        transaction.commit()
        self.assertEqual(conn.commits, 1)

    def test_commit_fail(self):
        cm = self._make_one()
        conn = cm.connection
        cm.set_changed()

        def faulty_commit():
            raise ValueError()

        conn.commit = faulty_commit
        self.assertEqual(conn.commits, 0)
        import transaction
        self.assertRaises(ValueError, transaction.commit)

    def test_sortKey(self):
        cm = self._make_one()
        self.assertTrue(isinstance(cm.sortKey(), str))

    def test_savepoint(self):
        cm = self._make_one()
        sp = cm.savepoint()
        self.assertEqual(sp.rollback(), None)
        self.assertTrue(sp.datamanager is cm)


class TestSafeClose(unittest.TestCase):

    def _call(self, obj):
        from repoze.pgtextindex.db import safe_close
        return safe_close(obj)

    def test_none(self):
        self._call(None)

    def test_success(self):
        class DummyCloseable:
            def close(self):
                self.closed = True

        obj = DummyCloseable()
        self._call(obj)
        self.assertTrue(obj.closed)

    def test_catch_operationalerror(self):
        import psycopg2

        class DummyCloseable:
            def close(self):
                raise psycopg2.OperationalError()

        obj = DummyCloseable()
        self._call(obj)

    def test_catch_interfaceerror(self):
        import psycopg2

        class DummyCloseable:
            def close(self):
                raise psycopg2.InterfaceError()

        obj = DummyCloseable()
        self._call(obj)

    def test_no_catch_other(self):
        class DummyCloseable:
            def close(self):
                raise ValueError()

        obj = DummyCloseable()
        self.assertRaises(ValueError, self._call, obj)


class DummyPsycopg2Module:
    def connect(self, dsn):
        return DummyPsycopg2Connection(dsn)


class DummyPsycopg2Connection:
    def __init__(self, dsn):
        self.dsn = dsn
        self.commits = 0
        self.rollbacks = 0

    def set_isolation_level(self, level):
        self.isolation_level = level

    def cursor(self):
        return DummyPsycopg2Cursor(self)

    def close(self):
        self._closed = True

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class DummyPsycopg2Cursor:
    def __init__(self, connection):
        self.connection = connection

    def close(self):
        self._closed = True


def test_suite():
    return unittest.TestSuite((
        unittest.makeSuite(TestPostgresConnectionManager),
    ))
