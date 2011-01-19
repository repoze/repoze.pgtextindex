
import unittest

class TestPGTextIndex(unittest.TestCase):

    def _get_class(self):
        from repoze.pgtextindex.index import PGTextIndex
        return PGTextIndex

    def _make_one(self, discriminator=None, dsn="dbname=dummy"):
        if discriminator is None:
            def discriminator(obj, default):
                return obj
        return self._get_class()(discriminator, dsn,
            connection_manager_factory=DummyConnectionManager)

    def test_class_conforms_to_ICatalogIndex(self):
        from zope.interface.verify import verifyClass
        from repoze.catalog.interfaces import ICatalogIndex
        verifyClass(ICatalogIndex, self._get_class())

    def test_instance_conforms_to_ICatalogIndex(self):
        from zope.interface.verify import verifyObject
        from repoze.catalog.interfaces import ICatalogIndex
        verifyObject(ICatalogIndex, self._make_one())

    def test_class_conforms_to_IIndexSort(self):
        from zope.interface.verify import verifyClass
        from zope.index.interfaces import IIndexSort
        verifyClass(IIndexSort, self._get_class())

    def test_instance_conforms_to_IIndexSort(self):
        from zope.interface.verify import verifyObject
        from zope.index.interfaces import IIndexSort
        verifyObject(IIndexSort, self._make_one())

    def test_ctor_invalid_discriminator(self):
        self.assertRaises(ValueError, self._make_one,
            object())

    def test_connection_manager_from_volatile_attr(self):
        index = self._make_one()
        self.assertEqual(index.connection_manager, index._v_temp_cm)

    def test_connection_manager_from_jar(self):
        class DummyZODBConnection:
            pass

        index = self._make_one()
        index._p_jar = DummyZODBConnection()
        index._p_oid = '1' * 8
        self.assertTrue(index._p_jar)
        self.assertTrue(index.connection_manager is not None)
        self.assertFalse(index._v_temp_cm)

    def test_jar_can_close_connection_manager(self):
        class DummyZODBConnection:
            released = False
            def _release_resources(self):
                self.released = True

        index = self._make_one()
        index._p_jar = jar = DummyZODBConnection()
        index._p_oid = '1' * 8
        cm = index.connection_manager
        self.assertFalse(cm.closed)
        self.assertFalse(jar.released)
        index._p_jar._release_resources()
        self.assertTrue(cm.closed)
        self.assertTrue(jar.released)

    def test_cursor_property(self):
        index = self._make_one()
        cursor = index.cursor
        self.assertTrue(hasattr(cursor, 'execute'))

    def _format_executed(self, executed):
        self.assertEqual(len(executed), 1)
        stmt, params = executed[0]
        lines = stmt.splitlines()
        lines = [line.strip() for line in lines if line.strip()]
        return lines, params

    def test_index_doc_none(self):
        index = self._make_one()
        index.cursor.executed = executed = []
        index.index_doc(6, None)
        self.assertEqual(len(executed), 0)

    def test_index_doc_empty_string(self):
        index = self._make_one()
        index.cursor.executed = executed = []
        index.index_doc(6, '')
        self.assertEqual(len(executed), 0)

    def test_index_doc_empty_strings_weighted(self):
        index = self._make_one()
        index.cursor.executed = executed = []
        index.index_doc(6, [['', ''], ''])
        self.assertEqual(len(executed), 0)

    def test_index_doc_unweighted(self):
        index = self._make_one()
        index.cursor.executed = executed = []
        index.index_doc(5, 'Waldo')
        lines, params = self._format_executed(executed)
        self.assertEqual(lines, [
            'LOCK pgtextindex IN EXCLUSIVE MODE;',
            'DELETE FROM pgtextindex WHERE docid = %s;',
            'INSERT INTO pgtextindex (docid, text_vector)',
            'VALUES (%s, to_tsvector(%s, %s))',
        ])
        self.assertEqual(params, (5, 5, 'english', 'Waldo'))

    def test_index_doc_using_attr_discriminator(self):
        class DummyObject:
            name = 'Osvaldo'

        index = self._make_one('name')
        index.cursor.executed = executed = []
        index.index_doc(6, DummyObject())
        lines, params = self._format_executed(executed)
        self.assertEqual(lines, [
            'LOCK pgtextindex IN EXCLUSIVE MODE;',
            'DELETE FROM pgtextindex WHERE docid = %s;',
            'INSERT INTO pgtextindex (docid, text_vector)',
            'VALUES (%s, to_tsvector(%s, %s))',
        ])
        self.assertEqual(params, (6, 6, 'english', 'Osvaldo'))

    def test_index_doc_missing_value(self):
        def discriminator(obj, default):
            return default

        index = self._make_one(discriminator)
        index.cursor.executed = executed = []
        index.index_doc(6, 'dummy')
        lines, params = self._format_executed(executed)
        self.assertEqual(lines, [
            'LOCK pgtextindex IN EXCLUSIVE MODE;',
            'DELETE FROM pgtextindex',
            'WHERE docid = %s',
        ])
        self.assertEqual(params, (6,))

    def test_index_doc_persistent_object(self):
        from persistent import Persistent

        class DummyObject(Persistent):
            pass

        index = self._make_one()
        self.assertRaises(ValueError,
            index.index_doc, 6, DummyObject())

    def test_index_doc_use_one_weight(self):
        index = self._make_one()
        index.cursor.executed = executed = []
        index.index_doc(5, ['Waldo', 'character'])
        lines, params = self._format_executed(executed)
        self.assertEqual(lines, [
            'LOCK pgtextindex IN EXCLUSIVE MODE;',
            'DELETE FROM pgtextindex WHERE docid = %s;',
            'INSERT INTO pgtextindex (docid, text_vector)',
            'VALUES (%s, setweight(to_tsvector(%s, %s), %s) || '
                'to_tsvector(%s, %s))',
        ])
        self.assertEqual(params, (5, 5,
            'english', 'Waldo', 'A',
            'english', 'character',
        ))

    def test_index_doc_use_more_than_all_possible_weights(self):
        index = self._make_one()
        index.cursor.executed = executed = []
        index.index_doc(5, ['Waldo', 'character', 'boy', 'person', 'entity'])
        lines, params = self._format_executed(executed)
        self.assertEqual(lines, [
            'LOCK pgtextindex IN EXCLUSIVE MODE;',
            'DELETE FROM pgtextindex WHERE docid = %s;',
            'INSERT INTO pgtextindex (docid, text_vector)',
            'VALUES (%s, setweight(to_tsvector(%s, %s), %s) || '
                'setweight(to_tsvector(%s, %s), %s) || '
                'setweight(to_tsvector(%s, %s), %s) || '
                'setweight(to_tsvector(%s, %s), %s) || '
                'to_tsvector(%s, %s))',
        ])
        self.assertEqual(params, (5, 5,
            'english', 'Waldo', 'A',
            'english', 'character', 'B',
            'english', 'boy', 'C',
            'english', 'person', 'C',
            'english', 'entity',
        ))

    def test_index_doc_skip_weights(self):
        index = self._make_one()
        index.cursor.executed = executed = []
        index.index_doc(5, ['Waldo', '', 'boy', ''])
        lines, params = self._format_executed(executed)
        self.assertEqual(lines, [
            'LOCK pgtextindex IN EXCLUSIVE MODE;',
            'DELETE FROM pgtextindex WHERE docid = %s;',
            'INSERT INTO pgtextindex (docid, text_vector)',
            'VALUES (%s, setweight(to_tsvector(%s, %s), %s) || '
                'setweight(to_tsvector(%s, %s), %s))',
        ])
        self.assertEqual(params, (5, 5,
            'english', 'Waldo', 'A',
            'english', 'boy', 'C',
        ))

    def test_index_doc_multiple_texts_with_the_same_weight(self):
        index = self._make_one()
        index.cursor.executed = executed = []
        index.index_doc(5, [['Waldo', 'Wally'], ''])
        lines, params = self._format_executed(executed)
        self.assertEqual(lines, [
            'LOCK pgtextindex IN EXCLUSIVE MODE;',
            'DELETE FROM pgtextindex WHERE docid = %s;',
            'INSERT INTO pgtextindex (docid, text_vector)',
            'VALUES (%s, setweight(to_tsvector(%s, %s), %s) || '
                'setweight(to_tsvector(%s, %s), %s))',
        ])
        self.assertEqual(params, (5, 5,
            'english', 'Waldo', 'A',
            'english', 'Wally', 'A',
        ))

    def test_unindex_doc(self):
        index = self._make_one()
        index.cursor.executed = executed = []
        index.unindex_doc(7)
        lines, params = self._format_executed(executed)
        self.assertEqual(lines, [
            'LOCK pgtextindex IN EXCLUSIVE MODE;',
            'DELETE FROM pgtextindex',
            'WHERE docid = %s',
        ])
        self.assertEqual(params, (7,))

    def test_clear(self):
        index = self._make_one()
        index.cursor.executed = executed = []
        index.clear()
        lines, params = self._format_executed(executed)
        self.assertEqual(lines, [
            'LOCK pgtextindex IN EXCLUSIVE MODE;',
            'DELETE FROM pgtextindex',
        ])
        self.assertEqual(params, None)

    def test_apply_success(self):
        index = self._make_one()
        index.cursor.executed = executed = []
        res = index.apply('Waldo Wally')
        lines, params = self._format_executed(executed)
        self.assertEqual(lines, [
            'SELECT docid, ts_rank_cd(text_vector, query) AS rank',
            'FROM pgtextindex, to_tsquery(%s, %s) query',
            'WHERE text_vector @@ query',
            'ORDER BY rank DESC',
        ])
        self.assertEqual(params,
            ('english', "( 'Waldo' ) & ( 'Wally' )"))
        self.assertTrue(isinstance(res, index.family.IF.Bucket))
        self.assertEqual(len(res), 2)

    def test_apply_intersect_no_docids(self):
        index = self._make_one()
        index.cursor.executed = executed = []
        res = index.apply_intersect('Waldo', [])
        self.assertTrue(isinstance(res, index.family.IF.Bucket))
        self.assertEqual(len(res), 0)
        self.assertEqual(len(executed), 0)

    def test_apply_intersect_with_docids(self):
        index = self._make_one()
        index.cursor.executed = executed = []
        res = index.apply_intersect('Waldo', [8,6,7])
        self.assertTrue(isinstance(res, index.family.IF.Bucket))
        self.assertEqual(len(res), 2)
        lines, params = self._format_executed(executed)
        self.assertEqual(lines, [
            'SELECT docid, ts_rank_cd(text_vector, query) AS rank',
            'FROM pgtextindex, to_tsquery(%s, %s) query',
            'WHERE text_vector @@ query',
            'AND docid IN (8,6,7)',
            'ORDER BY rank DESC',
        ])
        self.assertEqual(params, ('english', "'Waldo'"))

    def test_sort_nothing(self):
        index = self._make_one()
        self.assertEqual(index.sort({}), {})

    def test_sort_unweighted(self):
        index = self._make_one()
        self.assertRaises(TypeError, index.sort, [5])

    def test_sort_normal(self):
        index = self._make_one()
        bucket = index.family.IF.Bucket([
            (8, 0.3),
            (4, -0.5),
            (9, 0.0),
        ])
        res = index.sort(bucket)
        self.assertEqual(res, [8, 9, 4])

    def test_sort_reverse(self):
        index = self._make_one()
        bucket = index.family.IF.Bucket([
            (8, 0.3),
            (4, -0.5),
            (9, 0.0),
        ])
        res = index.sort(bucket, reverse=True)
        self.assertEqual(res, [4, 9, 8])

    def test_sort_limited(self):
        index = self._make_one()
        bucket = index.family.IF.Bucket([
            (8, 0.3),
            (4, -0.5),
            (9, 0.0),
        ])
        res = index.sort(bucket, limit=2)
        self.assertEqual(res, [8, 9])


class DummyConnectionManager:
    closed = False

    def __init__(self, dsn):
        self.dsn = dsn
        self.connection = DummyConnection()
        self.cursor = DummyCursor()

    def close(self):
        self.closed = True


class DummyConnection:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class DummyCursor:
    def __init__(self):
        self.executed = []

    def execute(self, stmt, params=None):
        self.executed.append((stmt, params))

    def __iter__(self):
        """Return an iterable of (docid, score) tuples"""
        return iter([(5, 1.3), (6, 0.7)])
