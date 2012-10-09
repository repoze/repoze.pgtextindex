
import unittest

class TestPGTextIndex(unittest.TestCase):

    @property
    def _class(self):
        from repoze.pgtextindex.index import PGTextIndex
        return PGTextIndex

    def _make_one(self, discriminator=None, dsn="dbname=dummy",
                  results=((5, 1.3), (6, 0.7)), execute_errors=None,
                  rowcounts=(1,), **kw):
        if discriminator is None:
            def discriminator(obj, default):
                return obj

        self.executed = executed = []
        self.commits = commits = []
        self.rollbacks = rollbacks = []
        results = list(results)
        rowcounts = list(rowcounts)

        class DummyConnectionManager:
            closed = False

            def __init__(self, dsn):
                self.dsn = dsn
                self.connection = DummyConnection()
                self.cursor = DummyCursor()

            def close(self):
                self.closed = True

        class DummyConnection:
            encoding = 'UTF-8'

            def commit(self):
                commits.append(1)

            def rollback(self):
                rollbacks.append(1)

        class DummyCursor:
            def execute(self, stmt, params=None):
                executed.append((stmt, params))
                if execute_errors:
                    error = execute_errors.pop(0)
                    if error is not None:
                        raise error
                if rowcounts:
                    self.rowcount = rowcounts.pop(0)
                else:
                    self.rowcount = 0

            def __iter__(self):
                """Return an iterable of (docid, score) tuples"""
                return iter(results)

            def fetchone(self):
                return ['one']

        return self._class(discriminator, dsn,
            connection_manager_factory=DummyConnectionManager, **kw)

    def test_class_conforms_to_ICatalogIndex(self):
        from zope.interface.verify import verifyClass
        from repoze.catalog.interfaces import ICatalogIndex
        verifyClass(ICatalogIndex, self._class)

    def test_instance_conforms_to_ICatalogIndex(self):
        from zope.interface.verify import verifyObject
        from repoze.catalog.interfaces import ICatalogIndex
        verifyObject(ICatalogIndex, self._make_one())

    def test_class_conforms_to_IIndexSort(self):
        from zope.interface.verify import verifyClass
        from zope.index.interfaces import IIndexSort
        verifyClass(IIndexSort, self._class)

    def test_instance_conforms_to_IIndexSort(self):
        from zope.interface.verify import verifyObject
        from zope.index.interfaces import IIndexSort
        verifyObject(IIndexSort, self._make_one())

    def test_ctor_invalid_discriminator(self):
        self.assertRaises(ValueError, self._make_one,
            object())

    def test_ctor_drop_and_create(self):
        index = self._make_one(drop_and_create=True)
        self.assertNotEqual(None, index._v_temp_cm)

    def test_connection_manager_from_volatile_attr(self):
        index = self._make_one()
        self.assertEqual(None, index._v_temp_cm)
        cm = index.connection_manager
        self.assertEqual(cm, index._v_temp_cm)

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
        index.index_doc(6, None)
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=null',
                          'WHERE docid=%s'])
        self.assertEqual(params, ('0.0', None, 6))

    def test_index_doc_empty_string(self):
        index = self._make_one()
        index.index_doc(6, '')
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=null',
                          'WHERE docid=%s'])
        self.assertEqual(params, ('0.0', None, 6))

    def test_index_doc_empty_strings_weighted(self):
        index = self._make_one()
        index.index_doc(6, [['', ''], ''])
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=setweight(to_tsvector(%s, %s), %s)',
                          'WHERE docid=%s'])
        self.assertEqual(params, (1.0, None, 'english', "['', '']", 'A', 6))

    def test_index_doc_unweighted(self):
        index = self._make_one()
        index.index_doc(5, 'Waldo')
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=to_tsvector(%s, %s)',
                          'WHERE docid=%s'])
        self.assertEqual(params, (1.0, None, 'english', 'Waldo', 5))

    def test_index_doc_unweighted_long(self):
        text = 'Waldo ' * 174763 # Over 1MB
        index = self._make_one()
        index.index_doc(5, text)
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=to_tsvector(%s, %s)',
                          'WHERE docid=%s'])
        self.assertEqual(
            params, (1.0, None, 'english', text[:1048571], 5))

    def test_index_doc_using_attr_discriminator(self):
        class DummyObject:
            name = 'Osvaldo'

        index = self._make_one('name')
        index.index_doc(6, DummyObject())
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=to_tsvector(%s, %s)',
                          'WHERE docid=%s'])
        self.assertEqual(params, (1.0, None, 'english', 'Osvaldo', 6))

    def test_index_doc_missing_value(self):
        def discriminator(obj, default):
            return default

        index = self._make_one(discriminator)
        index.index_doc(6, 'dummy')
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=null',
                          'WHERE docid=%s'])
        self.assertEqual(params, ('0.0', None, 6))

    def test_index_doc_persistent_object(self):
        # Unlike other indexes, PGTextIndex allows persistent objects
        # because there is no chance the index will accidentally hold a
        # persistent object reference.
        from persistent import Persistent

        class DummyObject(Persistent):
            def __str__(self):
                return 'x'

        index = self._make_one()
        index.index_doc(6, DummyObject())
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=to_tsvector(%s, %s)',
                          'WHERE docid=%s'])
        self.assertEqual(params, (1.0, None, 'english', 'x', 6))

    def test_index_doc_use_one_weight(self):
        index = self._make_one()
        index.index_doc(5, ['Waldo', 'character'])
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=to_tsvector(%s, %s) || '
                          'setweight(to_tsvector(%s, %s), %s)',
                          'WHERE docid=%s'])
        self.assertEqual(params, (1.0, None,
                                  'english', 'character',
                                  'english', 'Waldo', 'A',
                                  5))

    def test_index_doc_use_one_weight_long(self):
        text1 = 'Waldo ' * 174763 # Over 1MB
        text2 = 'Baldo ' * 174763 # Over 1MB
        index = self._make_one()
        index.index_doc(5, [text1, text2])
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=to_tsvector(%s, %s) || '
                          'setweight(to_tsvector(%s, %s), %s)',
                          'WHERE docid=%s'])
        self.assertEqual(params, (1.0, None,
                                  'english', text2[:1048571],
                                  'english', text1[:1048571], 'A',
                                  5))

    def test_index_doc_use_more_than_all_possible_weights(self):
        index = self._make_one()
        index.index_doc(5, ['Waldo', 'character', 'boy', 'person', 'entity'])
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=to_tsvector(%s, %s) || '
                              'setweight(to_tsvector(%s, %s), %s) || '
                              'setweight(to_tsvector(%s, %s), %s) || '
                              'setweight(to_tsvector(%s, %s), %s)',
                          'WHERE docid=%s'])
        self.assertEqual(params, (1.0, None,
                                  'english', 'person entity',
                                  'english', 'Waldo', 'A',
                                  'english', 'character', 'B',
                                  'english', 'boy', 'C',
                                  5))

    def test_index_doc_skip_weights(self):
        index = self._make_one()
        index.index_doc(5, ['Waldo', '', 'boy', ''])
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=setweight(to_tsvector(%s, %s), %s) || '
                              'setweight(to_tsvector(%s, %s), %s)',
                          'WHERE docid=%s'])
        self.assertEqual(params, (1.0, None,
                                  'english', 'Waldo', 'A',
                                  'english', 'boy', 'C',
                                  5))

    def test_index_doc_with_marker(self):
        index = self._make_one()

        from repoze.pgtextindex.interfaces import IWeightedText
        from zope.interface import implements

        class DummyText(unicode):
            implements(IWeightedText)
            marker = 'book'

        index.index_doc(5, DummyText('Where is Waldo'))
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=to_tsvector(%s, %s)',
                          'WHERE docid=%s'])
        self.assertEqual(params, (1.0, 'book', 'english', 'Where is Waldo', 5))

    def test_index_doc_multiple_texts_with_the_same_weight(self):
        index = self._make_one()
        index.index_doc(5, [['Waldo', 'Wally'], ''])
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=setweight(to_tsvector(%s, %s), %s)',
                          'WHERE docid=%s'])
        self.assertEqual(params, (1.0, None,
                                  'english', "['Waldo', 'Wally']", 'A', 5))

    def test_index_doc_using_insert_without_conflict(self):
        index = self._make_one(rowcounts=())
        sleeps = []
        index.sleep = sleeps.append
        index.index_doc(5, 'Waldo')
        self.assertEqual(len(sleeps), 0)
        self.assertEqual(len(self.executed), 3)

        lines, params = self._format_executed(self.executed[0:1])
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=to_tsvector(%s, %s)',
                          'WHERE docid=%s'])
        self.assertEqual(params, (1.0, None, 'english', 'Waldo', 5))

        lines, params = self._format_executed(self.executed[1:2])
        self.assertEqual(lines,
                         ['SAVEPOINT pgtextindex_upsert;',
                          'INSERT INTO pgtextindex '
                              '(docid, coefficient, marker, text_vector)',
                          'VALUES (%s, %s, %s, to_tsvector(%s, %s))'])
        self.assertEqual(params, (5, 1.0, None, 'english', 'Waldo'))

        lines, params = self._format_executed(self.executed[2:3])
        self.assertEqual(lines, ['RELEASE SAVEPOINT pgtextindex_upsert'])
        self.assertEqual(params, None)

    def test_index_doc_using_insert_with_one_integrity_error(self):
        import psycopg2
        index = self._make_one(execute_errors=[None, psycopg2.IntegrityError,
                                               None, None],
                               rowcounts=())
        sleeps = []
        index.sleep = sleeps.append
        index.index_doc(5, 'Waldo')
        self.assertEqual(len(sleeps), 1)
        self.assertEqual(len(self.executed), 6)

        lines, params = self._format_executed(self.executed[0:1])
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=to_tsvector(%s, %s)',
                          'WHERE docid=%s'])
        self.assertEqual(params, (1.0, None, 'english', 'Waldo', 5))

        lines, params = self._format_executed(self.executed[1:2])
        self.assertEqual(lines,
                         ['SAVEPOINT pgtextindex_upsert;',
                          'INSERT INTO pgtextindex '
                              '(docid, coefficient, marker, text_vector)',
                          'VALUES (%s, %s, %s, to_tsvector(%s, %s))'])
        self.assertEqual(params, (5, 1.0, None, 'english', 'Waldo'))

        lines, params = self._format_executed(self.executed[2:3])
        self.assertEqual(lines, ['ROLLBACK TO SAVEPOINT pgtextindex_upsert'])
        self.assertEqual(params, None)

        lines, params = self._format_executed(self.executed[3:4])
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=to_tsvector(%s, %s)',
                          'WHERE docid=%s'])
        self.assertEqual(params, (1.0, None, 'english', 'Waldo', 5))

        lines, params = self._format_executed(self.executed[4:5])
        self.assertEqual(lines,
                         ['SAVEPOINT pgtextindex_upsert;',
                          'INSERT INTO pgtextindex '
                              '(docid, coefficient, marker, text_vector)',
                          'VALUES (%s, %s, %s, to_tsvector(%s, %s))'])
        self.assertEqual(params, (5, 1.0, None, 'english', 'Waldo'))

        lines, params = self._format_executed(self.executed[5:6])
        self.assertEqual(lines, ['RELEASE SAVEPOINT pgtextindex_upsert'])
        self.assertEqual(params, None)

    def test_index_doc_with_three_integrity_errors(self):
        import psycopg2
        execute_errors = [None, psycopg2.IntegrityError, None] * 3
        index = self._make_one(execute_errors=execute_errors, rowcounts=())
        sleeps = []
        index.sleep = sleeps.append
        self.assertRaises(psycopg2.IntegrityError, index.index_doc, 5, 'Waldo')
        self.assertEqual(len(sleeps), 2)
        self.assertEqual(len(self.executed), 8)

    def test_unindex_doc(self):
        index = self._make_one()
        index.unindex_doc(7)
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'DELETE FROM pgtextindex WHERE docid = %s',
        ])
        self.assertEqual(params, (7,))

    def test_clear(self):
        index = self._make_one()
        index.clear()
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'DELETE FROM pgtextindex',
        ])
        self.assertEqual(params, None)

    def test_apply_simple(self):
        index = self._make_one()
        res = index.apply('Waldo Wally')
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'SELECT docid, coefficient *',
            "ts_rank_cd(text_vector, to_tsquery(%s, %s)) AS rank",
            'FROM pgtextindex',
            'WHERE (text_vector @@ to_tsquery(%s, %s))',
            'ORDER BY rank DESC',
        ])
        self.assertEqual(params, ('english', "( 'Waldo' ) & ( 'Wally' )",
                                  'english', "( 'Waldo' ) & ( 'Wally' )"))
        self.assertTrue(isinstance(res, index.family.IF.Bucket))
        self.assertEqual(len(res), 2)

    def test_applyEq(self):
        index = self._make_one()
        res = index.applyEq('Waldo Wally')
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'SELECT docid, coefficient *',
            "ts_rank_cd(text_vector, to_tsquery(%s, %s)) AS rank",
            'FROM pgtextindex',
            'WHERE (text_vector @@ to_tsquery(%s, %s))',
            'ORDER BY rank DESC',
        ])
        self.assertEqual(params, ('english', "( 'Waldo' ) & ( 'Wally' )",
                                  'english', "( 'Waldo' ) & ( 'Wally' )"))
        self.assertTrue(isinstance(res, index.family.IF.Bucket))
        self.assertEqual(len(res), 2)

    def test_applyContains(self):
        index = self._make_one()
        res = index.applyContains('Waldo Wally')
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'SELECT docid, coefficient *',
            "ts_rank_cd(text_vector, to_tsquery(%s, %s)) AS rank",
            'FROM pgtextindex',
            'WHERE (text_vector @@ to_tsquery(%s, %s))',
            'ORDER BY rank DESC',
        ])
        self.assertEqual(params, ('english', "( 'Waldo' ) & ( 'Wally' )",
                                  'english', "( 'Waldo' ) & ( 'Wally' )"))
        self.assertTrue(isinstance(res, index.family.IF.Bucket))
        self.assertEqual(len(res), 2)

    def test_applyNotEq(self):
        index = self._make_one()
        res = index.applyNotEq('Waldo Wally')
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'SELECT docid, coefficient *',
            "ts_rank_cd(text_vector, to_tsquery(%s, %s)) AS rank",
            'FROM pgtextindex',
            'WHERE NOT(text_vector @@ to_tsquery(%s, %s))',
            'ORDER BY rank DESC',
        ])
        self.assertEqual(params, ('english', "( 'Waldo' ) & ( 'Wally' )",
                                  'english', "( 'Waldo' ) & ( 'Wally' )"))
        self.assertTrue(isinstance(res, index.family.IF.Bucket))
        self.assertEqual(len(res), 2)

    def test_applyDoesNotContain(self):
        index = self._make_one()
        res = index.applyDoesNotContain('Waldo Wally')
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'SELECT docid, coefficient *',
            "ts_rank_cd(text_vector, to_tsquery(%s, %s)) AS rank",
            'FROM pgtextindex',
            'WHERE NOT(text_vector @@ to_tsquery(%s, %s))',
            'ORDER BY rank DESC',
        ])
        self.assertEqual(params, ('english', "( 'Waldo' ) & ( 'Wally' )",
                                  'english', "( 'Waldo' ) & ( 'Wally' )"))
        self.assertTrue(isinstance(res, index.family.IF.Bucket))
        self.assertEqual(len(res), 2)

    def test_apply_weighted_query_normal(self):
        index = self._make_one()

        from zope.interface import implements
        from repoze.pgtextindex.interfaces import IWeightedQuery

        class DummyWeightedQuery(unicode):
            implements(IWeightedQuery)
            A = 16 ** 3
            B = 16 ** 2
            C = 16
            D = 1

        q = DummyWeightedQuery('Waldo Wally')
        res = index.apply(q)
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'SELECT docid, coefficient *',
            "ts_rank_cd('{%s, %s, %s, %s}', "
                "text_vector, to_tsquery(%s, %s)) AS rank",
            'FROM pgtextindex',
            'WHERE (text_vector @@ to_tsquery(%s, %s))',
            'ORDER BY rank DESC',
        ])
        self.assertEqual(params, (1, 16, 256, 4096,
                                  'english', "( 'Waldo' ) & ( 'Wally' )",
                                  'english', "( 'Waldo' ) & ( 'Wally' )"))
        self.assertTrue(isinstance(res, index.family.IF.Bucket))
        self.assertEqual(len(res), 2)

    def test_apply_weighted_query_with_deprecated_text_method(self):
        index = self._make_one()

        from zope.interface import implements
        from repoze.pgtextindex.interfaces import IWeightedQuery

        class DummyWeightedQuery(unicode):
            implements(IWeightedQuery)
            A = 16 ** 3
            B = 16 ** 2
            C = 16
            D = 1
            text = 'Surly Susan'

        q = DummyWeightedQuery('Waldo Wally')
        res = index.apply(q)
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'SELECT docid, coefficient *',
            "ts_rank_cd('{%s, %s, %s, %s}', "
                "text_vector, to_tsquery(%s, %s)) AS rank",
            'FROM pgtextindex',
            'WHERE (text_vector @@ to_tsquery(%s, %s))',
            'ORDER BY rank DESC',
        ])
        self.assertEqual(params, (1, 16, 256, 4096,
                                  'english', "( 'Surly' ) & ( 'Susan' )",
                                  'english', "( 'Surly' ) & ( 'Susan' )"))
        self.assertTrue(isinstance(res, index.family.IF.Bucket))
        self.assertEqual(len(res), 2)

    def test_apply_with_marker(self):
        index = self._make_one()

        from zope.interface import implements
        from repoze.pgtextindex.interfaces import IWeightedQuery

        class DummyWeightedQuery(unicode):
            implements(IWeightedQuery)
            marker = 'book'

        q = DummyWeightedQuery('Waldo Wally')
        res = index.apply(q)
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'SELECT docid, coefficient *',
            "ts_rank_cd('{%s, %s, %s, %s}', "
                "text_vector, to_tsquery(%s, %s)) AS rank",
            'FROM pgtextindex',
            'WHERE (text_vector @@ to_tsquery(%s, %s))',
            'AND marker = %s',
            'ORDER BY rank DESC',
        ])
        self.assertEqual(params, (0.1, 0.2, 0.4, 1.0,
                                  'english', "( 'Waldo' ) & ( 'Wally' )",
                                  'english', "( 'Waldo' ) & ( 'Wally' )",
                                  'book'))
        self.assertTrue(isinstance(res, index.family.IF.Bucket))
        self.assertEqual(len(res), 2)

    def test_apply_with_limit_and_offset(self):
        index = self._make_one()

        from zope.interface import implements
        from repoze.pgtextindex.interfaces import IWeightedQuery

        class DummyWeightedQuery(unicode):
            implements(IWeightedQuery)
            limit = 5
            offset = 10

        q = DummyWeightedQuery('Waldo Wally')
        res = index.apply(q)
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'SELECT docid, coefficient *',
            "ts_rank_cd('{%s, %s, %s, %s}', "
                "text_vector, to_tsquery(%s, %s)) AS rank",
            'FROM pgtextindex',
            'WHERE (text_vector @@ to_tsquery(%s, %s))',
            'ORDER BY rank DESC',
            'LIMIT %s',
            'OFFSET %s',
        ])
        self.assertEqual(params, (0.1, 0.2, 0.4, 1.0,
                                  'english', "( 'Waldo' ) & ( 'Wally' )",
                                  'english', "( 'Waldo' ) & ( 'Wally' )",
                                  5, 10))
        self.assertTrue(isinstance(res, index.family.IF.Bucket))
        self.assertEqual(len(res), 2)

    def test_apply_with_all_weight_and_limit_features(self):
        index = self._make_one()

        from zope.interface import implements
        from repoze.pgtextindex.interfaces import IWeightedQuery

        class DummyWeightedQuery(unicode):
            implements(IWeightedQuery)
            A = 16 ** 3
            B = 16 ** 2
            C = 16
            D = 1
            marker = 'book'
            limit = 5
            offset = 10

        q = DummyWeightedQuery('Waldo Wally')
        res = index.apply(q)
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'SELECT docid, coefficient *',
            "ts_rank_cd('{%s, %s, %s, %s}', "
                "text_vector, to_tsquery(%s, %s)) AS rank",
            'FROM pgtextindex',
            'WHERE (text_vector @@ to_tsquery(%s, %s))',
            'AND marker = %s',
            'ORDER BY rank DESC',
            'LIMIT %s',
            'OFFSET %s',
        ])
        self.assertEqual(params, (1, 16, 256, 4096,
                                  'english', "( 'Waldo' ) & ( 'Wally' )",
                                  'english', "( 'Waldo' ) & ( 'Wally' )",
                                  'book', 5, 10))
        self.assertTrue(isinstance(res, index.family.IF.Bucket))
        self.assertEqual(len(res), 2)

    def test_docids(self):
        index = self._make_one(results=((5,), (6,)))
        res = index.docids()
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'SELECT docid FROM pgtextindex',
        ])
        self.assertEqual(params, None)
        self.assertTrue(isinstance(res, index.family.IF.Set))
        self.assertEqual(len(res), 2)

    def test_apply_intersect_with_no_docids(self):
        index = self._make_one()
        res = index.apply_intersect('Waldo', [])
        self.assertTrue(isinstance(res, index.family.IF.Bucket))
        self.assertEqual(len(res), 0)
        self.assertEqual(len(self.executed), 0)

    def test_apply_intersect_with_docids(self):
        index = self._make_one()
        res = index.apply_intersect('Waldo', [8,6,7])
        self.assertTrue(isinstance(res, index.family.IF.Bucket))
        self.assertEqual(len(res), 2)
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'SELECT docid, coefficient *',
            "ts_rank_cd(text_vector, to_tsquery(%s, %s)) AS rank",
            'FROM pgtextindex',
            'WHERE (text_vector @@ to_tsquery(%s, %s))',
            'AND docid IN (8,6,7)',
            'ORDER BY rank DESC',
        ])
        self.assertEqual(params, ('english', "'Waldo'", 'english', "'Waldo'"))

    def test_get_contextual_summary(self):
        index = self._make_one(results=[('<b>query</b>',)])
        res = index.get_contextual_summary('raw text', 'query', foo='bar')
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'SELECT ts_headline(%s, doc.text, to_tsquery(%s, %s), %s)',
            'FROM (VALUES (%s)) AS doc (text)',
        ])
        self.assertEqual(params,
            ('english', 'english', "'query'", 'foo=bar', 'raw text'))
        self.assertEqual(res, '<b>query</b>')

    def test_get_two_contextual_summaries(self):
        index = self._make_one(results=[('<b>query</b>',), ('<b>word</b>',)])
        raw_texts = ['raw 1', 'raw 2']
        res = index.get_contextual_summaries(raw_texts, 'query', foo='bar')
        lines, params = self._format_executed(self.executed)
        self.assertEqual(lines, [
            'SELECT ts_headline(%s, doc.text, to_tsquery(%s, %s), %s)',
            'FROM (VALUES (%s), (%s)) AS doc (text)',
        ])
        self.assertEqual(params,
            ('english', 'english', "'query'", 'foo=bar', 'raw 1', 'raw 2'))
        self.assertEqual(res, ['<b>query</b>', '<b>word</b>'])

    def test_get_zero_contextual_summaries(self):
        index = self._make_one()
        raw_texts = []
        res = index.get_contextual_summaries(raw_texts, 'query', foo='bar')
        self.assertFalse(self.executed)
        self.assertEqual(res, [])

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

    def test_unsupported_operations(self):
        index = self._make_one()
        for method in (index.applyGt,
                       index.applyLt,
                       index.applyGe,
                       index.applyLe,
                       index.applyAll,
                       index.applyNotAll,
                       index.applyAny,
                       index.applyNotAny,
                       index.applyInRange,
                       index.applyNotInRange):
            self.assertRaises(NotImplementedError, method, 'foo')

    def test_migrate_to_0_8_0(self):
        index = self._make_one(results=[(5,), (6,)], rowcounts=[0, 1])
        all_docids = index.family.IF.Set([5, 6, 7])
        index._migrate_to_0_8_0(all_docids)
        self.assertEqual(len(self.executed), 2)
        lines, params = self._format_executed(self.executed[0:1])
        self.assertEqual(lines, [
            'SELECT docid FROM pgtextindex',
        ])
        self.assertEqual(params, None)

        lines, params = self._format_executed(self.executed[1:2])
        self.assertEqual(lines,
                         ['UPDATE pgtextindex SET',
                          'coefficient=%s,',
                          'marker=%s,',
                          'text_vector=null',
                          'WHERE docid=%s'])
        self.assertEqual(params, ('0.0', None, 7))
