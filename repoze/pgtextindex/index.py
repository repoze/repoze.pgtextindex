
from persistent import Persistent
from repoze.catalog.interfaces import ICatalogIndex
from repoze.pgtextindex.db import PostgresConnectionManager
from repoze.pgtextindex.queryconvert import convert_query
from zope.index.interfaces import IIndexSort
from zope.interface import implements
import BTrees

import logging

_marker = object()
log = logging.getLogger(__name__)


class PGTextIndex(Persistent):
    implements(ICatalogIndex, IIndexSort)

    family = BTrees.family32
    connection_manager_factory = PostgresConnectionManager
    _v_temp_cm = None  # A PostgresConnectionManager used during initialization

    def __init__(self,
                 discriminator,
                 dsn,
                 table='pgtextindex',
                 ts_config='english',
                 connection_manager_factory=None,
                 drop_and_create=True
                 ):

        if not callable(discriminator):
            if not isinstance(discriminator, basestring):
                raise ValueError('discriminator value must be callable or a '
                                 'string')
        self.discriminator = discriminator
        self.dsn = dsn
        self.table = table
        self._subs = dict(table=table)  # map of query string substitutions
        self.ts_config = ts_config
        if connection_manager_factory is not None:
            self.connection_manager_factory = connection_manager_factory
        if drop_and_create:
            self.drop_and_create()

    @property
    def connection_manager(self):
        jar = self._p_jar
        oid = self._p_oid

        if jar is None or oid is None:
            # Not yet stored in ZODB, so use _v_temp_cm
            cm = self._v_temp_cm
            if cm is None or cm.dsn != self.dsn:
                cm = self.connection_manager_factory(self.dsn)
                self._v_temp_cm = cm

        else:
            fc = getattr(jar, 'foreign_connections', None)
            if fc is None:
                jar.foreign_connections = fc = {}
                _mp_release_resources(jar)

            cm = fc.get(oid)
            if cm is None or cm.dsn != self.dsn:
                cm = self.connection_manager_factory(self.dsn)
                fc[oid] = cm
                self._v_temp_cm = None

        return cm

    def drop_and_create(self):
        cm = self.connection_manager
        conn = cm.connection
        cursor = cm.cursor
        try:
            # create the table with 2 columns: the integer docid
            # and a tsvector object.
            stmt = """
            DROP TABLE IF EXISTS %(table)s;

            CREATE TABLE %(table)s (
                docid INTEGER NOT NULL PRIMARY KEY,
                text_vector tsvector
            );

            CREATE INDEX %(table)s_index
                ON %(table)s
                USING gin(text_vector)
            """ % self._subs
            cursor.execute(stmt)

            conn.commit()
        finally:
            cm.close()

    @property
    def cursor(self):
        return self.connection_manager.cursor

    @property
    def connection(self):
        return self.connection_manager.connection

    def index_doc(self, docid, obj):
        """Add a document to the index.

        docid: int, identifying the document

        value: the value to be indexed: either a list of strings,
            where each string has a progressively lower weight, or
            a single string.

        return: None

        This can also be used to reindex documents.
        """
        if callable(self.discriminator):
            value = self.discriminator(obj, _marker)
        else:
            value = getattr(obj, self.discriminator, _marker)

        if value is _marker:
            # unindex the previous value
            self._index_null(docid)
            return None

        if isinstance(value, Persistent):
            raise ValueError('Catalog cannot index persistent object %s' %
                             value)

        clauses = []
        params = [docid, docid]
        if isinstance(value, basestring):
            value = [value]
        elif not value:
            value = ['']

        # apply the highest weight to the first string,
        # progressively lower weight to successive strings,
        # and the default weight to the last string.
        for i, texts in enumerate(value[:-1]):
            if texts:
                if isinstance(texts, basestring):
                    texts = [texts]
                for text in texts:
                    if not text:
                        continue
                    # PostgreSQL supports 4 weights: A, B, C, and Default.
                    weight = 'ABC'[min(i, 2)]
                    clauses.append('setweight(to_tsvector(%s, %s), %s)')
                    params.extend([self.ts_config, text, weight])

        texts = value[-1]
        if isinstance(texts, basestring):
            texts = [texts]
        for text in texts:
            if not text:
                continue
            clauses.append('to_tsvector(%s, %s)')
            params.extend([self.ts_config, text])

        if len(params) > 2:
            clause = ' || '.join(clauses)
            stmt = """
            LOCK %(table)s IN EXCLUSIVE MODE;
            DELETE FROM %(table)s WHERE docid = %%s;
            INSERT INTO %(table)s (docid, text_vector)
            VALUES (%%s, %(clause)s)
            """ % {'table': self.table, 'clause': clause}
            self.cursor.execute(stmt, tuple(params))

        else:
            self._index_null(docid)

    reindex_doc = index_doc

    def _index_null(self, docid):
        stmt = """
        LOCK %(table)s IN EXCLUSIVE MODE;
        DELETE FROM %(table)s WHERE docid = %%s;
        INSERT INTO %(table)s (docid, text_vector)
        VALUES (%%s, null)
        """ % {'table': self.table}
        self.cursor.execute(stmt, (docid, docid))

    def unindex_doc(self, docid):
        """Remove a document from the index.

        docid: int, identifying the document

        return: None

        This call is a no-op if the docid isn't in the index, however,
        after this call, the index should have no references to the docid.
        """
        stmt = """
        LOCK %(table)s IN EXCLUSIVE MODE;
        DELETE FROM %(table)s
        WHERE docid = %%s
        """ % self._subs
        self.cursor.execute(stmt, (docid,))

    def clear(self):
        """Unindex all documents indexed by the index
        """
        stmt = """
        LOCK %(table)s IN EXCLUSIVE MODE;
        DELETE FROM %(table)s
        """ % self._subs
        self.cursor.execute(stmt)

    def applyContains(self, query):
        s = convert_query(query)
        stmt = """
        SELECT docid, ts_rank_cd(text_vector, query) AS rank
        FROM %s, to_tsquery(%%s, %%s) query
        WHERE text_vector @@ query
        ORDER BY rank DESC
        """ % self.table
        cursor = self.cursor
        cursor.execute(stmt, (self.ts_config, s))
        data = list(cursor)
        res = self.family.IF.Bucket()
        res.update(data)
        return res

    def applyDoesNotContain(self, query):
        s = convert_query(query)
        stmt = """
        SELECT docid, ts_rank_cd(text_vector, query) AS rank
        FROM %s, to_tsquery(%%s, %%s) query
        WHERE NOT(text_vector @@ query)
        ORDER BY rank DESC
        """ % self.table
        cursor = self.cursor
        cursor.execute(stmt, (self.ts_config, s))
        data = list(cursor)
        res = self.family.IF.Bucket()
        res.update(data)
        return res

    apply = applyEq = applyContains
    applyNotEq = applyDoesNotContain

    def docids(self):
        """
        Return all docids in the index.
        """
        stmt = """
        SELECT docid FROM %s
        """ % self.table
        cursor = self.cursor
        cursor.execute(stmt)
        res = self.family.IF.Set()
        for row in cursor:
            res.add(row[0])
        return res

    def get_contextual_summary(self, raw_text, query, **options):
        """BBB: get just one contextual summary."""
        return self.get_contextual_summaries([raw_text], query, **options)[0]

    def get_contextual_summaries(self, raw_texts, query, **options):
        """Get contextual summaries for each of several search results.

        Produces a list of the same length as the raw_texts sequence.
        For each raw_text, returns snippets of text with the words in
        the query highlighted using the html <b> tag. Calls the
        PostgreSQL function 'ts_headline'. Options are turned into an
        options string passed to 'ts_headline'. See the documentation
        for PostgreSQL for more information on the options that can be
        passed to 'ts_headline'.
        """
        if not raw_texts:
            return []
        s = convert_query(query)
        options = ','.join(['%s=%s' % (k, v) for k, v in options.items()])
        stmt = """
        SELECT ts_headline(%s, doc.text, to_tsquery(%s, %s), %s)
        FROM (VALUES <values>) AS doc (text)
        """
        stmt = stmt.replace('<values>', ', '.join(('(%s)',) * len(raw_texts)))
        cursor = self.cursor
        params = (self.ts_config, self.ts_config, s, options)
        cursor.execute(stmt, params + tuple(raw_texts))
        return [summary.decode(self.connection.encoding)
            for (summary,) in cursor]

    def apply_intersect(self, query, docids):
        """ Run the query implied by query, and return query results
        intersected with the ``docids`` set that is supplied.  If
        ``docids`` is None, return the bare query results.
        """
        if not docids:
            return self.family.IF.Bucket()
        docidstr = ','.join(str(docid) for docid in docids)

        s = convert_query(query)
        stmt = """
        SELECT docid, ts_rank_cd(text_vector, query) AS rank
        FROM %s, to_tsquery(%%s, %%s) query
        WHERE text_vector @@ query
            AND docid IN (%s)
        ORDER BY rank DESC
        """ % (self.table, docidstr)
        cursor = self.cursor
        cursor.execute(stmt, (self.ts_config, s))
        data = list(cursor)
        res = self.family.IF.Bucket()
        res.update(data)
        return res

    def sort(self, result, reverse=False, limit=None, sort_type=None):
        """Sort by text relevance.

        This only works if the query includes at least one text query,
        leading to a weighted result.  This method raises TypeError
        if the result is not weighted.

        A weighted result is a dictionary-ish object that has docids
        as keys and floating point weights as values.  This method
        sorts the dictionary by weight and returns the sorted
        docids as a list.
        """
        if not result:
            return result

        if not hasattr(result, 'items'):
            raise TypeError(
                "Unable to sort by relevance because the search "
                "result does not contain weights. To produce a weighted "
                "result, include a text search in the query.")

        items = [(weight, docid) for (docid, weight) in result.items()]
        # when reverse is false, output largest weight first.
        # when reverse is true, output smallest weight first.
        items.sort(reverse=not reverse)
        result = [docid for (weight, docid) in items]
        if limit:
            result = result[:limit]
        return result

    def applyGt(self, *args, **kw):
        raise NotImplementedError(
            "Gt is not supported for %s" % type(self).__name__)

    def applyLt(self, *args, **kw):
        raise NotImplementedError(
            "Lt is not supported for %s" % type(self).__name__)

    def applyGe(self, *args, **kw):
        raise NotImplementedError(
            "Ge is not supported for %s" % type(self).__name__)

    def applyLe(self, *args, **kw):
        raise NotImplementedError(
            "Le is not supported for %s" % type(self).__name__)

    def applyAny(self, *args, **kw):
        raise NotImplementedError(
            "Any is not supported for %s" % type(self).__name__)

    def applyNotAny(self, *args, **kw):
        raise NotImplementedError(
            "NotAny is not supported for %s" % type(self).__name__)

    def applyAll(self, *args, **kw):
        raise NotImplementedError(
            "All is not supported for %s" % type(self).__name__)

    def applyNotAll(self, *args, **kw):
        raise NotImplementedError(
            "NotAll is not supported for %s" % type(self).__name__)

    def applyInRange(self, *args, **kw):
        raise NotImplementedError(
            "InRange is not supported for %s" % type(self).__name__)

    def applyNotInRange(self, *args, **kw):
        raise NotImplementedError(
            "NotInRange is not supported for %s" % type(self).__name__)

    def _migrate_to_0_8_0(self, docids):
        """
        Seaver's law: "Persistence means always having to say you're sorry."

        Insert null value rows for docs that are in catalog but don't have
        values for this index.
        """
        for docid in self.family.IF.difference(docids, self.docids()):
            self._index_null(docid)


def _mp_release_resources(jar):
    """
    Monkey patch ZODB.DB.Connection._release_resources() in order to cause our
    Postgres connections to get closed whenever the ZODB connection we've
    piggy backed on top of is evicted from the connection pool.  This is only
    known to work with ZODB 3.10.X.
    """
    wrapped = getattr(jar, '_release_resources', None)
    if wrapped is None:
        log.warn(
            "Unable to close Postgres connections when ZODB garbage collects "
            "a connection from it's pool. This is only known to work with "
            "ZODB 3.10.X. In practice, the ZODB tends to rarely garbage "
            "collect a connection, keeping a handful of connections open for "
            "the life of the process, so this may not be problematic.")
        return

    def _release_resources():
        wrapped()
        for fc in jar.foreign_connections.values():
            close = getattr(fc, 'close', None)
            if close is not None:
                close()
        del jar.foreign_connections
    jar._release_resources = _release_resources
