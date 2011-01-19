
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
            self.unindex_doc(docid)
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
        # else there is nothing to add to the database.

    reindex_doc = index_doc

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

    def apply(self, query):
        """Apply an index to the given query

        The type of the query is index specific.

        A result is returned that is:

        - An IFBTree or an IFBucket mapping document ids to floating-point
          scores for document ids of documents that match the query,

        - An IFSet or IFTreeSet containing document ids of documents
          that match the query, or

        - None, indicating that the index could not use the query and
          that the result should have no impact on determining a final
          result.

        """
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
