
from persistent import Persistent
from repoze.catalog.interfaces import ICatalogIndex
from repoze.pgtextindex.queryconvert import convert_query
from repoze.pgtextindex.db import get_connection_manager
from zope.index.interfaces import IIndexSort
from zope.interface import implements
import BTrees
import psycopg2

_marker = object()

class PGTextIndex(Persistent):
    implements(ICatalogIndex, IIndexSort)

    family = BTrees.family32

    def __init__(self,
            discriminator,
            dsn,
            table='pgtextindex',
            database_name='pgtextindex',
            ts_config='english'):
        if not callable(discriminator):
            if not isinstance(discriminator, basestring):
                raise ValueError('discriminator value must be callable or a '
                                 'string')
        self.discriminator = discriminator
        self.dsn = dsn
        self.table = table
        self._subs = dict(table=table)  # map of query string substitutions
        self.database_name = database_name
        self.ts_config = ts_config
        self.drop_and_create()

    def drop_and_create(self):
        conn = psycopg2.connect(self.dsn)
        cursor = conn.cursor()
        try:
            # TODO: use a separate lock table?

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
            cursor.close()
            conn.close()

    @property
    def read_cursor(self):
        m = get_connection_manager(self._p_jar, self.dsn, self.database_name)
        return m.cursor

    @property
    def write_cursor(self):
        m = get_connection_manager(self._p_jar, self.dsn, self.database_name)
        m.set_changed()
        return m.cursor

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
        params = [docid]
        if isinstance(value, basestring):
            value = [value]
        elif not value:
            value = ['']

        # apply the highest weight to the first string,
        # progressively lower weight to successive strings,
        # and the default weight to the last string.
        for i, text in enumerate(value[:-1]):
            if text:
                # PostgreSQL supports 4 weights: A, B, C, and Default.
                weight = 'ABC'[min(i, 2)]
                clauses.append('setweight(to_tsvector(%s, %s), %s)')
                params.extend([self.ts_config, text, weight])
        clauses.append('to_tsvector(%s, %s)')
        params.extend([self.ts_config, value[-1]])

        clause = ' || '.join(clauses)
        stmt = """
        LOCK %(table)s IN EXCLUSIVE MODE;
        INSERT INTO %(table)s (docid, text_vector)
        VALUES (%%s, %(clause)s)
        """ % {'table': self.table, 'clause': clause}
        self.write_cursor.execute(stmt, tuple(params))

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
        self.write_cursor.execute(stmt, (docid,))

    def clear(self):
        """Unindex all documents indexed by the index
        """
        stmt = """
        LOCK %(table)s IN EXCLUSIVE MODE;
        DELETE FROM %(table)s
        """ % self._subs
        self.write_cursor.execute(stmt)

    def reindex_doc(self, docid, obj):
        """ Reindex the document numbered ``docid`` using in the
        information on object ``obj``"""
        self.unindex_doc(docid)
        self.index_doc(docid, obj)

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
        cursor = self.read_cursor
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
        cursor = self.read_cursor
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

