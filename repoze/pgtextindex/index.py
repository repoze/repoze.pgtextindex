
from persistent import Persistent
from repoze.catalog.interfaces import ICatalogIndex
from repoze.pgtextindex.db import PostgresConnectionManager
from repoze.pgtextindex.interfaces import IWeightedQuery
from repoze.pgtextindex.interfaces import IWeightedText
from repoze.pgtextindex.queryconvert import convert_query
from zope.index.interfaces import IIndexSort
from zope.interface import implements
import BTrees

import logging

_missing = object()
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
                 drop_and_create=False,
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
            # Create the table.
            stmt = """
            DROP TABLE IF EXISTS %(table)s;

            CREATE TABLE %(table)s (
                docid INTEGER NOT NULL PRIMARY KEY,
                coefficient REAL NOT NULL DEFAULT 1.0,
                marker CHARACTER VARYING,
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

        obj: The document to be indexed.

        The discriminator assigned to the index is used to extract a
        value from the document.  The value is either:

        - an object that implements IWeightedText, or

        - a string, or

        - a list or tuple of strings, which will be interpreted as weighted
          texts, with the default weight last, the A, B, and C weights
          first (if the list has enough strings), and other strings
          added to the default weight.

        return: None

        This can also be used to reindex documents.
        """
        if callable(self.discriminator):
            value = self.discriminator(obj, _missing)
        else:
            value = getattr(obj, self.discriminator, _missing)

        if value is _missing:
            # unindex the previous value
            self._index_null(docid)
            return None

        if isinstance(value, (list, tuple)):
            # Convert to a WeightedText.  The last value is always
            # a default text, then if there are other values, the first
            # value is assigned the A weight, the second is B, the third
            # is C, and the rest are assigned the default weight.
            abc = value[:-1][:3]
            kw = {'default': ' '.join(value[len(abc):])}
            value = SimpleWeightedText(*abc, **kw)

        clauses = []
        if IWeightedText.providedBy(value):
            coefficient = getattr(value, 'coefficient', 1.0)
            marker = getattr(value, 'marker', None)
            params = [docid, docid, coefficient, marker]
            text = '%s' % value  # Call the __str__() method
            if text:
                clauses.append('to_tsvector(%s, %s)')
                params.extend([self.ts_config, _truncate(text)])
            for weight in ('A', 'B', 'C'):
                text = getattr(value, weight, None)
                if text:
                    clauses.append('setweight(to_tsvector(%s, %s), %s)')
                    params.extend([self.ts_config, _truncate('%s' % text),
                                   weight])

        else:
            # The value is a simple string.  Strings can not
            # influence the weighting.
            params = [docid, docid, 1.0, None]
            if value:
                clauses.append('to_tsvector(%s, %s)')
                params.extend([self.ts_config, _truncate('%s' % value)])

        if len(params) > 4:
            clause = ' || '.join(clauses)
            stmt = """
            DELETE FROM %(table)s WHERE docid = %%s;
            INSERT INTO %(table)s (docid, coefficient, marker, text_vector)
            VALUES (%%s, %%s, %%s, %(clause)s)
            """ % {'table': self.table, 'clause': clause}
            self.cursor.execute(stmt, tuple(params))

        else:
            self._index_null(docid)

    reindex_doc = index_doc

    def _index_null(self, docid):
        stmt = """
        DELETE FROM %(table)s WHERE docid = %%s;
        INSERT INTO %(table)s (docid, coefficient, marker, text_vector)
        VALUES (%%s, 0.0, null, null)
        """ % {'table': self.table}
        self.cursor.execute(stmt, (docid, docid))

    def unindex_doc(self, docid):
        """Remove a document from the index.

        docid: int, identifying the document

        return: None

        This call is a no-op if the docid isn't in the index, however,
        after this call, the index should have no references to the docid.
        """
        stmt = "DELETE FROM %(table)s WHERE docid = %%s" % self._subs
        self.cursor.execute(stmt, (docid,))

    def clear(self):
        """Unindex all documents indexed by the index
        """
        stmt = "DELETE FROM %(table)s" % self._subs
        self.cursor.execute(stmt)

    def _run_query(self, query, invert=False, docids=None):
        kw = {
            'table': self.table,
            'weight': '',
            'not': '',
            'filter': '',
            'limit': '',
            'offset': '',
        }

        if invert:
            kw['not'] = 'NOT'

        if IWeightedQuery.providedBy(query):
            kw['weight'] = "'{%s, %s, %s, %s}', "
            text = getattr(query, 'text', None)
            if text is None:
                text = '%s' % query  # Use __str__()
            params = [
                getattr(query, 'D', 0.1),
                getattr(query, 'C', 0.2),
                getattr(query, 'B', 0.4),
                getattr(query, 'A', 1.0),
                self.ts_config,
                convert_query(text),
            ]
            marker = getattr(query, 'marker', None)
            if marker:
                kw['filter'] += " AND marker = %s"
                params.append(marker)
            limit = getattr(query, 'limit', None)
            if limit:
                kw['limit'] = "LIMIT %s"
                params.append(limit)
            offset = getattr(query, 'offset', None)
            if offset:
                kw['offset'] = "OFFSET %s"
                params.append(offset)
        else:
            params = (self.ts_config, convert_query(query))

        if docids is not None:
            docidstr = ','.join(str(docid) for docid in docids)
            kw['filter'] += ' AND docid IN (%s)' % docidstr

        stmt = """
        SELECT docid,
            coefficient * ts_rank_cd(%(weight)stext_vector, query) AS rank
        FROM %(table)s, to_tsquery(%%s, %%s) query
        WHERE %(not)s(text_vector @@ query)
        %(filter)s
        ORDER BY rank DESC
        %(limit)s
        %(offset)s
        """ % kw
        cursor = self.cursor
        cursor.execute(stmt, tuple(params))
        return cursor

    def applyContains(self, query):
        cursor = self._run_query(query)
        data = list(cursor)
        res = self.family.IF.Bucket()
        res.update(data)
        return res

    def applyDoesNotContain(self, query):
        cursor = self._run_query(query, invert=True)
        data = list(cursor)
        res = self.family.IF.Bucket()
        res.update(data)
        return res

    apply = applyEq = applyContains  # @ReservedAssignment
    applyNotEq = applyDoesNotContain

    def docids(self):
        """Return all docids in the index."""
        stmt = "SELECT docid FROM %s" % self.table
        cursor = self.cursor
        cursor.execute(stmt)
        res = self.family.IF.Set()
        for row in cursor:
            res.add(row[0])
        return res

    def get_contextual_summary(self, raw_text, query, **options):
        """BBB: get one contextual summary."""
        return self.get_contextual_summaries([raw_text], query, **options)[0]

    def get_contextual_summaries(self, raw_texts, query, **options):
        """Get a contextual summary for each search result.

        Produces a list of the same length as the raw_texts sequence.
        For each raw_text, returns snippets of text with the words in
        the query highlighted using HTML tags. Calls the
        PostgreSQL function 'ts_headline'. Options are turned into an
        options string passed to 'ts_headline'. See the documentation
        for PostgreSQL for more information on the options that can be
        passed to 'ts_headline'.
        """
        if not raw_texts:
            return []
        s = convert_query(query)
        options = ','.join(['%s=%s' % (k, v) for k, v in options.items()])

        value_clauses = ', '.join(('(%s)',) * len(raw_texts))
        stmt = """
        SELECT ts_headline(%%s, doc.text, to_tsquery(%%s, %%s), %%s)
        FROM (VALUES %s) AS doc (text)
        """ % value_clauses
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
        cursor = self._run_query(query, docids=docids)
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


def _truncate(text):
    """
    PostgreSQL can't handle ts_vectors that are 1MB or larger.
    """
    MAXLEN = 1048575
    l = len(text)
    if l <= MAXLEN:
        return text

    trunc = MAXLEN
    while text[trunc].isalnum():
        trunc -= 1
    return text[:trunc]


class SimpleWeightedText(object):
    implements(IWeightedText)

    def __init__(self, A=None, B=None, C=None, default=''):
        self.A = A
        self.B = B
        self.C = C
        self.default = default

    def __str__(self):
        return self.default
