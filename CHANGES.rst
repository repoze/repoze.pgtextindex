1.4 (2015-06-20)
================

- WeightedQueries can now be used as query result caches, making it
  possible to search the catalog many times while hitting the text
  index only once.

- When a query generates a large number of results, pgtextindex now disables
  the expensive text ranking for that query.  The max_ranked attribute
  controls the threshold for disabling ranking.  The default max_ranked
  value is 6000.

- Improved speed by using BTrees instead of Buckets and by using
  cursor.fetchall() instead of iter(cursor).


1.3 (2014-09-03)
================

- Changed the 'marker' column to an array and changed the 'marker' attribute of
  'IWeightedQuery' to accept either a single marker string or a sequence of
  marker strings. Since the database schema has changed, 
  'PGTextIndex.upgrade()' will need to be run on any indexes created with an 
  older version of the code. (LP #1353483)


1.2 (2012-10-09)
================

- Improved query speed by about 10% by duplicating the query parameter
  rather than joining with the query.

- Added the maxlen option to allow a configurable document size limit.


1.1 (2012-09-06)
================

- Handle concurrent index updates cleanly.


1.0 (2012-09-01)
================

- Retry on IntegrityError to avoid meaningless errors.

- Added metrics using the perfmetrics package.


0.5 (2012-04-27)
================

- Switched to read committed isolation and removed explicit locking.
  The explicit locking was reducing write performance and may have been
  interfering with autovacuum.  This change raises the probability
  of temporary inconsistency, but since this package did not provide
  ACID compliance anyway, developers already need to be prepared for
  temporary inconsistency.


0.4 (2011-11-18)
================

- Truncate text to 1MB per document in order to stay under (silly) limit
  imposed by PostgreSQL.


0.3 (2011-06-30)
================

- Fixed PostgreSQL ProgrammingError when query string contains a backslash
  character.  (LP #798725)

- Added ability to mark content with arbitrary markers which can be used as
  discriminators at query time.  (LP #792334)

- Support searches for words containing an apostrophe.  (LP #801265)


0.2 (2011-06-15)
================

- Reworked the scoring method: added a per-document score coefficient.
  The score coefficient can boost the score of documents known to be
  trustworthy.

- Added the IWeightedText interface.  The discriminator function can
  return an IWeightedText instance to control the weights and
  coefficient.

- Added the IWeightedQuery interface.  Text index queries can
  pass an IWeightedQuery instance to control the weight values.

- Allow persistent objects to be indexed, since the usual objection
  (accidental ZODB references) does not apply.

- Do not drop and create the table by default, making PGTextIndex
  easier to use outside ZODB.

- Added the 'get_contextual_summaries' and 'get_contextual_summary'
  methods to the index.

- Compatability with repoze.catalog 0.8.0.


0.1 (2011-01-20)
================

- Initial release.
