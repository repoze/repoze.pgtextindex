==================
repoze.pgtextindex
==================

``repoze.pgtextindex`` is an indexing plugin for ``repoze.catalog``
that provides a text search engine based on the powerful text indexing
capabilities of PostgreSQL 8.4 and above. It is designed to take the
place of any text search index based on ``zope.index``. Installation
typically requires few or no changes to code that already uses
``repoze.catalog``.

The advantages of ``repoze.pgtextindex`` over ``zope.index.text``
include:

    - Performance. For large datasets, ``repoze.pgtextindex`` can be
      orders of magnitude faster than ``zope.index``, mainly because
      ``repoze.pgtextindex`` does not have the overhead of unpickling
      objects that ``zope.index`` has.

    - Lower RAM consumption. Users of ``zope.index`` work around the
      unpickling overhead by keeping large caches of unpickled objects
      in RAM. Even worse, each thread keeps its own copy of the object
      cache. PostgreSQL, on the other hand, does not need to maintain
      complex structures in RAM. The PostgreSQL process size tends to
      be constant and reasonable.

    - Maintenance. The text indexing features of PostgreSQL are well
      documented and receive a great deal of active maintenance, while
      ``zope.index`` has not received much developer attention for
      years.

``repoze.pgtextindex`` does not cause PostgreSQL to be involved in
every catalog query and update. Only operations that use or change the
text index hit PostgreSQL.

Usage
-----

``repoze.pgtextindex`` is used just like any other index in
``repoze.catalog``::

    from repoze.pgtextindex import PGTextIndex

    index = PGTextIndex(
        discriminator,
        dsn,
        table='pgtextindex',
        ts_config='english',
        drop_and_create=False,
        maxlen=1048575)

The arguments to the constructor are as follows:

``discriminator``
        The ``repoze.catalog`` discriminator for this index.  For more
        information on discriminators see the `repoze.catalog documentation`_.
        This argument is required.

``dsn``
        The connection string for connecting to PostgreSQL.  This argument is
        required.

``table``
        The table to use for the index.  The default is 'pgtextindex'.

``ts_config``
        The PostgreSQL text search configuration to use for the index.  The
        default is 'english' which is the default built in configuration which
        ships with PostgreSQL.  For more information on text search
        configuration, see the `PostgreSQL full text search documentation`_.

``drop_and_create``
        If `True` the table and index used will dropped (if it exists) and
        (re)created.  The default is `False`.

``maxlen``
        The maximum number of characters to index per document.  The default is
        1048575 (2**20 - 1), which is the maximum allowed by the to_tsvector
        function.  Reduce this to improve query speed, since the
        ts_rank_cd function retrieves and decompresses entire TOAST tuples
        when querying.

.. _`repoze.catalog documentation`: http://docs.repoze.org/catalog/

.. _`PostgreSQL full text search documentation`: http://www.postgresql.org/docs/9.0/static/textsearch.html
