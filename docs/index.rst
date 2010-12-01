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

.. toctree::
   :maxdepth: 2

   narr
   changes

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
* :ref:`glossary`

