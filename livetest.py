
"""Test of concurrent indexing using PGTextIndex"""

from repoze.pgtextindex.index import PGTextIndex
from repoze.pgtextindex.interfaces import IWeightedText
from threading import Thread
from zope.interface import implements
import logging
import thread
import transaction


class Thing(object):
    implements(IWeightedText)

    def __init__(self, docid, title, text):
        self.docid = docid
        self.title = title
        self.text = text

    @property
    def A(self):
        return self.title

    def __str__(self):
        return self.text


def discriminate_thing(thing, default):
    return thing


def main():
    logging.basicConfig()

    dsn = "dbname='pgtxtest' user='pgtxtest' password='pgtxtest'"
    main_index = PGTextIndex(discriminator=discriminate_thing,
                             dsn=dsn,
                             drop_and_create=True)

    # Try to expose a bug in concurrent indexing.
    def reindex_loop():
        index = PGTextIndex(discriminator=discriminate_thing, dsn=dsn)
        for j in range(1000):
            obj10 = Thing(10, "Document", "Document body %d from thread %s"
                          % (j, thread.get_ident()))
            index.index_doc(obj10.docid, obj10)
            transaction.commit()

    threads = []
    for _i in range(8):
        t = Thread(target=reindex_loop)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print main_index.apply('body')


if __name__ == '__main__':
    main()
