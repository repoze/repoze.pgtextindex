
import unittest

class TestConvertQuery(unittest.TestCase):

    def _call(self, query):
        from repoze.pgtextindex.queryconvert import convert_query
        return convert_query(query)

    def test_simple(self):
        self.assertEqual(self._call("stuff"), "'stuff'")

    def test_extract_from_dict(self):
        self.assertEqual(self._call({'query': "stuff"}), "'stuff'")

    def test_phrase(self):
        self.assertEqual(self._call('"stuff here"'), "'stuff here'")

    def test_and(self):
        self.assertEqual(self._call('stuff and more'),
            "( 'stuff' ) & ( 'more' )")

    def test_or(self):
        self.assertEqual(self._call('stuff or less'),
            "( 'stuff' ) | ( 'less' )")

    def test_and_not(self):
        self.assertEqual(self._call('stuff and not more'),
            "( 'stuff' ) & ( ! ( 'more' ) )")

    def test_not_without_and(self):
        self.assertEqual(self._call('stuff not more'),
            "( 'stuff' ) & ( ! ( 'more' ) )")

    def test_glob(self):
        self.assertEqual(self._call('stuff*'), "'stuff':*")


def test_suite():
    return unittest.TestSuite((
        unittest.makeSuite(TestConvertQuery),
    ))
