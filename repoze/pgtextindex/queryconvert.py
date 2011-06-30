
from repoze.pgtextindex.queryparser import QueryParser
from repoze.pgtextindex.queryparser import remove_special_chars


def convert_query(query):
    """Convert a Zope text index query to PostgreSQL tsearch format"""
    if isinstance(query, dict):
        text = query['query']
    else:
        text = query
    tree = QueryParser().parseQuery(text)
    return ParseTreeEncoder().encode(tree)


class ParseTreeEncoder:

    def encode(self, node):
        m = getattr(self, 'encode_%s' % node.nodeType())
        return m(node)

    def encode_NOT(self, node):
        return '! ( %s )' % self.encode(node.getValue())

    def encode_AND(self, node):
        children = node.getValue()
        return ' & '.join('( %s )' % self.encode(c) for c in children)

    def encode_OR(self, node):
        children = node.getValue()
        return ' | '.join('( %s )' % self.encode(c) for c in children)

    def get_string(self, node):
        value = node.getValue()
        if not isinstance(value, basestring):
            value = ' '.join(value)
        res = remove_special_chars(value)
        res = res.replace('\\', '\\\\').replace("'", "''")
        return res

    def encode_ATOM(self, node):
        return "'%s'" % self.get_string(node)

    def encode_PHRASE(self, node):
        return "'%s'" % self.get_string(node)

    def encode_GLOB(self, node):
        return "'%s':*" % self.get_string(node)
