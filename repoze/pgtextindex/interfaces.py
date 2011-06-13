
from zope.interface import Attribute
from zope.interface import Interface


class IWeightedText(Interface):
    """An indexable text value with weighted components.

    Applications can return an object that implements this interface
    from the discriminator function attached to a PGTextIndex.

    PostgreSQL supports up to 4 text weights per document, labeled A,
    B, C, and D, where D is the weight applied by default. Applications
    can provide the numeric values of the weights at search time. To
    make use of weights, applications should index fields of the
    document using different weights. For example, a document's title
    could be indexed with the A weight and its description could be
    indexed with the B weight, while the body is indexed with the D
    weight.

    If no weights are assigned at search time, here are the default
    weights assigned by PostgreSQL:

    D = 0.1
    C = 0.2
    B = 0.4
    A = 1.0

    See:
    http://www.postgresql.org/docs/9.0/interactive/textsearch-controls.html
    """

    def __str__():
        """Required: get the default indexable text.

        The text will be assigned the D weight.
        """

    A = Attribute("Optional: text to index with the A weight.")

    B = Attribute("Optional: text to index with the B weight.")

    C = Attribute("Optional: text to index with the C weight.")

    coefficient = Attribute("""Optional: a floating point score multiplier.

    PGTextIndex multiplies each text match score by the coefficient
    after all weighting is computed.

    Use this to influence the document's score in text searches.
    For example, if a document is known to be provided by a reputable
    source, a coefficient of 1.5 would increase its score by 50%. The
    default coefficient is 1.
    """)


class IWeightedQuery(Interface):
    """A text query that controls text weights.

    Note that this interface is less important than IWeightedText.
    Applications should implement IWeightedText first, then only use
    IWeightedQuery for fine tuning the weights.
    """

    text = Attribute("Required: the human-provided query text.")

    A = Attribute("Required: the weight to apply to A text.")

    B = Attribute("Required: the weight to apply to B text.")

    C = Attribute("Required: the weight to apply to C text.")

    D = Attribute("Required: the weight to apply to D (default) text.")
