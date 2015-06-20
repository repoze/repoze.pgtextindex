
import sys

py3 = sys.version_info.major >= 3

if py3:  # pragma no cover
    basestr = str
    unicd = str
    intrn = sys.intern
    from threading import get_ident

else:  # pragma no cover
    basestr = basestring
    unicd = unicode
    intrn = intern
    from thread import get_ident
