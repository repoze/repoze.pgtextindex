
__version__ = '1.2'

import os
from setuptools import setup, find_packages

requires = [
    'setuptools',
    'perfmetrics',
    'psycopg2',
    'repoze.catalog',
    'transaction',
    'ZODB3',
    'zope.index',
    ]

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.txt')).read()
CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()

setup(
    name='repoze.pgtextindex',
    version=__version__,
    description="Text index for repoze.catalog based on PostgreSQL 8.4+",
    long_description=README + '\n\n' +  CHANGES,
    # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Framework :: ZODB",
        "License :: Repoze Public License",
        "Topic :: Database",
    ],
    keywords='repoze catalog ZODB PostgreSQL text index',
    author='Shane Hathaway',
    author_email='shane@hathawaymix.org',
    url='http://pypi.python.org/pypi/repoze.pgtextindex',
    license='BSD-derived (http://www.repoze.org/LICENSE.txt)',
    packages=find_packages(),
    include_package_data=True,
    namespace_packages=['repoze'],
    zip_safe=False,
    install_requires=requires,
    tests_require=requires + ['nose'],
    test_suite="nose.collector",
    entry_points = """
    """,
)
