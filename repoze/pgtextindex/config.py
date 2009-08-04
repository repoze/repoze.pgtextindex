
from ZODB.config import BaseConfig
from repoze.pgtextindex.db import PGDatabaseConnector

class PGTextIndexDatabaseFactory(BaseConfig):
    """Open a storage configured via ZConfig"""
    def open(self):
        config = self.config
        return PGDatabaseConnector(
            dsn=config.dsn,
            database_name=config.database_name,
            lock_table=config.lock_table,
            )
