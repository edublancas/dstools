import sqlite3
import json

from dstools.pipeline.products import Product
from dstools.pipeline.placeholders import SQLRelationPlaceholder


class SQLiteRelation(Product):
    IDENTIFIERCLASS = SQLRelationPlaceholder

    def __init__(self, identifier, conn=None):
        super().__init__(identifier)

        self.conn = conn

        if self.conn is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

        if self.identifier.schema is not None:
            raise ValueError('SQLite does not support schemas, you should '
                             'pass None')

    def _create_metadata_relation(self):

        create_metadata = """
        CREATE TABLE IF NOT EXISTS _metadata (
            name TEXT PRIMARY KEY,
            metadata BLOB
        )
        """

        cur = self.conn.cursor()
        cur.execute(create_metadata)
        self.conn.commit()
        cur.close()

    def fetch_metadata(self):
        self._create_metadata_relation()

        query = """
        SELECT metadata FROM _metadata
        WHERE name = '{name}'
        """.format(name=self.identifier.name)

        cur = self.conn.cursor()
        cur.execute(query)
        records = cur.fetchone()
        cur.close()

        if records:
            metadata_bin = records[0]
            return json.loads(metadata_bin.decode("utf-8"))
        else:
            return None

    def save_metadata(self):
        self._create_metadata_relation()

        metadata_bin = json.dumps(self.metadata).encode('utf-8')

        query = """
            REPLACE INTO _metadata(metadata, name)
            VALUES(?, ?)
        """
        cur = self.conn.cursor()
        cur.execute(query, (sqlite3.Binary(metadata_bin),
                            self.identifier.name))
        self.conn.commit()
        cur.close()

    def exists(self):
        query = """
        SELECT name
        FROM sqlite_master
        WHERE type = '{kind}'
        AND name = '{name}'
        """.format(kind=self.identifier.kind,
                   name=self.identifier.name)

        cur = self.conn.cursor()
        cur.execute(query)
        exists = cur.fetchone() is not None
        cur.close()
        return exists

    def delete(self):
        """Deletes the product
        """
        query = ("DROP {kind} IF EXISTS {relation}"
                 .format(kind=self.identifier.kind,
                         relation=str(self)))
        self.logger.debug('Running "{query}" on the databse...'
                          .format(query=query))
        cur = self.conn.cursor()
        cur.execute(query)
        cur.close()
        self.conn.commit()

    @property
    def name(self):
        return self.identifier.name

    @property
    def schema(self):
        return self.identifier.schema
