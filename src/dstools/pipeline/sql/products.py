import json

from dstools.pipeline.products import Product
from dstools.pipeline.placeholders import SQLRelationPlaceholder


class SQLiteRelation(Product):
    IDENTIFIERCLASS = SQLRelationPlaceholder

    def __init__(self, identifier, conn=None):
        self.conn = conn

        if self.conn is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

        super().__init__(identifier)

    def _create_metadata_relation(self):

        create_metadata = """
        CREATE TABLE IF NOT EXISTS _metadata (
            schema TEXT NOT NULL,
            name TEXT NOT NULL,
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
        WHERE schema = '{schema}'
        AND name = '{name}'
        """.format(schema=self.identifier.schema,
                   name=self.identifier.name)

        cur = self.conn.cursor()
        cur.execute(query)
        metadata = cur.fetchone()
        cur.close()

        if metadata is None:
            return None
        else:
            return metadata

    def save_metadata(self):
        metadata_bin = json.dumps(self.metadata).encode('utf-8')
        metadata_bin_str = "{:08b}".format(int(metadata_bin.hex(), 16))

        query = """
            UPDATE _metadata
            SET metadata = {metadata}
            WHERE schema = '{schema}'
            AND name = '{name}'
        """.format(schema=self.identifier.schema,
                   name=self.identifier.name,
                   metadata=metadata_bin_str)

        cur = self.conn.cursor()
        cur.execute(query)
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
        # cascade = 'CASCADE' if force else ''
        cascade = ''
        query = f"DROP {self.identifier.kind} IF EXISTS {self} {cascade}"
        self.logger.debug(f'Running "{query}" on the databse...')
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
