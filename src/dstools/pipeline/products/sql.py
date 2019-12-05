import sqlite3
import json

from psycopg2 import sql

from dstools.pipeline.products import Product
from dstools.pipeline.products.serializers import Base64Serializer
from dstools.templates.Placeholder import SQLRelationPlaceholder


class SQLiteRelation(Product):

    def __init__(self, identifier, client=None):
        super().__init__(identifier)
        self._identifier._schema = None
        self._client = client

    def _init_identifier(self, identifier):
        if identifier[0] is not None:
            raise ValueError('SQLite does not support schemas, you should '
                             'pass None')

        # SQLRelationPlaceholder needs a schema value, we use a dummy value
        # for itniialization
        # FIXME: this is a hacky, refactor SQLRelationPlaceholder
        identifier = ('', identifier[1], identifier[2])
        return SQLRelationPlaceholder(identifier)

    @property
    def client(self):
        if self._client is None:
            default = self.task.dag.clients.get(type(self))

            if default is None:
                raise ValueError('{} must be initialized with a client'
                                 .format(type(self).__name__))
            else:
                self._client = default

        return self._client

    def _create_metadata_relation(self):

        create_metadata = """
        CREATE TABLE IF NOT EXISTS _metadata (
            name TEXT PRIMARY KEY,
            metadata BLOB
        )
        """
        self.client.execute(create_metadata)

    def fetch_metadata(self):
        self._create_metadata_relation()

        query = """
        SELECT metadata FROM _metadata
        WHERE name = '{name}'
        """.format(name=self._identifier.name)

        cur = self.client.connection.cursor()
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
        cur = self.client.connection.cursor()
        cur.execute(query, (sqlite3.Binary(metadata_bin),
                            self._identifier.name))
        self.client.connection.commit()
        cur.close()

    def exists(self):
        query = """
        SELECT name
        FROM sqlite_master
        WHERE type = '{kind}'
        AND name = '{name}'
        """.format(kind=self._identifier.kind,
                   name=self._identifier.name)

        cur = self.client.connection.cursor()
        cur.execute(query)
        exists = cur.fetchone() is not None
        cur.close()
        return exists

    def delete(self):
        """Deletes the product
        """
        query = ("DROP {kind} IF EXISTS {relation}"
                 .format(kind=self._identifier.kind,
                         relation=str(self)))
        self.logger.debug('Running "{query}" on the databse...'
                          .format(query=query))
        self.client.execute(query)

    @property
    def name(self):
        return self._identifier.name

    @property
    def schema(self):
        return self._identifier.schema


class PostgresRelation(Product):
    """A Product that represents a postgres relation (table or view)
    """
    # FIXME: identifier has schema as optional but that introduces ambiguity
    # when fetching metadata and checking if the table exists so maybe it
    # should be required

    def __init__(self, identifier, client=None):
        self._client = client
        super().__init__(identifier)

    def _init_identifier(self, identifier):
        return SQLRelationPlaceholder(identifier)

    @property
    def client(self):
        if self._client is None:
            default = self.task.dag.clients.get(type(self))

            if default is None:
                raise ValueError('{} must be initialized with a client'
                                 .format(type(self).__name__))
            else:
                self._client = default

        return self._client

    def fetch_metadata(self):
        # https://stackoverflow.com/a/11494353/709975
        query = """
        SELECT description
        FROM pg_description
        JOIN pg_class ON pg_description.objoid = pg_class.oid
        JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
        WHERE nspname = %(schema)s
        AND relname = %(name)s
        """
        cur = self.client.connection.cursor()
        cur.execute(query, dict(schema=self._identifier.schema,
                                name=self._identifier.name))
        metadata = cur.fetchone()
        cur.close()

        # no metadata saved
        if metadata is None:
            return None
        else:
            return Base64Serializer.deserialize(metadata[0])

        # TODO: also check if metadata  does not give any parsing errors,
        # if yes, also return a dict with None values, and maybe emit a warn

    def save_metadata(self):
        metadata = Base64Serializer.serialize(self.metadata)

        if self._identifier.kind == 'table':
            query = (sql.SQL("COMMENT ON TABLE {} IS %(metadata)s;"
                             .format(self._identifier)))
        else:
            query = (sql.SQL("COMMENT ON VIEW {} IS %(metadata)s;"
                             .format(self._identifier)))

        cur = self.client.connection.cursor()
        cur.execute(query, dict(metadata=metadata))
        self.client.connection.commit()
        cur.close()

    def exists(self):
        # https://stackoverflow.com/a/24089729/709975
        query = """
        SELECT EXISTS (
            SELECT 1
            FROM   pg_catalog.pg_class c
            JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE  n.nspname = %(schema)s
            AND    c.relname = %(name)s
        );
        """

        cur = self.client.connection.cursor()
        cur.execute(query, dict(schema=self._identifier.schema,
                                name=self._identifier.name))
        exists = cur.fetchone()[0]
        cur.close()
        return exists

    def delete(self, force=False):
        """Deletes the product
        """
        cascade = 'CASCADE' if force else ''
        query = f"DROP {self._identifier.kind} IF EXISTS {self} {cascade}"
        self.logger.debug(f'Running "{query}" on the databse...')

        cur = self.client.connection.cursor()
        cur.execute(query)
        cur.close()
        self.client.connection.commit()

    @property
    def name(self):
        return self._identifier.name

    @property
    def schema(self):
        return self._identifier.schema
