"""
Module for using PostgresSQL
"""
import logging
import warnings
import base64
import json
from dstools.pipeline.products import Product
from dstools.pipeline.tasks import Task
from dstools.pipeline.sql import SQLRelationKind
from dstools.pipeline.placeholders import SQLRelationPlaceholder
from dstools.sql import infer

from psycopg2 import sql

CONN = None


class JSONSerializer:
    @staticmethod
    def serialize(metadata):
        return json.dumps(metadata)

    @staticmethod
    def deserialize(metadata_str):
        return json.loads(metadata_str)


class Base64Serializer:
    @staticmethod
    def serialize(metadata):
        json_str = json.dumps(metadata)
        return base64.encodebytes(json_str.encode('utf-8')).decode('utf-8')

    @staticmethod
    def deserialize(metadata_str):
        bytes_ = metadata_str.encode('utf-8')
        metadata = json.loads(base64.decodebytes(bytes_).decode('utf-8'))
        return metadata


class PostgresRelation(Product):
    """A Product that represents a postgres relation (table or view)
    """
    # FIXME: identifier has schema as optional but that introduces ambiguity
    # when fetching metadata and checking if the table exists so maybe it
    # should be required
    IDENTIFIERCLASS = SQLRelationPlaceholder

    def __init__(self, identifier, conn=None):
        self.conn = conn or CONN

        if self.conn is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

        super().__init__(identifier)

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
        cur = self.conn.cursor()
        cur.execute(query, dict(schema=self.identifier.schema,
                                name=self.identifier.name))
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

        if self.identifier.kind == SQLRelationKind.table:
            query = (sql.SQL("COMMENT ON TABLE {} IS %(metadata)s;"
                     .format(self.identifier)))
        else:
            query = (sql.SQL("COMMENT ON VIEW {} IS %(metadata)s;"
                     .format(self.identifier)))

        cur = self.conn.cursor()
        cur.execute(query, dict(metadata=metadata))
        self.conn.commit()
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

        cur = self.conn.cursor()
        cur.execute(query, dict(schema=self.identifier.schema,
                                name=self.identifier.name))
        exists = cur.fetchone()[0]
        cur.close()
        return exists

    def delete(self, force=False):
        """Deletes the product
        """
        cascade = 'CASCADE' if force else ''
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


class PostgresScript(Task):
    """A tasks represented by a SQL script run agains a Postgres database
    """
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, )

    def __init__(self, code, product, dag, name, conn=None, params={}):
        super().__init__(code, product, dag, name, params)

        self.conn = conn or CONN

        if self.conn is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

    def _validate(self):
        infered_relations = infer.created_relations(self.source_code)

        if not infered_relations:
            warnings.warn(f'It seems like your task "{self}" will not create '
                          'any tables or views but the task has product '
                          f'"{self.product}"')
        # FIXME: check when product is metaproduct
        elif len(infered_relations) > 1:
            warnings.warn(f'It seems like your task "{self}" will create '
                          'more than one table or view but you only declared '
                          f' one product: "{self.product}"')
        else:
            schema, name, kind = infered_relations[0]
            id_ = self.product.identifier

            if ((schema != id_.schema) or (name != id_.name)
                    or (kind != id_.kind)):
                warnings.warn(f'It seems like your task "{self}" create '
                              f'a {kind} "{schema}.{name}" but your product '
                              f'did not match: "{self.product}"')

    def run(self):
        self._validate()
        cursor = self.conn.cursor()
        cursor.execute(self.source_code)
        self.conn.commit()

        self.product.check()
        self.product.test()
