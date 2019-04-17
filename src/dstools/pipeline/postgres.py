import base64
import json
from dstools.pipeline.products import Product
from dstools.pipeline.tasks import Task

from psycopg2 import sql

CONN = None


class PostgresConnectionMixin:
    def _set_conn(self, conn):
        self._conn = conn

    def _get_conn(self):
        if self._conn is not None:
            return self._conn
        elif CONN is not None:
            return CONN
        else:
            ValueError('You have to either pass a connection object '
                       'in the constructor or set postgres.CONN to '
                       'a connection to be used by all postgres objects')


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


class PostgresRelation(PostgresConnectionMixin, Product):
    def __init__(self, identifier, conn=None,
                 metadata_serializer=Base64Serializer):
        if len(identifier) != 3:
            raise ValueError('identifier must have 3 elements, '
                             f'got: {len(identifier)}')

        self._set_conn(conn)

        # check if a valid conn is available before moving forward
        self._get_conn()

        self.metadata_serializer = metadata_serializer

        super().__init__(PostgresIdentifier(*identifier))

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
        cur = self._get_conn().cursor()
        cur.execute(query, dict(schema=self.identifier.schema,
                                name=self.identifier.name))
        metadata = cur.fetchone()
        cur.close()

        # no metadata saved
        if metadata is None:
            return None
        else:
            return self.metadata_serializer.deserialize(metadata[0])

        # TODO: also check if metadata  does not give any parsing errors,
        # if yes, also return a dict with None values, and maybe emit a warn

    def save_metadata(self):
        metadata = self.metadata_serializer.serialize(self.metadata)

        schema = sql.Identifier(self.identifier.schema)
        name = sql.Identifier(self.identifier.name)

        if self.identifier.kind == PostgresIdentifier.TABLE:
            query = (sql.SQL("COMMENT ON TABLE {}.{} IS %(metadata)s;")
                     .format(schema, name))
        else:
            query = (sql.SQL("COMMENT ON VIEW {}.{} IS %(metadata)s;")
                     .format(schema, name))

        cur = self._get_conn().cursor()
        cur.execute(query, dict(metadata=metadata))
        self._get_conn().commit()
        cur.close()

    def __repr__(self):
        id_ = self.identifier
        return f'PostgresRelation({id_.kind}): {id_.schema}.{id_.name}'

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

        cur = self._get_conn().cursor()
        cur.execute(query, dict(schema=self.identifier.schema,
                                name=self.identifier.name))
        exists = cur.fetchone()[0]
        cur.close()
        return exists


class PostgresIdentifier:
    TABLE = 'table'
    VIEW = 'view'

    def __init__(self, schema, name, kind):
        if kind not in [self.TABLE, self.VIEW]:
            raise ValueError('kind must be one of ["view", "table"] '
                             f'got "{kind}"')

        self.kind = kind
        self.schema = schema
        self.name = name


class PostgresScript(PostgresConnectionMixin, Task):
    """A tasks represented by a SQL script run agains a Postgres database
    """
    def __init__(self, source_code, product, dag, conn=None, name=None):
        super().__init__(source_code, product, dag)
        self._set_conn(conn)

        # check if a valid conn is available before moving forward
        self._get_conn()
        self.name = name

        if self.path_to_source_code is None and self.name is None:
            ValueError('If you pass the code directly (instead of a Path '
                       'object you have to provide a name in the constructor')

    def run(self):
        cursor = self._get_conn().cursor()
        cursor.execute(self.source_code)
        self._get_conn().commit()

    def __repr__(self):
        if self.path_to_source_code is not None:
            return f'{type(self).__name__}: {self.path_to_source_code}'
        else:
            return f'{type(self).__name__}: {self.name}'
