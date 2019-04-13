import json
from dstools.pipeline.products import Product
from dstools.pipeline.tasks import Task

from psycopg2 import sql


class PostgresRelation(Product):
    def __init__(self, identifier, conn):
        self._conn = conn
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
        cur = self._conn.cursor()
        cur.execute(query, dict(schema=self.identifier.schema,
                                name=self.identifier.name))
        metadata = cur.fetchone()
        cur.close()

        # no metadata saved
        if metadata is None:
            return None
        else:
            return json.loads(metadata[0])

        # TODO: also check if metadata  does not give any parsing errors,
        # if yes, also return a dict with None values, and maybe emit a warn

        return metadata

    def save_metadata(self):
        metadata = json.dumps(self.metadata)

        schema = sql.Identifier(self.identifier.schema)
        name = sql.Identifier(self.identifier.name)

        if self.identifier.kind == PostgresIdentifierTable.kind:
            query = (sql.SQL("COMMENT ON TABLE {}.{} IS %(metadata)s;")
                     .format(schema, name))
        else:
            query = (sql.SQL("COMMENT ON VIEW {}.{} IS %(metadata)s;")
                     .format(schema, name))

        cur = self._conn.cursor()
        cur.execute(query, dict(metadata=metadata))
        self._conn.commit()
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

        cur = self._conn.cursor()
        cur.execute(query, dict(schema=self.identifier.schema,
                                name=self.identifier.name))
        exists = cur.fetchone()[0]
        cur.close()
        return exists


class PostgresIdentifier:

    def __init__(self, schema, name):
        self.schema = schema
        self.name = name


class PostgresIdentifierTable(PostgresIdentifier):
    kind = 'table'


class PostgresIdentifierView(PostgresIdentifier):
    kind = 'view'


class PostgresScript(Task):
    """A tasks represented by a SQL script run agains a Postgres database
    """
    def __init__(self, source_code, product, conn):
        super().__init__(source_code, product)
        self._conn = conn

    def run(self):
        cursor = self._conn.cursor()
        cursor.execute(self.source_code)
        self._conn.commit()

    def __repr__(self):
        return f'{type(self).__name__}: {self.path_to_source_code}'
