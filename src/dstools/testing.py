from psycopg2 import sql


class Postgres:

    @staticmethod
    def no_nas_in_column(col):
        col = sql.Identifier(col)

        def _no_nas_in_column(pg_product):
            id_ = pg_product._identifier
            schema = sql.Identifier(id_.schema)
            name = sql.Identifier(id_.name)

            sql_code = (sql.SQL("""SELECT NOT EXISTS(SELECT * FROM
                                {schema}.{name}
                                WHERE {col} IS NULL LIMIT 1)""")
                        .format(col=col, schema=schema, name=name))

            # FIXME: probably abstract this cursor, execute, try thing...
            conn = pg_product.client.raw_connection()
            cur = conn.cursor()

            try:
                cur.execute(sql_code)
            except Exception as e:
                conn.rollback()
                raise e

            result = cur.fetchone()[0]
            conn.close()
            return result

        return _no_nas_in_column

    @staticmethod
    def distinct_values_in_column(col, values):
        col = sql.Identifier(col)

        def _distinct_values_in_column(pg_product):
            id_ = pg_product._identifier
            schema = sql.Identifier(id_.schema)
            name = sql.Identifier(id_.name)

            sql_code = (sql.SQL("""SELECT DISTINCT {col} FROM
                                {schema}.{name}""")
                        .format(col=col, schema=schema, name=name))

            # FIXME: probably abstract this cursor, execute, try thing...
            conn = pg_product.client.raw_connection()
            cur = conn.cursor()

            try:
                cur.execute(sql_code)
            except Exception as e:
                conn.rollback()
                raise e

            result = [row[0] for row in cur.fetchall()]
            conn.close()
            return set(result) == set(values)

        return _distinct_values_in_column

    @staticmethod
    def no_duplicates_in_column(col):
        col = sql.Identifier(col)

        def _no_duplicates_in_column(pg_product):
            id_ = pg_product._identifier
            schema = sql.Identifier(id_.schema)
            name = sql.Identifier(id_.name)

            sql_code = (sql.SQL("""SELECT NOT EXISTS(
                                    SELECT {col}, COUNT(*)
                                    FROM {schema}.{name}
                                    GROUP BY {col}
                                    HAVING COUNT(*) > 1
                                    LIMIT 1
                                    );
                                """)
                        .format(col=col, schema=schema, name=name))

            # FIXME: probably abstract this cursor, execute, try thing...
            conn = pg_product.client.raw_connection()
            cur = conn.cursor()

            try:
                cur.execute(sql_code)
            except Exception as e:
                conn.rollback()
                raise e

            result = cur.fetchone()[0]
            conn.close()
            return result

        return _no_duplicates_in_column
