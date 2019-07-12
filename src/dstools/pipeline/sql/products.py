from dstools.pipeline.products import Product


class SQLiteRelation(Product):
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
        query = """

        SELECT metadata FROM _metadata
        WHERE schema = %(schema)s
        AND relname = %(name)s
        """
        cur = self.conn.cursor()
        cur.execute(query, dict(schema=self.identifier.schema,
                                name=self.identifier.name))
        metadata = cur.fetchone()
        cur.close()

        if metadata is None:
            return None
        else:
            return metadata

    def save_metadata(self):
        pass

    def exists(self):
        query = """
        SELECT name
        FROM sqlite_master
        WHERE type= %(kind)s
        AND name= %(name)s
        """
        cur = self.conn.cursor()
        cur.execute(query, dict(kind=self.identifier.kind,
                                name=self.identifier.name))
        exists = cur.fetchone() is not None
        cur.close()
        return exists

    def delete():
        pass
