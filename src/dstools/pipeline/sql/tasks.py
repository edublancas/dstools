import logging
from datetime import datetime
import shutil
from pathlib import Path

from dstools.pipeline.tasks.Task import Task
from dstools.pipeline.placeholders import ClientCodePlaceholder
from dstools.pipeline.products import File
from dstools.pipeline.postgres import PostgresRelation
from dstools.pipeline.sql.products import SQLiteRelation

import pandas as pd


class SQLDump(Task):
    """
    Dumps data from a SQL SELECT statement to parquet files (one per chunk)
    """
    CODECLASS = ClientCodePlaceholder
    PRODUCT_CLASSES_ALLOWED = (File, )

    def __init__(self, code, product, dag, name, conn=None, params={},
                 chunksize=10000):
        super().__init__(code, product, dag, name, params)

        self._logger = logging.getLogger(__name__)

        self.conn = conn
        self.chunksize = chunksize

        if self.conn is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

    def run(self):
        source_code = str(self._code)
        chunksize = self.chunksize

        path = Path(str(self.params['product']))

        if path.exists():
            shutil.rmtree(path)

        path.mkdir()

        cursor = self.conn.cursor()
        cursor.execute(source_code)

        i = 0
        chunk = True

        while chunk:
            now = datetime.now()
            self._logger.info(f'Fetching chunk {i}...')
            chunk = cursor.fetchmany(chunksize)
            elapsed = datetime.now() - now
            self._logger.info(f'Done fetching chunk, elapsed: {elapsed} '
                              'saving...')

            if chunk:
                chunk_df = pd.DataFrame.from_records(chunk)
                chunk_df.columns = [row[0] for row in cursor.description]
                chunk_df.to_parquet(path / f'{i}.parquet', index=False)
                self._logger.info('Done saving chunk...')
            else:
                self._logger.info('Got empty chunk...')

            i = i + 1


class SQLTransfer(Task):
    """Transfers data from a SQL statement to a SQL relation
    """
    CODECLASS = ClientCodePlaceholder
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation)

    def __init__(self, code, product, dag, name, conn=None, params={},
                 chunksize=10000):
        super().__init__(code, product, dag, name, params)

        self._logger = logging.getLogger(__name__)

        self.conn = conn

        if self.conn is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

        self.chunksize = chunksize

    def run(self):
        source_code = str(self._code)
        conn = self.conn
        product = self.params['product']

        # read from source_code, use connection from the Task
        dfs = pd.read_sql_query(source_code, conn, chunksize=self.chunksize)

        for i, df in enumerate(dfs):
            # dump to the product object, use product.conn
            df.to_sql(name=product.name,
                      con=product.conn,
                      schema=product.schema,
                      if_exists='replace' if i == 0 else 'append',
                      index=False)
