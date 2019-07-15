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
import pyarrow as pa
import pyarrow.parquet as pq


def to_parquet(df, path):
    """
    Notes
    -----
    going from pandas.DataFrame to parquet has an intermediate
    apache arrow conversion (since arrow has the actual implementation
    for writing parquet). using the shortcut pandas.DataFrame.to_parquet
    gives this error with timestamps:
    ArrowInvalid: Casting from timestamp[ns] to timestamp[ms] would lose data
    so we are using taking the long way
    """
    table = pa.Table.from_pandas(df)
    return pq.write_table(table, str(path))


class SQLDump(Task):
    """
    Dumps data from a SQL SELECT statement to parquet files (one per chunk)
    """
    CODECLASS = ClientCodePlaceholder
    PRODUCT_CLASSES_ALLOWED = (File, )

    def __init__(self, code, product, dag, name=None, client=None, params=None,
                 chunksize=10000):
        params = params or {}
        super().__init__(code, product, dag, name, params)

        self._logger = logging.getLogger(__name__)

        self.client = client
        self.chunksize = chunksize

        if self.client is None:
            raise ValueError('{} must be initialized with a client'
                             .format(type(self).__name__))

    def run(self):
        source_code = str(self._code)
        chunksize = self.chunksize

        path = Path(str(self.params['product']))

        if path.exists():
            shutil.rmtree(path)

        path.mkdir()

        cursor = self.client.raw_connection().cursor()
        cursor.execute(source_code)

        i = 0
        chunk = True

        while chunk:
            now = datetime.now()
            self._logger.info('Fetching chunk {i}...'.format(i=i))
            chunk = cursor.fetchmany(chunksize)
            elapsed = datetime.now() - now
            self._logger.info('Done fetching chunk, elapsed: {elapsed} '
                              'saving....'.format(elapsed=elapsed))

            if chunk:
                chunk_df = pd.DataFrame.from_records(chunk)
                chunk_df.columns = [row[0] for row in cursor.description]
                to_parquet(chunk_df, path / '{i}.parquet'.format(i=i))
                self._logger.info('Done saving chunk...')
            else:
                self._logger.info('Got empty chunk...')

            i = i + 1


class SQLTransfer(Task):
    """Transfers data from a SQL statement to a SQL relation
    """
    CODECLASS = ClientCodePlaceholder
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation)

    def __init__(self, code, product, dag, name=None, client=None, params=None,
                 chunksize=10000):
        params = params or {}
        super().__init__(code, product, dag, name, params)

        self._logger = logging.getLogger(__name__)

        self.client = client

        if self.client is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

        self.chunksize = chunksize

    def run(self):
        source_code = str(self._code)
        product = self.params['product']

        # read from source_code, use connection from the Task
        self._logger.info('Fetching data...')
        dfs = pd.read_sql_query(source_code, self.client.engine,
                                chunksize=self.chunksize)
        self._logger.info('Done fetching data...')

        for i, df in enumerate(dfs):
            self._logger.info('Storing chunk {i}...'.format(i=i))
            df.to_sql(name=product.name,
                      con=product.client.engine,
                      schema=product.schema,
                      if_exists='replace' if i == 0 else 'append',
                      index=False)
