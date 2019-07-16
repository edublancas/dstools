import warnings
import logging
from datetime import datetime
import shutil
from pathlib import Path

from dstools.sql import infer
from dstools.pipeline.tasks.Task import Task
from dstools.pipeline.placeholders import ClientCodePlaceholder
from dstools.pipeline.products import File, PostgresRelation, SQLiteRelation

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class SQLScript(Task):
    """
    A tasks represented by a SQL script run agains a database this Task
    does not make any assumptions about the underlying SQL engine, it should
    work witn all DBs supported by SQLAlchemy
    """
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation)

    def __init__(self, code, product, dag, name=None, client=None,
                 params=None):
        params = params or {}

        super().__init__(code, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a client'
                             .format(type(self).__name__))

    def _validate(self):
        infered_relations = infer.created_relations(self.source_code)

        if not infered_relations:
            warnings.warn('It seems like your task "{self}" will not create '
                          'any tables or views but the task has product '
                          '"{product}"'
                          .format(self=self,
                                  product=self.product))
        # FIXME: check when product is metaproduct
        elif len(infered_relations) > 1:
            warnings.warn('It seems like your task "{self}" will create '
                          'more than one table or view but you only declared '
                          ' one product: "{self.product}"'
                          .format(self=self,
                                  product=self.product))
        else:
            schema, name, kind = infered_relations[0]
            id_ = self.product.identifier

            if ((schema != id_.schema) or (name != id_.name)
                    or (kind != id_.kind)):
                warnings.warn('It seems like your task "{self}" create '
                              'a {kind} "{schema}.{name}" but your product '
                              'did not match: "{product}"'
                              .format(self=self, kind=kind, schema=schema,
                                      name=name, product=self.product))

    def run(self):
        self._validate()
        conn = self.client.raw_connection()
        cur = conn.cursor()
        cur.execute(self.source_code)
        conn.commit()
        conn.close()

        self.product.check()
        self.product.test()


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

        self.client = client or self.dag.clients.get(type(self))
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

        self.client = client or self.dag.clients.get(type(self))

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
