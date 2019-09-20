import warnings
import shutil
from pathlib import Path
from io import StringIO

from dstools.sql import infer
from dstools.pipeline.tasks.Task import Task
from dstools.pipeline.placeholders import (ClientCodePlaceholder,
                                           StringPlaceholder)
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
            id_ = self.product._identifier

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


def _to_parquet(df, path, schema=None):
    """Export a pandas.DataFrame to parquet

    Notes
    -----

    going from pandas.DataFrame to parquet has an intermediate
    apache arrow conversion (since arrow has the actual implementation
    for writing parquet). pandas provides the pandas.DataFrame.to_parquet
    to do this 2-step process but it gives errors with timestamps:
    ArrowInvalid: Casting from timestamp[ns] to timestamp[ms] would lose data

    This function uses the pyarrow package directly to save to parquet
    """
    # keeping the index causes a "KeyError: '__index_level_0__'" error,
    # so remove it
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, str(path))
    return table.schema


class SQLDump(Task):
    """
    Dumps data from a SQL SELECT statement to parquet files (one per chunk)
    """
    CODECLASS = ClientCodePlaceholder
    PRODUCT_CLASSES_ALLOWED = (File, )
    PRODUCT_IN_CODE = False

    def __init__(self, code, product, dag, name=None, client=None, params=None,
                 chunksize=10000):
        params = params or {}
        super().__init__(code, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))
        self.chunksize = chunksize

        if self.client is None:
            raise ValueError('{} must be initialized with a client'
                             .format(type(self).__name__))

    def run(self):
        source_code = str(self._code)
        path = Path(str(self.params['product']))

        self._logger.debug('Code: %s', source_code)

        if self.chunksize is None:
            df = pd.read_sql(source_code, self.client.engine,
                             chunksize=self.chunksize)
            self._logger.info('Fetching data...')
            _to_parquet(df, path)
        else:
            if path.exists():
                shutil.rmtree(path)

            path.mkdir()

            self._logger.debug('Fetching first chunk...')

            # during the first chunk, we pass None as schema, so it's inferred
            schema = None

            for i, df in enumerate(pd.read_sql(source_code, self.client.engine,
                                               chunksize=self.chunksize)):

                self._logger.info('Fetched chunk {i}'.format(i=i))

                s = _to_parquet(df, path / '{i}.parquet'.format(i=i), schema)

                # save the inferred schema, following iterations will use this
                # to avoid incompatibility between schemas:
                # https://github.com/dask/dask/issues/4194
                if i == 0:
                    schema = s

                self._logger.info('Fetching chunk {j}...'.format(j=i + 1))


class SQLTransfer(Task):
    """Transfers data from a SQL statement to a SQL relation
    """
    CODECLASS = ClientCodePlaceholder
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation)
    PRODUCT_IN_CODE = False

    def __init__(self, code, product, dag, name=None, client=None, params=None,
                 chunksize=10000):
        params = params or {}
        super().__init__(code, product, dag, name, params)

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


class SQLUpload(Task):
    """Upload data to a database from a parquet file
    """
    CODECLASS = StringPlaceholder
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation)
    PRODUCT_IN_CODE = False

    def __init__(self, code, product, dag, name=None, client=None,
                 params=None):
        params = params or {}
        super().__init__(code, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

    def run(self):
        product = self.params['product']

        self._logger.info('Reading data...')
        df = pd.read_parquet(str(self._code))
        self._logger.info('Done reading data...')

        df.to_sql(name=product.name,
                  con=product.client.engine,
                  schema=product.schema,
                  if_exists='replace',
                  index=False)


class PostgresCopy(Task):
    """Efficiently copy data to a postgres database using COPY
    """
    CODECLASS = StringPlaceholder
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation,)
    PRODUCT_IN_CODE = False

    def __init__(self, code, product, dag, name=None, client=None,
                 params=None, sep='\t', null='\\N', columns=None):
        params = params or {}
        super().__init__(code, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

        self.sep = sep
        self.null = null
        self.columns = columns

    def run(self):
        product = self.params['product']
        df = pd.read_parquet(str(self._code))

        # create the table
        self._logger.info('Creating table...')
        df.head(0).to_sql(name=product.name,
                          con=product.client.engine,
                          schema=product.schema,
                          if_exists='replace',
                          index=False)
        self._logger.info('Done creating table.')

        # if product.kind != 'table':
        #     raise ValueError('COPY is only supportted in tables')

        # create file-like object
        f = StringIO()
        df.to_csv(f, sep='\t', na_rep='\\N', header=False, index=False)
        f.seek(0)

        # upload using copy
        conn = self.client.raw_connection()
        cur = conn.cursor()

        self._logger.info('Copying data...')
        cur.copy_from(f,
                      table=str(product),
                      sep='\t',
                      null='\\N')

        f.close()
        conn.commit()
        conn.close()
