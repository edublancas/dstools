import warnings
from pathlib import Path
from io import StringIO

from dstools.sql import infer
from dstools.pipeline.tasks.Task import Task
from dstools.pipeline.placeholders import (ClientCodePlaceholder,
                                           StringPlaceholder)
from dstools.pipeline.products import File, PostgresRelation, SQLiteRelation
from dstools.pipeline import io

import pandas as pd


class SQLInputTask(Task):
    """Tasks whose code is SQL code
    """

    @property
    def language(self):
        return 'sql'


class SQLScript(SQLInputTask):
    """
    A tasks represented by a SQL script run agains a database this Task
    does not make any assumptions about the underlying SQL engine, it should
    work witn all DBs supported by SQLAlchemy
    """
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation, File)

    def __init__(self, source, product, dag, name=None, client=None,
                 params=None):
        params = params or {}
        super().__init__(source, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a client'
                             .format(type(self).__name__))

    def _validate(self):
        infered_relations = infer.created_relations(self.source_code)

        if not infered_relations:
            warnings.warn('It seems like your task "{task}" will not create '
                          'any tables or views but the task has product '
                          '"{product}"'
                          .format(task=self.name,
                                  product=self.product))
        # FIXME: check when product is metaproduct
        elif len(infered_relations) > 1:
            warnings.warn('It seems like your task "{task}" will create '
                          'more than one table or view but you only declared '
                          ' one product: "{self.product}"'
                          .format(sk=self.name,
                                  product=self.product))
        else:
            schema, name, kind = infered_relations[0]
            id_ = self.product._identifier

            if ((schema != id_.schema) or (name != id_.name)
                    or (kind != id_.kind)):
                warnings.warn('It seems like your task "{task}" create '
                              'a {kind} "{schema}.{name}" but your product '
                              'did not match: "{product}"'
                              .format(task=self.name, kind=kind, schema=schema,
                                      name=name, product=self.product))

    def run(self):
        if (isinstance(self.product, PostgresRelation)
                or isinstance(self.product, SQLiteRelation)):
            self._validate()

        return self.client.execute(self.source_code)


class SQLDump(SQLInputTask):
    """
    Dumps data from a SQL SELECT statement to parquet files (one per chunk)

    Parameters
    ----------
    source: str
        The SQL query to run in the database
    product: File
        The directory location for the output parquet files
    dag: DAG
        The DAG for this task
    name: str, optional
        Name for this task
    params: dict, optional
        Extra parameters for the task
    chunksize: int, optional
        Size of each chunk, one parquet file will be generated per chunk. If
        None, only one file is created


    Notes
    -----
    The chunksize parameter is set in cursor.arraysize object, this parameter
    can greatly speed up the dump for some databases when the driver uses
    cursors.arraysize as the number of rows to fetch on a single call
    """
    SOURCECLASS = ClientCodePlaceholder
    PRODUCT_CLASSES_ALLOWED = (File, )
    PRODUCT_IN_CODE = False

    def __init__(self, source, product, dag, name=None, client=None,
                 params=None,
                 chunksize=10000, io_handler=None):
        params = params or {}
        super().__init__(source, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))
        self.chunksize = chunksize
        self.io_handler = io_handler or io.CSVIO

        if self.client is None:
            raise ValueError('{} must be initialized with a client'
                             .format(type(self).__name__))

    def run(self):
        source_code = str(self.source)
        path = Path(str(self.params['product']))
        handler = self.io_handler(path, chunked=bool(self.chunksize))

        self._logger.debug('Code: %s', source_code)

        cursor = self.client.connection.cursor()
        cursor.execute(source_code)

        if self.chunksize:
            i = 1
            headers = None
            cursor.arraysize = self.chunksize

            while True:
                self._logger.info('Fetching chunk {}...'.format(i))
                data = cursor.fetchmany()
                self._logger.info('Fetched chunk {}'.format(i))

                if i == 1:
                    headers = [c[0] for c in cursor.description]

                if not data:
                    break

                handler.write(data, headers)

                i = i + 1
        else:
            data = cursor.fetchall()
            headers = [c[0] for c in cursor.description]
            handler.write(data, headers)

        cursor.close()

# FIXME: this can be a lot faster for clients that transfer chunksize
# rows over the network


class SQLTransfer(SQLInputTask):
    """Transfers data from a SQL statement to a SQL relation
    """
    SOURCECLASS = ClientCodePlaceholder
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation)
    PRODUCT_IN_CODE = False

    def __init__(self, source, product, dag, name=None, client=None,
                 params=None, chunksize=10000):
        params = params or {}
        super().__init__(source, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

        self.chunksize = chunksize

    def run(self):
        source_code = str(self.source)
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

    Parameters
    ----------
    source: str or pathlib.Path
        Path to parquet file to upload
    """
    SOURCECLASS = StringPlaceholder
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation)
    PRODUCT_IN_CODE = False

    def __init__(self, source, product, dag, name=None, client=None,
                 params=None):
        params = params or {}
        super().__init__(source, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

    def run(self):
        product = self.params['product']

        self._logger.info('Reading data...')
        df = pd.read_parquet(str(self.source))
        self._logger.info('Done reading data...')

        df.to_sql(name=product.name,
                  con=product.client.engine,
                  schema=product.schema,
                  if_exists='replace',
                  index=False)


class PostgresCopy(Task):
    """Efficiently copy data to a postgres database using COPY (better
    alternative to SQLUpload for postgres)

    Parameters
    ----------
    source: str or pathlib.Path
        Path to parquet file to upload
    """
    SOURCECLASS = StringPlaceholder
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation,)
    PRODUCT_IN_CODE = False

    def __init__(self, source, product, dag, name=None, client=None,
                 params=None, sep='\t', null='\\N', columns=None):
        params = params or {}
        super().__init__(source, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

        self.sep = sep
        self.null = null
        self.columns = columns

    def run(self):
        product = self.params['product']
        df = pd.read_parquet(str(self.source))

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
        cur = self.client.connection.cursor()

        self._logger.info('Copying data...')
        cur.copy_from(f,
                      table=str(product),
                      sep='\t',
                      null='\\N')

        f.close()
        cur.close()
