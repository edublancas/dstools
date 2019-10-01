from pathlib import Path

from dstools.pipeline import DAG
from dstools.pipeline.tasks import SQLDump, SQLTransfer
from dstools.pipeline.products import File, SQLiteRelation
from dstools.pipeline.clients import SQLAlchemyClient

import pandas as pd
import numpy as np


def test_can_dump_sqlite(tmp_directory):
    tmp = Path(tmp_directory)

    # create a db
    client = SQLAlchemyClient('sqlite:///{}'.format(tmp / "database.db"))
    # dump output path
    out = tmp / 'dump'

    # make some data and save it in the db
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', client.engine)

    # create the task and run it
    dag = DAG()
    SQLDump('SELECT * FROM numbers -- {{product}}',
            File(out),
            dag,
            name='dump',
            client=client,
            chunksize=10)
    dag.build()

    # load dumped data and data from the db
    dump = pd.read_parquet(out)
    db = pd.read_sql_query('SELECT * FROM numbers', client.engine)

    client.close()

    # make sure they are the same
    assert dump.equals(db)


def test_can_dump_postgres(tmp_directory, pg_client):
    tmp = Path(tmp_directory)

    # dump output path
    out = tmp / 'dump'

    # make some data and save it in the db
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', pg_client.engine, if_exists='replace')

    # create the task and run it
    dag = DAG()
    SQLDump('SELECT * FROM numbers -- {{product}}',
            File(out),
            dag,
            name='dump',
            client=pg_client,
            chunksize=10)
    dag.build()

    # load dumped data and data from the db
    dump = pd.read_parquet(out)
    db = pd.read_sql_query('SELECT * FROM numbers', pg_client.engine)

    # make sure they are the same
    assert dump.equals(db)


def test_can_transfer_sqlite(tmp_directory):
    """
    >>> import tempfile
    >>> tmp_directory = tempfile.mkdtemp()
    """
    tmp = Path(tmp_directory)

    # create clientections to 2 dbs
    client_in = SQLAlchemyClient('sqlite:///{}'.format(tmp / "database_in.db"))
    client_out = SQLAlchemyClient('sqlite:///{}'.format(tmp / "database_out.db"))

    # make some data and save it in the db
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', client_in.engine, index=False)

    # create the task and run it
    dag = DAG()
    SQLTransfer('SELECT * FROM numbers -- {{product}}',
                SQLiteRelation((None, 'numbers2', 'table'), client=client_out),
                dag,
                name='transfer',
                client=client_in,
                chunksize=10)
    dag.build()

    # load dumped data and data from the db
    original = pd.read_sql_query('SELECT * FROM numbers', client_in.engine)
    transfer = pd.read_sql_query('SELECT * FROM numbers2', client_out.engine)

    client_in.close()
    client_out.close()

    # make sure they are the same
    assert original.equals(transfer)
