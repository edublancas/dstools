import sqlite3
from pathlib import Path

from dstools.pipeline import DAG
from dstools.pipeline.sql.tasks import SQLDump, SQLTransfer
from dstools.pipeline.products import File
from dstools.pipeline.sql.products import SQLiteRelation

import pandas as pd
import numpy as np


def test_can_dump_sqlite(tmp_directory):
    tmp = Path(tmp_directory)

    # create a db
    conn = sqlite3.connect(str(tmp / 'database.db'))
    # dump output path
    out = tmp / 'dump'

    # make some data and save it in the db
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', conn)

    # create the task and run it
    dag = DAG()
    SQLDump('SELECT * FROM numbers -- {{product}}',
            File(out),
            dag,
            name='dump',
            conn=conn,
            chunksize=10)
    dag.build()

    # load dumped data and data from the db
    dump = pd.read_parquet(out)
    db = pd.read_sql_query('SELECT * FROM numbers', conn)

    conn.close()

    # make sure they are the same
    assert dump.equals(db)


# def test_can_dump_postgres(tmp_directory, open_conn):
#     tmp = Path(tmp_directory)

#     # dump output path
#     out = tmp / 'dump'

#     # make some data and save it in the db
#     df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
#     df.to_sql('numbers', open_conn)

#     # create the task and run it
#     dag = DAG()
#     SQLDump('SELECT * FROM numbers -- {{product}}',
#             File(out),
#             dag,
#             name='dump',
#             conn=open_conn,
#             chunksize=10)
#     dag.build()

#     # load dumped data and data from the db
#     dump = pd.read_parquet(out)
#     db = pd.read_sql_query('SELECT * FROM numbers', open_conn)

#     # make sure they are the same
#     assert dump.equals(db)


def test_can_transfer_sqlite(tmp_directory):
    """
    >>> import tempfile
    >>> tmp_directory = tempfile.mkdtemp()
    """
    tmp = Path(tmp_directory)

    # create connections to 2 dbs
    conn_in = sqlite3.connect(str(tmp / 'database_in.db'))
    conn_out = sqlite3.connect(str(tmp / 'database_out.db'))

    # make some data and save it in the db
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', conn_in, index=False)

    # create the task and run it
    dag = DAG()
    SQLTransfer('SELECT * FROM numbers -- {{product}}',
                SQLiteRelation((None, 'numbers2', 'table'), conn=conn_out),
                dag,
                name='transfer',
                conn=conn_in,
                chunksize=10)
    dag.build()

    # load dumped data and data from the db
    original = pd.read_sql_query('SELECT * FROM numbers', conn_in)
    transfer = pd.read_sql_query('SELECT * FROM numbers2', conn_out)

    conn_in.close()
    conn_out.close()

    # make sure they are the same
    assert original.equals(transfer)
