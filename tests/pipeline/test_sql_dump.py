import sqlite3
from pathlib import Path

from dstools.pipeline import DAG
from dstools.pipeline.sql.tasks import SQLDump
from dstools.pipeline.products import File

import pandas as pd
import numpy as np


def test_can_dump_sql(tmp_directory):
    tmp = Path(tmp_directory)

    # create a db
    conn = sqlite3.connect(str(tmp / 'database.db'))
    # dump output path
    out = tmp / 'dump.parquet'

    # make some data and save it in the db
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', conn)

    # create the task and run it
    dag = DAG()
    SQLDump('SELECT * FROM numbers -- {{product}}', File(out),
            dag, name='dump', conn=conn)
    dag.build()

    # load dumped data and data from the db
    dump = pd.read_parquet(out)
    db = pd.read_sql_query('SELECT * FROM numbers', conn)

    # make sure they are the same
    assert dump.equals(db)
