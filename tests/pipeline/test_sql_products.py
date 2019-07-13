import sqlite3
from pathlib import Path

from dstools.pipeline import DAG
from dstools.pipeline.sql.tasks import SQLDump, SQLTransfer
from dstools.pipeline.sql.products import SQLiteRelation
from dstools.pipeline.products import File
from dstools.pipeline.postgres import PostgresRelation

import pandas as pd
import numpy as np


def test_sqlite_product(tmp_directory):
    """

    >>> import tempfile
    >>> tmp_directory = tempfile.mkdtemp()
    """
    tmp = Path(tmp_directory)

    # create a db
    conn = sqlite3.connect(str(tmp / 'database.db'))

    numbers = SQLiteRelation(('public', 'numbers', 'table'), conn)
    numbers.render({})
    numbers.exists()

    numbers.fetch_metadata()
    numbers.delete()
