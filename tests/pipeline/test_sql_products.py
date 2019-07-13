from datetime import datetime
import sqlite3
from pathlib import Path

from dstools.pipeline.sql.products import SQLiteRelation

import pandas as pd
import numpy as np


def test_sqlite_product_exists(tmp_directory):
    """

    >>> import tempfile
    >>> tmp_directory = tempfile.mkdtemp()
    """
    tmp = Path(tmp_directory)

    # create a db
    conn = sqlite3.connect(str(tmp / 'database.db'))

    numbers = SQLiteRelation((None, 'numbers', 'table'), conn)
    numbers.render({})

    assert not numbers.exists()

    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', conn)

    assert numbers.exists()


def test_sqlite_product_delete(tmp_directory):
    """
    >>> import tempfile
    >>> tmp_directory = tempfile.mkdtemp()
    """
    tmp = Path(tmp_directory)
    conn = sqlite3.connect(str(tmp / 'database.db'))

    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', conn)

    numbers = SQLiteRelation((None, 'numbers', 'table'), conn)
    numbers.render({})
    numbers.delete()

    assert not numbers.exists()


def test_sqlite_product_fetch_metadata_none_if_not_exists(tmp_directory):
    tmp = Path(tmp_directory)
    conn = sqlite3.connect(str(tmp / 'database.db'))

    numbers = SQLiteRelation((None, 'numbers', 'table'), conn)
    numbers.render({})

    assert numbers.fetch_metadata() is None


def test_sqlite_product_fetch_metadata_none_if_empty_metadata(tmp_directory):
    tmp = Path(tmp_directory)
    conn = sqlite3.connect(str(tmp / 'database.db'))

    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', conn)

    numbers = SQLiteRelation((None, 'numbers', 'table'), conn)
    numbers.render({})

    assert numbers.fetch_metadata() is None


def test_sqlite_product_save_metadata(tmp_directory):
    tmp = Path(tmp_directory)
    conn = sqlite3.connect(str(tmp / 'database.db'))

    numbers = SQLiteRelation((None, 'numbers', 'table'), conn)
    numbers.render({})

    numbers.metadata['timestamp'] = datetime.now().timestamp()
    numbers.metadata['stored_source_code'] = 'some code'

    numbers.save_metadata()

    fetched = numbers.fetch_metadata()

    assert fetched == numbers.metadata
