from pathlib import Path

import pandas as pd

from dstools.pipeline import io


def test_csv_write(tmp_directory):
    df = pd.DataFrame({'a': [0, 1, 2]})

    csv = io.CSVIO('file.csv', chunked=False)
    csv.write(df)

    assert Path('file.csv').is_file()


def test_csv_write_chunks(tmp_directory):
    df = pd.DataFrame({'a': [0, 1, 2]})

    csv = io.CSVIO('files', chunked=True)
    csv.write(df)
    csv.write(df)

    assert Path('files/0.csv').is_file()
    assert Path('files/1.csv').is_file()


def test_parquet_write(tmp_directory):
    df = pd.DataFrame({'a': [0, 1, 2]})

    parquet = io.ParquetIO('file.parquet', chunked=False)
    parquet.write(df)

    assert Path('file.parquet').is_file()


def test_parquet_write_chunks(tmp_directory):
    df = pd.DataFrame({'a': [0, 1, 2]})

    parquet = io.ParquetIO('files', chunked=True)
    parquet.write(df)
    parquet.write(df)

    assert Path('files/0.parquet').is_file()
    assert Path('files/1.parquet').is_file()
