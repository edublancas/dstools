"""
Handling file I/O
"""
from pathlib import Path
import shutil

import pyarrow as pa
import pyarrow.parquet as pq


def safe_remove(path):
    if path.exists():
        if path.is_file():
            path.unlink()
        else:
            shutil.rmtree(path)


class FileIO:

    def __init__(self, path, chunked):
        self.path = Path(path)
        self.chunked = chunked

        # if the file (or if a dir) exists, remove it...
        if self.path.exists():
            safe_remove(self.path)
        # if not...
        else:
            # if parent is currently a file, delete
            if self.path.parent.is_file():
                self.path.parent.unlink()

            # make sure parent exists
            self.path.parent.mkdir(parents=True, exist_ok=True)

        # if this is a chunked file, make sure the file that holds all the
        # chunks exists
        if self.chunked:
            self.path.mkdir()

        # only used when chunked
        self.i = 0

    def write(self, df):
        if self.chunked:
            path = self.path / '{i}.{ext}'.format(i=self.i, ext=self.extension)
            self.write_in_path(str(path), df)
            self.i = self.i + 1
        else:
            self.write_in_path(str(self.path), df)

    @classmethod
    def write_in_path(cls, path, df):
        raise NotImplementedError

    @property
    def extension(self):
        raise NotImplementedError


class ParquetIO(FileIO):
    """parquet handler

    Notes
    -----

    going from pandas.DataFrame to parquet has an intermediate
    apache arrow conversion (since arrow has the actual implementation
    for writing parquet). pandas provides the pandas.DataFrame.to_parquet
    to do this 2-step process but it gives errors with timestamps:
    ArrowInvalid: Casting from timestamp[ns] to timestamp[ms] would lose data

    This function uses the pyarrow package directly to save to parquet
    """

    def __init__(self, path, chunked):
        super().__init__(path, chunked)
        self.schema = None

    def write(self, df):
        if self.chunked:
            path = self.path / '{i}.{ext}'.format(i=self.i, ext=self.extension)
            schema = self.write_in_path(str(path), df, self.schema)
            self.i = self.i + 1

            if self.i == 0:
                self.schema = schema

                self._logger.info('Got first chunk, to avoid schemas '
                                  'incompatibility, the schema from this chunk '
                                  'will be applied to the other chunks, verify '
                                  'that this is correct: %s. Columns might be '
                                  'incorrectly detected as "null" if all values'
                                  ' from the first chunk are empty, in such '
                                  'case the only safe way to dump is in one '
                                  'chunk (by setting chunksize to None)',
                                  schema)
        else:
            self.write_in_path(str(self.path), df, schema=None)

    @classmethod
    def write_in_path(cls, path, df, schema):
        # keeping the index causes a "KeyError: '__index_level_0__'" error,
        # so remove it
        table = pa.Table.from_pandas(df,
                                     schema=schema,
                                     preserve_index=False)
        pq.write_table(table, str(path))

        return table.schema

    @property
    def extension(self):
        return 'parquet'


class CSVIO(FileIO):

    @classmethod
    def write_in_path(cls, path, df):
        raise df.to_csv(path, index=False)

    @property
    def extension(self):
        raise 'csv'
