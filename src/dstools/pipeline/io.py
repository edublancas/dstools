"""
Handling file I/O
"""
import csv
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from dstools.pipeline.util import safe_remove


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

    def write(self, data, headers):
        if self.chunked:
            path = self.path / '{i}.{ext}'.format(i=self.i, ext=self.extension)
            self.write_in_path(str(path), data, headers)
            self.i = self.i + 1
        else:
            self.write_in_path(str(self.path), data, headers)

    @classmethod
    def write_in_path(cls, path, data, headers):
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

    def write(self, data, headers):
        if self.chunked:
            path = self.path / '{i}.{ext}'.format(i=self.i, ext=self.extension)
            schema = self.write_in_path(str(path), data, headers, self.schema)
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
            self.write_in_path(str(self.path), data, headers, schema=None)

    @classmethod
    def write_in_path(cls, path, data, headers, schema):
        arrays = [pa.array(col) for col in map(list, zip(*data))]
        table = pa.Table.from_arrays(arrays, names=headers, schema=schema)
        pq.write_table(table, str(path))
        return table.schema

    @property
    def extension(self):
        return 'parquet'


class CSVIO(FileIO):

    @classmethod
    def write_in_path(cls, path, data, headers):
        with open(path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, dialect=csv.unix_dialect)
            writer.writerow(headers)
            writer.writerows(data)

    @property
    def extension(self):
        return 'csv'
