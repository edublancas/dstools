import logging
from datetime import datetime
import shutil
from pathlib import Path

from dstools.pipeline.tasks.Task import Task
from dstools.pipeline.identifiers import ClientCode
from dstools.pipeline.products import File

import pandas as pd


class SQLDump(Task):
    CODECLASS = ClientCode
    PRODUCT_CLASSES_ALLOWED = (File, )

    def __init__(self, code, product, dag, name, conn=None, params={}):
        super().__init__(code, product, dag, name, params)

        self._logger = logging.getLogger(__name__)

        self.conn = conn

        if self.conn is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

    def run(self):
        source_code = str(self._code)
        chunksize = self.params.get('chunksize') or 20000

        # NOTE: parquet might be a better option since saving is faster
        # https://stackoverflow.com/a/48097717
        path = Path(str(self.params['product']))

        if path.exists():
            shutil.rmtree(path)

        path.mkdir()

        cursor = self.conn.cursor()
        cursor.execute(source_code)

        # TODO: add option to determine initial i to resume a download,
        # but warn the user that this only works in ordered  queries
        i = 0
        times = []
        chunk = True

        while chunk:
            now = datetime.now()
            self._logger.info(f'Fetching chunk {i}...')
            chunk = cursor.fetchmany(chunksize)
            elapsed = datetime.now() - now
            times.append(elapsed)
            self._logger.info(f'Done fetching chunk, elapsed: {elapsed} '
                              'saving...')

            if chunk:
                chunk_df = pd.DataFrame.from_records(chunk)
                chunk_df.columns = [row[0] for row in cursor.description]
                chunk_df.to_parquet(path / f'{i}.parquet', index=False)
                self._logger.info('Done saving chunk...')
            else:
                self._logger.info('Got empty chunk...')

            i = i + 1