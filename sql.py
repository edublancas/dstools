"""
This module provides subclasses of jinja2 for better integration with SQL
"""
import logging
import shutil
from datetime import datetime
from pathlib import Path
import re

from numpydoc.docscrape import NumpyDocString
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import jinja2
from jinja2 import Environment, meta, PackageLoader, FileSystemLoader

# TODO: https://stackoverflow.com/q/3983581


class SQLStore:
    """
    Utility class for loading SQL files from a folder, supports parametrized
    SQL templates (jinja2)

    Examples
    --------
    >>> from tax_estimator.sql import SQLStore
    >>> from dstools import Env
    >>> env = Env()
    >>> path = env.path.home / 'load' / 'sql'
    >>> sqlstore = SQLStore(path)
    """

    def __init__(self, module, path, conn=None):
        if module is None:
            loader = FileSystemLoader(path)
        else:
            loader = PackageLoader(module, path)

        self.env = Environment(
            loader=loader,
            # this will cause jinja2 to raise an exception if a variable
            # declared in the template is not passed in the render parameters
            undefined=jinja2.StrictUndefined)
        self.conn = conn

    def __dir__(self):
        return [t for t in self.env.list_templates() if t.endswith('.sql')]

    def get_template(self, name):
        template = self.env.get_template(name)
        return SQLTemplate(template, self.conn)


class SQLTemplate:
    """
    Wrapper for Jinja2 Template object that provides specific features for
    SQL templating
    """

    def __init__(self, template, conn=None):
        self.template = template
        self.raw = Path(template.filename).read_text()
        self.conn = conn
        self.logger = logging.getLogger(__name__)

        # dynamically set the docstring
        self.__doc__ = self._parse_docstring()

    def _parse_docstring(self):
        """Finds the docstring at the beginning of the template
        """
        # [any whitespace] /* [capture] */ [any string]
        regex = r'^\s*\/\*([\w\W]+)\*\/[\w\W]*'
        return re.match(regex, self.raw).group(1)

    def __str__(self):
        return self.raw

    def diagnose(self):
        """Prints some diagnostics
        """
        env = Environment()
        ast = env.parse(self.template)
        found = meta.find_undeclared_variables(ast)
        docstring_np = NumpyDocString(self.docstring())
        documented = set([p[0] for p in docstring_np['Parameters']])

        print('The following variables were found in the template but are '
              f'not documented: {found - documented}')

        print('The following variables are documented but were not found in '
              f'the template: {documented - found}')

        return documented, found

    def render(self, **kwargs):
        """
        """
        return self.template.render(**kwargs)

    def load(self, **render_kwargs):
        """Load the query to a pandas.DataFrame
        """
        sql = self.render(**render_kwargs)
        return pd.read_sql_query(sql, self.conn)

    def download(self, path, **render_kwargs):
        """Download the query to parquet
        """
        # make sure the parent folder exists
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        df = self.load(**render_kwargs)

        # NOTE: groing from pandas.DataFrame to parquet has an intermediate
        # apache arrow conversion (since arrow has the actual implementation
        # for writing parquet). using the shortcut pandas.DataFrame.to_parquet
        # gives this error with timestamps:
        # ArrowInvalid: Casting from timestamp[ns] to timestamp[ms] would lose data
        # so we are using taking the long way
        table = pa.Table.from_pandas(df)
        return pq.write_table(table, str(path))

    def download_batch(self, path, chunksize=20000, **render_kwargs):
        # NOTE: parquet might be a better option since saving is faster
        # https://stackoverflow.com/a/48097717
        path = Path(path)

        if path.exists():
            shutil.rmtree(path)

        path.mkdir()

        sql = self.render(**render_kwargs)
        cursor = self.conn.cursor()
        cursor.execute(sql)
        # TODO: add option to determine initial i to resume a download,
        # but warn the user that this only works in ordered  queries
        i = 0
        times = []
        chunk = True

        while chunk:
            now = datetime.now()
            self.logger.info(f'Fetching chunk {i}...')
            chunk = cursor.fetchmany(chunksize)
            elapsed = datetime.now() - now
            times.append(elapsed)
            self.logger.info(f'Done fetching chunk, elapsed: {elapsed} '
                             'saving...')

            if chunk:
                chunk_df = pd.DataFrame.from_records(chunk)
                chunk_df.columns = [row[0] for row in cursor.description]
                chunk_df.to_parquet(path / f'{i}.parquet', index=False)
                self.logger.info('Done saving chunk...')
            else:
                self.logger.info('Got empty chunk...')

            i = i + 1
