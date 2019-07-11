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
from jinja2 import Environment, meta, Template


class StrictTemplate:
    """
    A jinja2 Template-like object that adds the following features:

        * template.raw - Returns the raw template used for initialization
        * template.location - Returns a path to the Template object if available
        * strict - will not render if missing or extra parameters
        * docstring parsing

    Note that this does not implement the full jinja2.Template API
    """

    def __init__(self, source, conn=None):

        if isinstance(source, str):
            self.source = Template(source,
                                   undefined=jinja2.StrictUndefined)
            self.raw = source
            self.location = '[Loaded from str]'
        elif isinstance(source, Template):
            location = Path(source.filename)
            if not location.exists():
                raise ValueError('Could not load raw source from '
                                 'jinja2.Template, this usually happens '
                                 'when Templates are initialized directly '
                                 'from a str, only Templates loaded from '
                                 'the filesystem are supported, using a '
                                 'jinja2.Environment will fix this issue, '
                                 'if you want to create a template from '
                                 'a string pass it directly to this '
                                 'constructor')

            self.source = source
            self.raw = location.read_text()
            self.location = location

        self.declared = self._get_declared()

        self.conn = conn
        self.logger = logging.getLogger(__name__)

        # dynamically set the docstring
        self.__doc__ = self._parse_docstring()

    def _parse_docstring(self):
        """Finds the docstring at the beginning of the source
        """
        # [any whitespace] /* [capture] */ [any string]
        regex = r'^\s*\/\*([\w\W]+)\*\/[\w\W]*'
        match = re.match(regex, self.raw)
        return '' if match is None else match.group(1)

    def __str__(self):
        return self.raw

    def __repr__(self):
        return '{}("{}")'.format(type(self).__name__, str(self))

    def _get_declared(self):
        env = Environment()
        ast = env.parse(self.raw)
        declared = meta.find_undeclared_variables(ast)
        return declared

    def diagnose(self):
        """Prints some diagnostics
        """
        found = self.declared
        docstring_np = NumpyDocString(self.docstring())
        documented = set([p[0] for p in docstring_np['Parameters']])

        print('The following variables were found in the template but are '
              f'not documented: {found - documented}')

        print('The following variables are documented but were not found in '
              f'the template: {documented - found}')

        return documented, found

    def render(self, params, optional=None):
        """
        """
        optional = optional or {}
        optional = set(optional)

        passed = set(params.keys())

        missing = self.declared - passed
        extra = passed - self.declared - optional

        if missing:
            raise TypeError('Error rendering template {}, missing required '
                            'arguments: {}, got params {}'
                            .format(repr(self), missing, params))

        if extra:
            raise TypeError(f'Got unexpected arguments {extra}')

        return self.source.render(**params)

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
