"""
A mapping object with text and HTML representations
"""
from collections.abc import Mapping
from tabulate import tabulate


class Table:
    """

    >>> from dstools.pipeline.Table import Table
    >>> data = {'name': 'task', 'elapsed': 10, 'errors': False}
    >>> table = Table(data)
    >>> table
    """
    def __init__(self, data):
        if isinstance(data, Mapping):
            data = [data]

        self._data = data
        self._repr = tabulate(self._data, headers='keys')
        self._html = tabulate(self._data, headers='keys', tablefmt='html')

    def __str__(self):
        return str(self._data)

    def __repr__(self):
        return self._repr

    def _repr_html_(self):
        return self._html

    def __getitem__(self, key):
        return self._data[key]

    @classmethod
    def from_tables(cls, tables):
        rows = [row for table in tables for row in table]
        return cls(rows)
