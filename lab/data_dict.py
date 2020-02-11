"""
enhancement proposal:

Functions can decide if they want "product" or not (the other parameters)
are always required. If product is in the function signature nothing happens.

If it is not, then they should provide an extra parameter to PythonCallable,
a serializer function: the user's function is run and then passed to the,
serializer as the first argument. Raise a useful error when the function
returns None.

The idea of adding parameters is to defer logic to the developer: how to
serialize, other parameters are available such as dictionary that returns
a parsed version of the docstring.

The serializer function can also request certain parameters such as the
dictionary. Product is mandatory.
"""
import json
from warnings import warn
from functools import wraps
from inspect import getfullargspec


import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from numpydoc.docscrape import NumpyDocString


def add_metadata(table, metadata, key='my_metadata'):
    """
    Add json metadata to a pyarrow table under key "key"

    Notes
    -----
    Based on: https://stackoverflow.com/a/58978449/709975
    """
    metadata_b = json.dumps(metadata).encode('utf-8')
    key = bytes(key, encoding='utf-8')
    new_schema_metadata = {**{key: metadata_b},
                           **table.schema.metadata}
    table_w_metadata = table.replace_schema_metadata(new_schema_metadata)
    return table_w_metadata


def validate_dictionary(dictionary, table, name):
    """
    Validate dictionary against pandas.DataFrame
    """

    expected = set(p['name'] for p in dictionary['returns'])
    actual = set(table.column_names)

    missing = expected - actual
    extra = actual - expected

    if missing:
        warn('Data dictionary for function "{}" has missing columns: {}'
             .format(name, missing))

    if extra:
        warn('Data dictionary for function "{}" has extra columns: {}'
             .format(name, extra))


def add_dictionary(fn):
    """
    Add dictionary and summary to a function that returns a pyarrow.Table
    """
    # args = getfullargspec(fn).args
    # kwonlyargs = getfullargspec(fn).kwonlyargs
    # args_all = args + kwonlyargs

    # if 'dictionary' not in args_all:
    #     raise TypeError('callable "{}" does not have arg "dictionary"'
    #                     .format(fn.__name__))

    @wraps(fn)
    def wrapper(*args, **kwargs):
        dictionary = docstring2list(fn.__doc__)
        table = fn(*args, **kwargs)
        validate_dictionary(dictionary, table, fn.__name__)
        return add_metadata(table, dictionary, key='my_metadata')

    return wrapper


def docstring2list(doc):
    """Convert numpydoc docstring to a list of dictionaries
    """
    doc = NumpyDocString(doc)
    returns = [{'name': p.name, 'desc': p.desc, 'type': p.type}
               for p in doc['Returns']]
    summary = doc['Summary']
    return {'returns': returns, 'summary': summary}


@add_dictionary
def clean_data():
    """
    Clean the data


    Returns
    -------
    column : int
        Some column
        spanning
        multiple lines
    another_column : float
        Another column
    """
    df = pd.DataFrame({'column': [0, 1], 'extra_column': [0, 1]})
    table = pa.Table.from_pandas(df)
    return table


table = clean_data()
pq.write_table(table, 'table.parquet')

table = pq.read_table('table.parquet')
json.loads(table.schema.metadata[b'my_metadata'].decode('utf-8'))

schema = pq.read_schema('table.parquet')
json.loads(schema.metadata[b'my_metadata'].decode('utf-8'))
