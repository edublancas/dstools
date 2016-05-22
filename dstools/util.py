import os
import yaml


def _can_iterate(obj):
    import types
    import collections

    is_string = isinstance(obj, types.StringTypes)
    is_iterable = isinstance(obj, collections.Iterable)

    return is_iterable and not is_string


def format_column_names(columns, prefix=None):
    import re
    import pandas as pd

    # Get rid of non alphanumeric characters and capital letters
    def format_str(s):
        re.sub('[^0-9a-zA-Z]+', '_', s).lower()

    names = columns.map(format_str)

    if prefix:
        names = pd.Series(names).map(lambda s: '{}_{}'.format(prefix, s))

    return names


def load_yaml(name):
    folder = os.environ['ROOT_FOLDER']
    path = "%s/%s" % (folder, name)
    with open(path, 'r') as f:
        text = f.read()
    dic = yaml.load(text)
    return dic

config = load_yaml('config.yaml')

pg_uri = ('{dialect}://{user}:{password}@{host}:5432/{database}'
          .format(**config['db']))
