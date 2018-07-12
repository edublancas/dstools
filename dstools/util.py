import os
import yaml
from pydoc import locate
import re


def class_name(obj):
    class_name = str(type(obj))
    class_name = re.search(".*'(.+?)'.*", class_name).group(1)
    return class_name


def instantiate_from_class_string(class_str, kwargs):
    return locate(class_str)(**kwargs)


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


def load_yaml(path):
    '''
        Load yaml file and return the contents of it. If ROOT_FOLDER
        environment variable is defined, the function will load the file
        from ROOT_FOLDER/path else from path
    '''
    try:
        base_path = '{}/'.format(os.environ['ROOT_FOLDER'])
    except:
        base_path = ''

    path = "%s%s" % (base_path, path)
    with open(path, 'r') as f:
        text = f.read()

    return yaml.load(text)


try:
    config = load_yaml('config.yaml')
except Exception, e:
    pass

try:
    db_uri = ('{dialect}://{user}:{password}@{host}:{port}}/{database}'
              .format(**config['db']))
except Exception, e:
    pass
