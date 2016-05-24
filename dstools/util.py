import os
import yaml


def instantiate_from_class_string(class_str, kwargs):
    from pydoc import locate
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
