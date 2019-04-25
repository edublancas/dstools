import pickle
from pathlib import Path
import yaml
from pydoc import locate
import re

import collections
from functools import wraps
from inspect import signature, _empty, getargspec
from copy import copy


def isiterable(obj):
    try:
        iter(obj)
    except TypeError:
        return False
    else:
        return True


def _is_iterable(obj):
    """Determine wheter obj is an interable (excluding strings and mappings)
    """
    # FIXME: remove this
    iterable = isinstance(obj, collections.Iterable)
    string = isinstance(obj, str)
    mapping = isinstance(obj, collections.Mapping)
    return iterable and not string and not mapping


def _wrap_in_list_if_needed(obj):
    if _is_iterable(obj):
        return obj
    else:
        return [obj]


def _unwrap_if_single_element(obj):
    if len(obj) == 1:
        return obj[0]
    else:
        return obj


def map_parameters_in_fn_call(args, kwargs, func):
    """
    Based on function signature, parse args to to convert them to key-value
    pairs and merge them with kwargs

    Any parameter found in args that does not match the function signature
    is still passed.

    Missing parameters are filled with their default values
    """
    # Get missing parameters in kwargs to look for them in args
    args_spec = getargspec(func).args
    params_all = set(args_spec)
    params_missing = params_all - set(kwargs.keys())

    # Remove self parameter from params missing since it's not used
    if 'self' in args_spec:
        params_missing.remove('self')
        offset = 1
    else:
        offset = 0

    # Get indexes for those args
    idxs = [args_spec.index(name) for name in params_missing]

    # Parse args
    args_parsed = dict()

    for idx in idxs:
        key = args_spec[idx]

        try:
            value = args[idx-offset]
        except IndexError:
            pass
        else:
            args_parsed[key] = value

    parsed = copy(kwargs)
    parsed.update(args_parsed)

    # fill default values
    default = {k: v.default for k, v
               in signature(func).parameters.items()
               if v.default != _empty}

    to_add = set(default.keys()) - set(parsed.keys())

    default_to_add = {k: v for k, v in default.items() if k in to_add}
    parsed.update(default_to_add)

    return parsed


def ensure_iterator(param):
    """Ensure a certain parameter or parameters are always an iterator,
    unless is None, in that case, it keeps it as it is
    """

    def _ensure_repeated(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            kwargs = map_parameters_in_fn_call(args, kwargs, func)

            params = _wrap_in_list_if_needed(param)

            for p in params:
                if kwargs[p] is not None:
                    kwargs[p] = _wrap_in_list_if_needed(kwargs[p])

            return func(**kwargs)

        return wrapper

    return _ensure_repeated


def ensure_iterator_in_method(param):
    """Ensure a certain parameters is always an iterator, unles is None,
    in that case, it keeps it as it is
    """

    def _ensure_repeated(func):

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            kwargs = map_parameters_in_fn_call(args, kwargs, func)

            if kwargs[param] is not None:
                kwargs[param] = _wrap_in_list_if_needed(kwargs[param])

            return func(self, **kwargs)

        return wrapper

    return _ensure_repeated


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


def save(obj, path):
    path = Path(path)

    if path.suffix == '.npy':
        import numpy as np
        np.save(str(path), obj)

    elif path.suffix == '.yaml':

        with open(str(path), 'w') as f:
            yaml.dump(obj, f)

    elif path.suffix == '.pickle':
        with open(str(path), 'wb') as file:
            pickle.dump(obj, file, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        raise ValueError('Do not know how to save file with extension '
                         '{}'.format(path.suffix))


def load(path):
    path = Path(path)

    if path.suffix == '.npy':
        import numpy as np
        return np.load(str(path))

    elif path.suffix == '.yaml':

        with open(str(path), 'r') as f:
            return yaml.load(f)

    elif path.suffix == '.pickle':
        with open(str(path), 'rb') as file:
            return pickle.load(file, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        raise ValueError('Do not know how to save file with extension '
                         '{}'.format(path.suffix))


# def load_yaml(path):
#     '''
#         Load yaml file and return the contents of it. If ROOT_FOLDER
#         environment variable is defined, the function will load the file
#         from ROOT_FOLDER/path else from path
#     '''
#     try:
#         base_path = '{}/'.format(os.environ['ROOT_FOLDER'])
#     except:
#         base_path = ''

#     path = "%s%s" % (base_path, path)
#     with open(path, 'r') as f:
#         text = f.read()

#     return yaml.load(text)


# try:
#     config = load_yaml('config.yaml')
# except Exception, e:
#     pass

# try:
#     db_uri = ('{dialect}://{user}:{password}@{host}:{port}}/{database}'
#               .format(**config['db']))
# except Exception, e:
#     pass
