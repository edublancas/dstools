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
    f = lambda s: re.sub('[^0-9a-zA-Z]+', '_', s).lower()
    names = columns.map(f)
    names = pd.Series(names).map(lambda s: '{}_{}'.format(prefix, s)) if prefix else names
    return names
