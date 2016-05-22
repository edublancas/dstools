from collections import Mapping, MutableSequence
import keyword


class FrozenJSON(object):
    """A read-only facade for navigating a JSON-like object
       using attribute notation.
       Based on FrozenJSON from 'Fluent Python'
    """
    def __new__(cls, arg):
        if isinstance(arg, Mapping):
            return super(FrozenJSON, cls).__new__(cls)
        elif isinstance(arg, MutableSequence):
            return [cls(item) for item in arg]
        else:
            return arg

    def __init__(self, mapping):
        self._data = {}
        for key, value in mapping.items():
            if keyword.iskeyword(key):
                key += '_'
            self._data[key] = value

    def __getattr__(self, name):
        if hasattr(self._data, name):
            return getattr(self._data, name)
        else:
            return FrozenJSON(self._data[name])

    def __dir__(self):
        return self._data.keys()

    def __getitem__(self, key):
        return self._data[key]
