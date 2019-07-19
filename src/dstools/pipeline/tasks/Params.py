import collections


class Params(collections.abc.Mapping):
    """
    Mapping for representing parameters, it's a collections.OrderedDict
    under the hood with an added .pop(key) method (like the one in a regular
    dictionary) and with a .first attribute, that returns the first value,
    useful when there is only one key-value pair
    """
    @property
    def first(self):
        first_key = next(iter(self._dict))
        return self._dict[first_key]

    def pop(self, key):
        return self._dict.pop(key)

    def __init__(self, data=None):
        self._dict = collections.OrderedDict(data or {})

    def __getitem__(self, key):
        return self._dict[key]

    def __setitem__(self, key, value):
        self._dict[key] = value

    def __iter__(self):
        for name in self._dict.keys():
            yield name

    def __len__(self):
        return len(self._dict)
