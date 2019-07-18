import collections


class Params(collections.abc.Mapping):
    """Mapping for representing parameters
    """
    @property
    def first(self):
        return next(iter(self._dict))

    def __init__(self):
        self._dict = collections.OrderedDict()

    def __getitem__(self, key):
        return self._dict[key]

    def __setitem__(self, key, value):
        self._dict[key] = value

    def __iter__(self):
        for name in self._dict.keys():
            yield name

    def __len__(self):
        return len(self._dict)
