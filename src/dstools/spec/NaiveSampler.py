import pandas as pd
import numpy as np


class NaiveSampler:
    """
    A simple sampler for tabular data

    Notes
    -----
    _sample_{type} convert to pd.Series before inserting NAs to avoing having
    to fiddle with the numpy.dtype. Sometimes the array gets created with
    a dtype that does not support NAs. e.g. if generating an array where
    choices are ['a', 'b', 'c'] the array is created as a unicode string
    of length 1 (<U1), when we set np.nan to an array with this dtype nas
    get inserted as 'n', by converting to pd.Series, they get automatically
    converted to 'objects'
    """
    def __init__(self, spec):
        self.spec = spec

    def _na_indexes(self, p, n):
        p_all = [1 - p, p]
        return np.random.choice([False, True], p=p_all, size=n)

    def _insert_nas(self, arr, col):
        nas_idx = self._na_indexes(self.spec[col]['nas_prop'], n=len(arr))
        arr[nas_idx] = np.nan
        return arr

    def _sample_id():
        pass

    def _sample_categorical(self, n, col):
        """
        Sample with equal probability from allowed values and
        randomly insert NAs with the same proportion as in the
        spec
        """
        values = self.spec[col]['values']

        sample = pd.Series(np.random.choice(values, size=n))
        return self._insert_nas(sample, col)

    def _sample_numeric(self, n, col):
        """
        Sample with equal probability from allowed range and
        randomly insert NAs with the same proportion as in the
        spec
        """
        min_, max_ = self.spec[col]['range']
        sample = pd.Series((max_ - min_) * np.random.random_sample(size=n) +
                           min_)
        return self._insert_nas(sample, col)

    def _sample_col(self, n, col):
        sampler = getattr(self, '_sample_' + self.spec[col]['kind'])
        return sampler(n, col)

    def sample(self, n=1):
        """Sample from a spec
        """
        d = {}

        for col in self.spec.keys():
            d[col] = self._sample_col(n, col)

        return pd.DataFrame(d)
