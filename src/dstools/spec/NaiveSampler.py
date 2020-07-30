import pandas as pd
import numpy as np


class NaiveSampler:
    """
    A simple sampler for tabular data
    """
    def __init__(self, spec):
        self.spec = spec

    def _sample_id():
        pass

    def _sample_categorical(self, n, col):
        """
        Sample with equal probability from allowed values and
        randomly insert NAs with the same proportion as in the
        spec
        """
        values = self.spec[col]['values']
        return np.random.choice(values, size=n)

    def _sample_numeric(self, n, col):
        """
        Sample with equal probability from allowed range and
        randomly insert NAs with the same proportion as in the
        spec
        """
        min_, max_ = self.spec[col]['range']
        return (max_ - min_) * np.random.random_sample(size=n) + min_

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
