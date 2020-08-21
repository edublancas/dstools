import warnings
from typing import Any
from dataclasses import dataclass
from sklearn.base import BaseEstimator, TransformerMixin


@dataclass
class DtypeMismatch:
    name: str
    dtype: Any
    expected: Any

    def __str__(self):
        return '"{m.name}". Expected {m.expected}, got {m.dtype}'.format(
            m=self)


class ColumnGuard(BaseEstimator, TransformerMixin):
    def __init__(self, strict=True):
        self.strict = strict

    def fit(self, X, y=None):
        self.expected_cols = list(X.columns)
        return self

    def transform(self, X):
        columns_got = list(X.columns)

        if self.strict:
            if self.expected_cols != columns_got:
                missing = set(self.expected_cols) - set(columns_got)
                raise ValueError('Columns during fit were: {}, but got {} '
                                 'for predict.'
                                 ' Missing: {}'.format(self.expected_cols,
                                                       columns_got, missing))
        else:
            missing = set(self.expected_cols) - set(columns_got)
            extra = set(columns_got) - set(self.expected_cols)

            if missing:
                raise ValueError('Missing columns: {}'.format(missing))
            elif extra:
                extra = set(columns_got) - set(self.expected_cols)
                warnings.warn('Got extra columns: {}, ignoring'.format(extra))
                return X[self.expected_cols]

        return X


class InputGuard(BaseEstimator, TransformerMixin):
    """
    Verify column names at predict time match the ones used when fitting. It
    also verifies that dtypes match.

    Parameters
    ----------
    strict : bool, optional
        If True, it will raise an error if the input does not match
        exactly (same columns, same order), if False, it will ignore
        order and extra columns (will only show a warning), defaults
        to True

    Notes
    -----
    Must be used in a Pipeline object and must be the first step. fit
    and predict should be called with a pandas.DataFrame object
    """
    def __init__(self, strict=True):
        self.strict = strict

    def fit(self, X, y=None):
        self.dtypes_expected = X.dtypes.to_dict()
        self.expected_cols = list(X.columns)
        return self

    def transform(self, X):
        columns_got = list(X.columns)

        if self.strict:
            if self.expected_cols != columns_got:
                missing = set(self.expected_cols) - set(columns_got)
                raise ValueError('Columns during fit were: {}, but got {} '
                                 'for predict.'
                                 ' Missing: {}'.format(self.expected_cols,
                                                       columns_got, missing))
        else:
            missing = set(self.expected_cols) - set(columns_got)
            extra = set(columns_got) - set(self.expected_cols)

            if missing:
                raise ValueError('Missing columns: {}'.format(missing))
            elif extra:
                extra = set(columns_got) - set(self.expected_cols)
                warnings.warn('Got extra columns: {}, ignoring'.format(extra))
                return X[self.expected_cols]

        dtypes = X.dtypes.to_dict()

        dtypes_mismatch = [
            str(DtypeMismatch(col, dtype, self.dtypes_expected[col]))
            for col, dtype in dtypes.items()
            if dtype != self.dtypes_expected[col]
        ]

        if dtypes_mismatch:
            raise ValueError('Some dtypes do not match:\n - {}'.format(
                '\n'.join(dtypes_mismatch)))

        return X
