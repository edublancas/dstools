from typing import List, Union, Mapping
from sklearn.base import BaseEstimator, TransformerMixin


class CategoricalCaster(BaseEstimator, TransformerMixin):
    """
    Transform selected cols to category, the rest are casted to float

    Parameters
    ----------
    cols
        Columns to cast to categorical
    predefined_dtypes:
        Exact type to map columns to, otherwise just learn the type using
        .astype('category')

    cast_numeric_to_float
        Automatically cast int columns to float

    """
    def __init__(self,
                 cols: Union[str, List[str]] = None,
                 strict: bool = True,
                 predefined_dtypes: Mapping = None,
                 cast_int_to_float: bool = True):
        self.strict = strict
        self.cols = cols or []
        self.dtypes = {}
        self.predefined_dtypes = predefined_dtypes or {}
        self.cast_int_to_float = cast_int_to_float

    def fit(self, X, y=None):
        for col in self.cols:

            if col in X:
                # cast column
                if col in self.predefined_dtypes:
                    X[col] = X[col].astype(self.predefined_dtypes[col])
                else:
                    X[col] = X[col].astype('category')

                # save resulting dtype
                self.dtypes[col] = X[col].dtype
            elif self.strict:
                raise ValueError('{} was initialized with column "{}", but '
                                 'it does not appear in the data'.format(
                                     type(self).__name__, col))

        self._cast_to_float(X)

        return self

    def transform(self, X):
        for col in self.cols:
            if col in X:
                X[col] = X[col].astype(self.dtypes[col])
            elif self.strict:
                raise ValueError('{} was initialized with column "{}", but '
                                 'it does not appear in the data'.format(
                                     type(self).__name__, col))

        self._cast_to_float(X)

        return X

    def _cast_to_float(self, X):
        for col in X:
            # NOTE: X[col].dtype == 'int' works on linux but not on windows
            is_int = str(X[col].dtype).startswith('int')

            if (col not in self.cols and is_int and self.cast_int_to_float):
                X[col] = X[col].astype('float')
