from sklearn.base import BaseEstimator, TransformerMixin


class CategoricalCaster(BaseEstimator, TransformerMixin):
    def __init__(self, cols=None, strict=True):
        self.strict = strict
        self.cols = cols or []
        self.dtypes = {}

    def fit(self, X, y=None):
        for col in self.cols:

            if col in X:
                # cast column
                X[col] = X[col].astype('category')
                # save resulting dtype
                self.dtypes[col] = X[col].dtype
            elif self.strict:
                raise ValueError('{} was initialized with column "{}", but '
                                 'it does not appear in the data'.format(
                                     type(self).__name__, col))

        return self

    def transform(self, X):
        for col in self.cols:
            X[col] = X[col].astype(self.dtypes[col])

        return X
