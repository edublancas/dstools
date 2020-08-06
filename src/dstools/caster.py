from sklearn.base import BaseEstimator, TransformerMixin


class CategoricalCaster(BaseEstimator, TransformerMixin):
    def __init__(self, cols=None):
        self.cols = cols or []
        self.dtypes = {}

    def fit(self, X, y=None):
        for col in self.cols:
            # cast column
            X[col] = X[col].astype('category')
            # save resulting dtype
            self.dtypes[col] = X[col].dtype

        return self

    def transform(self, X):
        for col in self.cols:
            X[col] = X[col].astype(self.dtypes[col])
        return X
