from pydoc import locate
from dstools.util import hash_sha1_numpy_array


class MetaEstimator:
    def __init__(self, record):
        self._data_sha1_hashes = record._data_sha1_hashes
        model_class = locate(record._model_class)
        params = record._params
        self._skmodel = model_class(**params)

    @property
    def skmodel(self):
        return self._skmodel

    def __getattr__(self, name):
        return getattr(self.skmodel, name)

    def fit(self, X, y, *args, **kwargs):
        x_hash = hash_sha1_numpy_array(X)
        y_hash = hash_sha1_numpy_array(y)

        x_is_equal = (x_hash == self._data_sha1_hashes.X_train_hash)
        y_is_equal = (y_hash == self._data_sha1_hashes.y_train_hash)

        if x_is_equal and y_is_equal:
            self.skmodel.fit(X, y, *args, **kwargs)
            return self
        else:
            raise ValueError('Hashes are not equal. Cannot train.')

    def fit_transform(self, X, y=None, *args, **kwargs):
        print 'Validating data...'
        return self.skmodel.fit_transform(X, y, *args, **kwargs)
