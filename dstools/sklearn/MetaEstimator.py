from pydoc import locate


class MetaEstimator:
    def __init__(self, record):
        model_class = locate(record.model_class)
        params = record.parameters
        self._skmodel = model_class(**params)

    @property
    def skmodel(self):
        return self._skmodel

    def __getattr__(self, name):
        return getattr(self.model, name)

    def fit(self, X, y, *args, **kwargs):
        print 'Validating data...'
        return self.model.fit(X, y, *args, **kwargs)

    def fit_transform(self, X, y=None, *args, **kwargs):
        print 'Validating data...'
        return self.model.fit_transform(X, y, *args, **kwargs)
