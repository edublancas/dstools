from dstools.lab import FrozenJSON
from dstools.sklearn import MetaEstimator


class Record(FrozenJSON):
    def __init__(self, mapping):
        super(Record, self).__init__(mapping)
        self._is_on_db = False
        self._is_dirty = False

    def __setitem__(self, key, value):
        self._is_dirty = True
        self._data[key] = value


class SKRecord(Record):
    '''
        Record subclass to provide enhanced capabilities when
        saving scikit-learn models.
    '''
    def __init__(self, mapping):
        super(SKRecord, self).__init__(mapping)

    @property
    def model(self):
        return MetaEstimator(self)
