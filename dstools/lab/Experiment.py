from dstools import FrozenJSON
from itertools import chain
from dstools.Backend import MongoBackend


class Experiment:
    def __init__(self, conf, backend='mongo', read_only=False):
        if backend == 'mongo':
            self.backend = MongoBackend(conf)
        else:
            raise Exception('Backend not supported')

        self.records = []
        self.read_only = read_only

    def get(self, **kwargs):
        # backend returns dictionaries
        results = self.backend.get(**kwargs)
        # convert them to records
        results = [Record(r) for r in results]
        for r in results:
            r._is_on_db = True
        self.records.extend(results)

    def save(self):
        '''
            Save all the dictionaries in records. It uses the needs_save flag
            to determine which records are not on the database or have been
            modified and only sends those to the backend.
        '''
        if self.read_only:
            raise Exception('Experiment is on read-only mode')
        # step 1 - save everything that is not in the db
        not_in_db = filter(lambda r: not r._is_on_db, self.records)
        dicts = [dict(r) for r in not_in_db]
        if dicts:
            self.backend.save(dicts)
        # step 2 - update records that have been modified
        need_update = filter(lambda r:  r._is_on_db and r._is_dirty,
                             self.records)
        dicts = [dict(r) for r in need_update]
        if dicts:
            self.backend.update(dicts)
        # update their db status
        for r in chain(not_in_db, need_update):
            r._is_on_db = True
            r._is_dirty = False

    def record(self):
        # create and empty record
        record = Record(dict())
        self.records.append(record)
        return record


class Record(FrozenJSON.FrozenJSON):
    def __init__(self, mapping):
        super(Record, self).__init__(mapping)
        self._is_on_db = False
        self._is_dirty = False

    def __setitem__(self, key, value):
        self._is_dirty = True
        self._data[key] = value
