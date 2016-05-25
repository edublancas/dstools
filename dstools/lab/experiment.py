from itertools import chain
import logging
from dstools.lab import Record, SKRecord

log = logging.getLogger(__name__)


class Experiment:
    _RecordClass = Record

    def __init__(self, conf, backend='mongo', read_only=False):
        # Experiment uses generic Record class
        if backend == 'mongo':
            from dstools.lab.backends import MongoBackend

            self.backend = MongoBackend(conf)
        elif backend == 'tiny':
            from dstools.lab.backends import TinyDBBackend

            self.backend = TinyDBBackend(conf)
        else:
            raise Exception('Backend not supported')

        log.info('Backend selected: {}'.format(self.backend))

        self.records = []
        self.read_only = read_only

    def get(self, **kwargs):
        # backend returns dictionaries
        results = self.backend.get(**kwargs)
        # convert them to records
        results = [self.__class__._RecordClass(r) for r in results]
        for r in results:
            r._is_on_db = True
        self.records.extend(results)

    def __setitem__(self, key, value):
        '''
            Set item to the experiment instance affects all records
        '''
        for r in self.records:
            r[key] = value

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
        record = self.__class__._RecordClass(dict())
        self.records.append(record)
        return record


class SKExperiment(Experiment):
    _RecordClass = SKRecord
