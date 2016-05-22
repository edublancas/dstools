from dstools import FrozenJSON
from dstools.utils import _can_iterate

from itertools import chain

class Experiment:
    def __init__(self, conf, backend='mongo'):
        if backend == 'mongo':
            self.backend = MongoBackend(conf)
        else:
            raise Exception('Backend not supported')

        self.records = []

    def get(self, id_):
        res = self.backend.get(id_)
        if _can_iterate(res):
            for r in res:
                r._is_on_db = True
            self.records.extend(res)
        else:
            res._is_on_db = True
            self.records.append(res)

    def save(self):
        '''
            Save all the dictionaries in records. It uses the needs_save flag
            to determine which records are not on the database or have been
            modified and only sends those to the backend.
        '''
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
        for r in chain(not_in_db and need_update):
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

    def save(self):
        pass

    def __setitem__(self, key, value):
        self._is_dirty = True
        self._data[key] = value


class LoggerBackend:
    def __init__(self, conf):
        self.con = None

    def get(id):
        # fetch model with id or ids if it's a list
        pass

    def get_k(self, subset, key, k, descending=True):
        # get k models by filtering and sorting
        pass

    def store(record):
        # store record or records if it's a list
        pass


from pymongo import MongoClient
from bson.objectid import ObjectId
from dstools.utils import _can_iterate


class MongoBackend:
    def __init__(self, conf):
        client = MongoClient(conf['uri'])
        db = conf['db']
        collection = conf['collection']
        self.con = client[db][collection]

    def get(self, id_):
        # fetch model with id or ids if it's a list
        if _can_iterate(id_):
            id_ = [ObjectId(an_id) for an_id in id_]
            results = list(self.con.find({"_id": {'$in': id_}}))
            return [Record(r) for r in results]
        else:
            return Record(self.con.find_one({"_id": ObjectId(id_)}))

    def save(self, dicts):
        self.con.insert_many(dicts)

    def update(self, dicts):
        for d in dicts:
            self.con.replace_one({'_id': ObjectId(d['_id'])}, d)
