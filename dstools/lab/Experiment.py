from dstools import FrozenJSON
from dstools.utils import _can_iterate


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
        not_in_db = filter(lambda r: not r._is_on_db, self.records)
        # convert records to dictionaries
        dicts = [dict(r) for r in not_in_db]
        # first - save records that are new
        return self.backend.save(dicts)

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
