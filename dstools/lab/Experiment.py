from dstools import FrozenJSON
from dstools.util import _can_iterate
from itertools import chain


class Experiment:
    def __init__(self, conf, backend='mongo', read_only=False):
        if backend == 'mongo':
            self.backend = MongoBackend(conf)
        else:
            raise Exception('Backend not supported')

        self.records = []
        self.read_only = read_only

    def get(self, **kwargs):
        res = self.backend.get(**kwargs)
        for r in res:
            r._is_on_db = True
        self.records.extend(res)

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
from dstools.util import _can_iterate


class MongoBackend:
    def __init__(self, conf):
        client = MongoClient(conf['uri'])
        db = conf['db']
        collection = conf['collection']
        self.con = client[db][collection]

    def save(self, dicts):
        self.con.insert_many(dicts)

    def update(self, dicts):
        for d in dicts:
            self.con.replace_one({'_id': ObjectId(d['_id'])}, d)

    def get(self, **kwargs):
        # process ids in case _id is on kwargs
        if '_id' in kwargs:
            value = kwargs['_id']
            if _can_iterate(value):
                kwargs['_id'] = [ObjectId(an_id) for an_id in value]
            else:
                kwargs['_id'] = ObjectId(value)

        def to_mongo(value):
            if _can_iterate(value):
                return {'$in': value}
            else:
                return value

        for k in kwargs:
            kwargs[k] = to_mongo(kwargs[k])

        results = list(self.con.find(kwargs))
        return [Record(r) for r in results]
