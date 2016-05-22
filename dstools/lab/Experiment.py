import functools
import inspect
from dstools import FrozenJSON

#not sure if this is a good idea
#http://stackoverflow.com/questions/4214936/how-can-i-get-the-values-of-the-locals-of-a-function-after-it-has-been-executed/4249347#4249347
#http://stackoverflow.com/questions/9186395/python-is-there-a-way-to-get-a-local-function-variable-from-within-a-decorator
#http://code.activestate.com/recipes/577283-decorator-to-expose-local-variables-of-a-function-/


class Experiment:
    def __init__(self, conf, backend='mongo'):
        if backend == 'mongo':
            self.backend = MongoBackend(conf)
        else:
            raise Exception('Backend not supported')

        self.records = []

    def get(self, id_):
        # maybe change this to fetch and instead of returning them
        # putting them in records
        return self.backend.get(id_)

    def save(self):
        '''
            Save all the dictionaries in records. It uses the needs_save flag
            to determine which records are not on the database or have been
            modified and only sends those to the backend.
        '''
        # convert records to dictionaries
        dicts = [dict(r) for r in self.records]
        # first - save records that are new
        new_recs = filter(lambda r: not self.backend.is_on_db(r), dicts)
        return self.backend.save(new_recs)

    def record(self):
        # create and empty record
        record = Record(dict())
        self.records.append(record)
        return record

    def record_with_suffix(self, func, suffix='save'):
        @functools.wraps(func)
        def dec_func(*args, **kwargs):
            func(*args, **kwargs)
            vars_ = locals()
            vars_ = {k: v for k, v in vars_.iteritems() if k.startswith(suffix)}
            print vars_
        return dec_func


class Record(FrozenJSON.FrozenJSON):
    def save(self):
        pass

    def __setitem__(self, key, value):
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

    def is_on_db(self, dict):
        return '_id' in dict
