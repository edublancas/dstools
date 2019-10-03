"""
Pickling is needed for parallel processing since objects are serialized
to be sent to other processed
"""
import pickle

from dstools.pipeline.products import File, PostgresRelation
from dstools.pipeline.placeholders import StringPlaceholder, ClientCodePlaceholder
from dstools.templates.StrictTemplate import StrictTemplate


def test_postgres_relation_is_picklable():
    rel = PostgresRelation(('schema', 'name', 'table'))
    pickle.loads(pickle.dumps(rel))


def test_file_is_pickable():
    f = File('/path/to/file.csv')
    pickle.loads(pickle.dumps(f))


def test_string_placeholder_is_picklable():
    p = StringPlaceholder('{{hi}}')
    pickle.loads(pickle.dumps(p))


def test_strict_template_is_pickable():
    t = StrictTemplate('{{hi}}')
    pickle.loads(pickle.dumps(t))
