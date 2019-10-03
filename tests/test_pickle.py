"""
Pickling is needed for parallel processing since objects are serialized
to be sent to other processed
"""
import pickle

from dstools.pipeline.products import File, PostgresRelation
from dstools.pipeline.placeholders import StringPlaceholder
from dstools.templates.StrictTemplate import StrictTemplate

# f = File('/path/to/file.csv')
# pickle.dumps(f)

# rel = PostgresRelation(('schema', 'name', 'table'))
# pickle.dumps(rel)


def test_string_placeholder_is_picklable():
    p = StringPlaceholder('{{hi}}')
    pickle.loads(pickle.dumps(p))


def test_strict_template_is_pickable():
    t = StrictTemplate('{{hi}}')
    pickle.loads(pickle.dumps(t))
