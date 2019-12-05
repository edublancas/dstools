"""
Pickling is needed for parallel processing since objects are serialized
to be sent to other processed
"""
import pickle

from dstools.pipeline import DAG
from dstools.pipeline.tasks import PythonCallable, BashCommand
from dstools.pipeline.products import File, PostgresRelation
from dstools.pipeline.sources import SQLScriptSource
from dstools.templates.Placeholder import Placeholder


def fn():
    pass


def test_can_pickle_dag():
    dag = DAG()

    t = BashCommand('cat "hi" > {{product}}', File('/tmp/file.txt'), dag,
                    name='bash')

    t2 = PythonCallable(fn, File('/tmp/file2.txt'), dag, name='fn')

    t >> t2

    pickle.loads(pickle.dumps(dag))


def test_postgres_relation_is_picklable():
    rel = PostgresRelation(('schema', 'name', 'table'))
    pickle.loads(pickle.dumps(rel))


def test_file_is_pickable():
    f = File('/path/to/file.csv')
    pickle.loads(pickle.dumps(f))


def test_placeholder_is_picklable():
    p = Placeholder('{{hi}}')
    pickle.loads(pickle.dumps(p))
