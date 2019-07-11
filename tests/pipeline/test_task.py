from dstools.pipeline import DAG
from dstools.pipeline.products import File
from dstools.pipeline.tasks import PythonCallable
from dstools.pipeline.postgres import PostgresScript, PostgresRelation


# TODO: if there is only one product class supported, infer from a tuple?
# TODO: make PostgresRelation accept three parameters instead of a tuple
# TODO: provide a way to manage locations in products, so a relation
# is fulle specified

def my_fn(product, upstream):
    pass


def test_python_callable_with_file():
    dag = DAG()
    t = PythonCallable(my_fn, File('/path/to/{{name}}'), dag, name='name',
                       params=dict(name='file'))
    t.render()

    assert str(t.product) == '/path/to/file'
    assert str(t._code) == 'def my_fn(product, upstream):\n    pass\n'


def test_postgresscript_with_relation():
    dag = DAG()
    t = PostgresScript('CREATE TABLE {{product}} AS SELECT * FROM {{name}}',
                       PostgresRelation(('user', 'table', 'table')),
                       dag,
                       name='name',
                       params=dict(name='some_table'))

    t.render()

    assert str(t.product) == '"user"."table"'
    assert (str(t._code)
            == 'CREATE TABLE "user"."table" AS SELECT * FROM some_table')
