from dstools.exceptions import RenderError
from dstools.pipeline import DAG
from dstools.pipeline.products import File, PostgresRelation
from dstools.pipeline.tasks import PythonCallable, SQLScript

import pytest


# TODO: if there is only one product class supported, infer from a tuple?
# TODO: make PostgresRelation accept three parameters instead of a tuple
# TODO: provide a way to manage locations in products, so a relation
# is fulle specified

class Dummy:
    pass


def my_fn(product, upstream):
    pass


def test_task_can_infer_name_from_product():
    dag = DAG()
    t = PythonCallable(my_fn, File('/path/to/{{name}}'), dag,
                       params=dict(name='file'))
    assert t.name == 'file'


def test_task_raises_error_if_name_cannot_be_infered():
    dag = DAG()

    with pytest.raises(RenderError):
        PythonCallable(my_fn, File('/path/to/{{upstream["t1"]}}_2'), dag)


def test_task_can_infer_name_if_product_does_not_depend_on_upstream():
    dag = DAG()
    t1 = PythonCallable(my_fn, File('/path/to/{{name}}'), dag,
                        params=dict(name='file'))
    t2 = PythonCallable(my_fn, File('/path/to/{{name}}'), dag,
                        params=dict(name='file2'))
    assert t1.name == 'file' and t2.name == 'file2'


def test_python_callable_with_file():
    dag = DAG()
    t = PythonCallable(my_fn, File('/path/to/{{name}}'), dag, name='name',
                       params=dict(name='file'))
    t.render()

    assert str(t.product) == '/path/to/file'
    assert str(t._code) == 'def my_fn(product, upstream):\n    pass\n'


def test_postgresscript_with_relation():
    dag = DAG()
    t = SQLScript('CREATE TABLE {{product}} AS SELECT * FROM {{name}}',
                  PostgresRelation(('user', 'table', 'table'),
                                   client=Dummy()),
                  dag,
                  name='name',
                  params=dict(name='some_table'),
                  client=Dummy())

    t.render()

    assert str(t.product) == '"user"."table"'
    assert (str(t._code)
            == 'CREATE TABLE "user"."table" AS SELECT * FROM some_table')
