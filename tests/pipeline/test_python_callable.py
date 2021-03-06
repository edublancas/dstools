import pytest
from pathlib import Path
from dstools.pipeline import DAG
from dstools.pipeline.tasks import PythonCallable
from dstools.pipeline.products import File


class MyException(Exception):
    pass


def fn(product, a):
    Path(str(product)).write_text('things')


def fn_w_exception(product):
    raise MyException


def test_params_are_accesible_after_init():
    dag = DAG()
    t = PythonCallable(fn, File('file.txt'), dag, 'callable',
                       params=dict(a=1))
    assert t.params == dict(a=1)


def test_upstream_and_me_are_added():
    dag = DAG()
    t = PythonCallable(fn, File('file.txt'), dag, 'callable',
                       params=dict(a=1))
    dag.render()

    p = t.params.copy()
    p['product'] = str(p['product'])
    assert p == dict(a=1, product='file.txt')


def test_can_execute_python_callable(tmp_directory):
    dag = DAG()
    PythonCallable(fn, File('file.txt'), dag, 'callable',
                   params=dict(a=1))
    assert dag.build()


def test_exceptions_are_raised_with_serial_executor():
    dag = DAG()
    PythonCallable(fn_w_exception, File('file.txt'),
                   dag, 'callable')

    with pytest.raises(MyException):
        dag.build()
