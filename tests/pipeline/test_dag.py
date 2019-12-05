from mock import Mock
from pathlib import Path

import pytest

from dstools.pipeline.dag import DAG
from dstools.pipeline.tasks import BashCommand, PythonCallable, SQLDump
from dstools.pipeline.products import File


# can test this since this uses dag.plot(), which needs dot for plotting
# def test_to_html():
#     def fn1(product):
#         pass

#     def fn2(product):
#         pass

#     dag = DAG()
#     t1 = PythonCallable(fn1, File('file1.txt'), dag)
#     t2 = PythonCallable(fn2, File('file2.txt'), dag)
#     t1 >> t2

#     dag.to_html('a.html')


def test_warn_on_python_missing_docstrings():
    def fn1(product):
        pass

    dag = DAG()
    PythonCallable(fn1, File('file1.txt'), dag)

    with pytest.warns(UserWarning):
        dag.diagnose()


def test_does_not_warn_on_python_docstrings():
    def fn1(product):
        """This is a docstring
        """
        pass

    dag = DAG()
    PythonCallable(fn1, File('file1.txt'), dag)

    with pytest.warns(None) as warn:
        dag.diagnose()

    assert not warn


def test_warn_on_sql_missing_docstrings():
    dag = DAG()

    sql = 'SELECT * FROM table'
    SQLDump(sql, File('file1.txt'), dag, client=Mock())

    with pytest.warns(UserWarning):
        dag.diagnose()


def test_does_not_warn_on_sql_docstrings():
    dag = DAG()

    sql = '/* get data from table */\nSELECT * FROM table'
    SQLDump(sql, File('file1.txt'), dag, client=Mock())

    with pytest.warns(None) as warn:
        dag.diagnose()

    assert not warn


def test_can_access_sub_dag():
    sub_dag = DAG('sub_dag')

    ta = BashCommand('echo "a" > {{product}}', File('a.txt'), sub_dag, 'ta')
    tb = BashCommand('cat {{upstream["ta"]}} > {{product}}',
                     File('b.txt'), sub_dag, 'tb')
    tc = BashCommand('tcat {{upstream["tb"]}} > {{product}}',
                     File('c.txt'), sub_dag, 'tc')

    ta >> tb >> tc

    dag = DAG('dag')

    fd = Path('d.txt')
    td = BashCommand('touch {{product}}', File(fd), dag, 'td')

    td.set_upstream(sub_dag)

    assert 'sub_dag' in td.upstream


def test_can_access_tasks_inside_dag_using_getitem():
    dag = DAG('dag')
    dag2 = DAG('dag2')

    ta = BashCommand('touch {{product}}', File(Path('a.txt')), dag, 'ta')
    tb = BashCommand('touch {{product}}', File(Path('b.txt')), dag, 'tb')
    tc = BashCommand('touch {{product}}', File(Path('c.txt')), dag, 'tc')

    # td is not in the same dag, which is ok, but it still should be
    # discoverable
    td = BashCommand('touch {{product}}', File(Path('c.txt')), dag2, 'td')
    te = BashCommand('touch {{product}}', File(Path('e.txt')), dag2, 'te')

    td >> ta >> tb >> tc >> te

    assert set(dag) == {'ta', 'tb', 'tc'}


def test_partial_build(tmp_directory):
    dag = DAG('dag')

    ta = BashCommand('echo "hi" >> {{product}}',
                     File(Path('a.txt')), dag, 'ta')
    code = 'cat {{upstream.first}} >> {{product}}'
    tb = BashCommand(code, File(Path('b.txt')), dag, 'tb')
    tc = BashCommand(code, File(Path('c.txt')), dag, 'tc')
    td = BashCommand(code, File(Path('d.txt')), dag, 'td')
    te = BashCommand(code, File(Path('e.txt')), dag, 'te')

    ta >> tb >> tc
    tb >> td >> te

    table = dag.build_partially('tc')

    assert {row['name'] for row in table} == {'ta', 'tb'}
    assert all(row['Ran?'] for row in table)
