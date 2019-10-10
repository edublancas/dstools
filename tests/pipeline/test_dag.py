from pathlib import Path

from dstools.pipeline.dag import DAG
from dstools.pipeline.tasks import BashCommand, PythonCallable
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

#     dag.to_html()


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
    td = BashCommand('touch d.txt', File(fd), dag, 'td')

    td.set_upstream(sub_dag)

    assert 'sub_dag' in td.upstream


def test_can_access_tasks_inside_dag_using_getitem():
    dag = DAG('dag')
    dag2 = DAG('dag2')

    ta = BashCommand('touch a.txt', File(Path('a.txt')), dag, 'ta')
    tb = BashCommand('touch b.txt', File(Path('b.txt')), dag, 'tb')
    tc = BashCommand('touch c.txt', File(Path('c.txt')), dag, 'tc')

    # td is not in the same dag, which is ok, but it still should be
    # discoverable
    td = BashCommand('touch d.txt', File(Path('c.txt')), dag2, 'td')
    te = BashCommand('touch e.txt', File(Path('e.txt')), dag2, 'te')

    td >> ta >> tb >> tc >> te

    assert set(dag) == {'ta', 'tb', 'tc'}
