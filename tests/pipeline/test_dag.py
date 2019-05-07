from pathlib import Path

from dstools.pipeline.dag import DAG
from dstools.pipeline.tasks import BashCommand
from dstools.pipeline.products import File


def test_can_access_sub_dag():
    sub_dag = DAG('sub_dag')

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = BashCommand('touch a.txt', File(fa), sub_dag, 'ta')
    tb = BashCommand('touch b.txt', File(fb), sub_dag, 'tb')
    tc = BashCommand('touch c.txt', File(fc), sub_dag, 'tc')

    ta >> tb >> tc

    dag = DAG('dag')

    fd = Path('d.txt')
    td = BashCommand('touch d.txt', File(fd), dag, 'td')

    td.set_upstream(sub_dag)

    assert 'sub_dag' in td.upstream_by_name


def test_dag_can_access_tasks_by_name():
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

    # FIXME: i have to come up with a unified api for this
    assert set(dag.tasks_by_name.keys()) == {'ta', 'tb', 'tc'}

    assert set(dag.to_dict().keys()) == {'ta', 'tb', 'tc', 'td'}

    # FIXME: te should also be discoverable
