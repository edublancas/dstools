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
