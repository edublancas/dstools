import subprocess
from pathlib import Path

from dstools.pipeline.dag import DAG
from dstools.pipeline.tasks import BashCommand
from dstools.pipeline.products import File


def test_can_create_task_with_more_than_one_product(tmp_directory):
    dag = DAG()

    kwargs = {'stderr': subprocess.PIPE,
              'stdout': subprocess.PIPE,
              'shell': True}

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = BashCommand('touch {{product[0]}} {{product[1]}}',
                     (File(fa), File(fb)), dag, 'ta',
                     {}, kwargs, False)
    tc = BashCommand('cat {{upstream["ta"][0]}} {{upstream["ta"][1]}} > {{product}}',
                     File(fc), dag, 'tc',
                     {}, kwargs, False)

    ta >> tc

    dag.render()

    str(ta._code)
    str(tc._code)

    dag.build()
