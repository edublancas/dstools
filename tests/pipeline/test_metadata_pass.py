"""
Testing that upstream tasks metadata is available
"""
import subprocess
from pathlib import Path

from dstools.pipeline.dag import DAG
from dstools.pipeline.tasks import BashCommand
from dstools.pipeline.products import File


def test_passing_self_and_up_in_bashcommand(tmp_directory):
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    kwargs = {'stderr': subprocess.PIPE,
              'stdout': subprocess.PIPE,
              'shell': True}

    ta = BashCommand('echo a > {self.product.identifier}', File(fa), dag,
                     'ta', {}, kwargs, False)
    tb = BashCommand('cat {up[ta].product.identifier} > '
                     '{self.product.identifier}'
                     '&& echo b >> {self.product.identifier} ', File(fb), dag,
                     'tb', {}, kwargs, False)
    tc = BashCommand('cat {up[tb].product.identifier} > '
                     '{self.product.identifier} '
                     '&& echo c >> {self.product.identifier}', File(fc), dag,
                     'tc', {}, kwargs, False)

    ta >> tb >> tc

    dag.build()

    assert fc.read_text() == 'a\nb\nc\n'
