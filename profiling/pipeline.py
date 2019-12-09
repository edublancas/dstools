import logging
from pathlib import Path
import time

from dstools.pipeline.tasks import PythonCallable
from dstools.pipeline.products import File
from dstools.pipeline import DAG


logging.basicConfig(level=logging.INFO)


def wait(product):
    time.sleep(1)
    Path(str(product)).touch()


def wait2(product, upstream):
    time.sleep(1)
    Path(str(product)).touch()


dag = DAG()

t1 = PythonCallable(wait, File('t1'), dag, name='t1')
t2 = PythonCallable(wait, File('t2'), dag, name='t2')
t3 = PythonCallable(wait, File('t3'), dag, name='t3')
t4 = PythonCallable(wait2, File('t4'), dag, name='t4')
t5 = PythonCallable(wait2, File('t5'), dag, name='t5')
t6 = PythonCallable(wait2, File('t6'), dag, name='t6')
t7 = PythonCallable(wait2, File('t7'), dag, name='t7')

(t1 + t2 + t3) >> t4 >> (t5 + t6) >> t7

dag.build(force=True)
