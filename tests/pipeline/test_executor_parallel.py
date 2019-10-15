from dstools.exceptions import RenderError
from dstools.pipeline import DAG
from dstools.pipeline.products import File, PostgresRelation
from dstools.pipeline.tasks import PythonCallable, SQLScript, BashCommand
from dstools.pipeline.tasks.TaskStatus import TaskStatus

import pytest


def test_parallel_execution():
    dag = DAG('dag')

    a1 = BashCommand('echo "a" > {{product}}', File('a1.txt'), dag, 'a1')
    a2 = BashCommand('echo "a" > {{product}}', File('a1.txt'), dag, 'a2')
    b = BashCommand('cat {{upstream["a1"]}} > {{product}}',
                     File('b.txt'), dag, 'b')
    c = BashCommand('cat {{upstream["b"]}} > {{product}}',
                     File('c.txt'), dag, 'c')

    (a1 + a2) >> b >> c

    dag.render()

    a1.build()
    a2.build()


    for n, t in dag._dict.items():
        print(n, t, t._status)

import time
from pathlib import Path


from dstools.exceptions import RenderError
from dstools.pipeline import DAG
from dstools.pipeline.products import File, PostgresRelation
from dstools.pipeline.tasks import PythonCallable, SQLScript, BashCommand


def fna1(product):
    print('running fna1')
    Path(str(product)).touch()
    time.sleep(3)


def fna2(product):
    print('running fna2')
    time.sleep(3)
    Path(str(product)).touch()


def fnb(upstream, product):
    Path(str(product)).touch()


def fnc(upstream, product):
    Path(str(product)).touch()


dag = DAG('dag')

a1 = PythonCallable(fna1, File('a1.txt'), dag, 'a1')
a2 = PythonCallable(fna2, File('a2.txt'), dag, 'a2')
b = PythonCallable(fnb, File('b.txt'), dag, 'b')
c = PythonCallable(fnc, File('c.txt'), dag, 'c')


(a1 + a2) >> b >> c

dag.render()
