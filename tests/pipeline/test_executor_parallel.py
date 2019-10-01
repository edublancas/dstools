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