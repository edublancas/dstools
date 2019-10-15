from multiprocessing import Pool, TimeoutError
import time
import os
from pathos.multiprocessing import ProcessingPool
from pathlib import Path


from dstools.exceptions import RenderError
from dstools.pipeline import DAG
from dstools.pipeline.products import File, PostgresRelation
from dstools.pipeline.tasks import PythonCallable, SQLScript, BashCommand
from dstools.pipeline.tasks.TaskStatus import TaskStatus

import pytest


def fna1(product):
    Path(str(product)).touch()
    time.sleep(30)


def fna2(product):
    time.sleep(30)
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


done = []
started = []

def callback(task):
    # print('finished', task)
    # task = dag[task.name]
    # task._status = TaskStatus.Executed
    # task._update_status()
    done.append(task)


def next_task():
    # global remaining

    # print('done', done)
    # print('remaining ', remaining)

    if done:
        for task in done:
            # print('updating', name)
            task = dag[task.name]
            task._status = TaskStatus.Executed

            for t in task._get_downstream():
                t._update_status()

    # if not remaining:
        # raise StopIteration


    # for n, t in dag._dict.items():
    #     print(n, t, t._status)

    for task_name in dag:
        if (dag[task_name]._status == TaskStatus.WaitingExecution
            and dag[task_name] not in started):
            # print('got ', task_name)
            t = dag[task_name]
            # remaining = remaining - 1
            return t

    if set([t.name for t in done]) == set(dag):
        raise StopIteration


# next_task()

with Pool(processes=4) as pool:
    while True:
        try:
            task = next_task()
        except StopIteration:
            break
        else:
            if task is not None:
                res = pool.apply_async(task.build, [], callback=callback)#.get()
                started.append(task)
                print('started', task.name)
                # time.sleep(3)
            else:
                pass
                # print('waiting...')
