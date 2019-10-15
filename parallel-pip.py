"""
Pickling is the main challenge: idea, just pass the task name,
and re-build the dag in the child process, then do dag[name].build()

option two: pickle by passing class name and then initializing the task with
the actual parameters (no placeholders) - this option is better. don't have
to include dag or any other things, just class, code and params, check
actual task.run implementation to see what i need

"""

from multiprocessing import Pool, TimeoutError
import time
import os
from pathos.multiprocessing import ProcessingPool


from dstools.exceptions import RenderError
from dstools.pipeline import DAG
from dstools.pipeline.products import File, PostgresRelation
from dstools.pipeline.tasks import PythonCallable, SQLScript, BashCommand
from dstools.pipeline.tasks.TaskStatus import TaskStatus

import pytest


def fna1(product):
    pass


def fna2(product):
    pass


def fnb(product):
    pass

def fnc(product):
    pass

dag = DAG('dag')

a1 = PythonCallable(fna1, File('a1.txt'), dag, 'a1')
a2 = PythonCallable(fna2, File('a2.txt'), dag, 'a2')
b = PythonCallable(fnb, File('b.txt'), dag, 'b')
c = PythonCallable(fnc, File('c.txt'), dag, 'c')


(a1 + a2) >> b >> c

dag.render()


done = []

def callback(x):
    done.append(x)

def poll():
    global i
    print('polling', i)

    if i < len(t):
        r = t[i]
        i+=1
        print('return ', r)
        return r
    else:
        print('NONEEE')
        return None


from functools import partial

f = partial(a1._code._source, **{k: str(v) for k, v in a1.params.items()})

with Pool(processes=4) as pool:
    pool.apply_async(f, [], callback=callback).get()



with ProcessingPool(processes=4) as pool:
    pool.apipe(a1._code._source, callback=callback).get()
