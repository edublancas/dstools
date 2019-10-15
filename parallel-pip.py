"""
Parallel DAG execution proof of concept

A few notes:
    * Have to test this with other Tasks, especially the ones that use
    clients - have to make sure they are serialized correctly
"""
from multiprocessing import Pool, TimeoutError
import time
from pathlib import Path


from dstools.exceptions import RenderError
from dstools.pipeline import DAG
from dstools.pipeline.products import File, PostgresRelation
from dstools.pipeline.tasks import PythonCallable, SQLScript, BashCommand
from dstools.pipeline.tasks.TaskStatus import TaskStatus


def fna1(product):
    Path(str(product)).touch()
    time.sleep(10)


def fna2(product):
    time.sleep(10)
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
    """Keep track of finished tasks
    """
    done.append(task)


def next_task():
    """
    Return the next Task to execute, returns None if no Tasks are available
    for execution (cause their dependencies are not done yet) and raises
    a StopIteration exception if there are no more tasks to run, which means
    the DAG is done
    """
    if done:
        for task in done:
            task = dag[task.name]
            # TODO: must check for esxecution status - if there is an error
            # is this status updated automatically? cause it will be better
            # for tasks to update their status by themselves, then have a
            # manager to update the other tasks statuses whenever one finishes
            # to know which ones are available for execution
            task._status = TaskStatus.Executed

            # update other tasks status, should abstract this in a execution
            # manager, also make the _get_downstream more efficient by
            # using the networkx data structure directly
            for t in task._get_downstream():
                t._update_status()

    # iterate over tasks to find which is ready for execution
    for task_name in dag:
        # ignore tasks that are already started, I should probably add an
        # executing status but that cannot exist in the task itself,
        # maybe in the manaer?
        if (dag[task_name]._status == TaskStatus.WaitingExecution
           and dag[task_name] not in started):
            t = dag[task_name]
            return t

    # if all tasks are done, stop
    if set([t.name for t in done]) == set(dag):
        raise StopIteration


with Pool(processes=4) as pool:
    while True:
        try:
            task = next_task()
        except StopIteration:
            break
        else:
            if task is not None:
                res = pool.apply_async(task.build, [], callback=callback)
                started.append(task)
                print('started', task.name)
                # time.sleep(3)
