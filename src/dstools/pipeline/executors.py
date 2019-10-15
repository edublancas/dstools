"""
DAG executors
"""
from multiprocessing import Pool, TimeoutError

import networkx as nx
from tqdm.auto import tqdm
from dstools.pipeline.Table import BuildReport
from dstools.pipeline.constants import TaskStatus


def serial(dag):
    """Runs a DAG serially
    """
    status_all = []

    g = dag._to_graph()
    pbar = tqdm(nx.algorithms.topological_sort(g), total=len(g))

    for t in pbar:
        pbar.set_description('Building task "{}"'.format(t.name))

        try:
            t.build()
        except Exception as e:
            if dag._on_task_failure:
                dag._on_task_failure(t)

            raise e
        else:
            if dag._on_task_finish:
                dag._on_task_finish(t)

        status_all.append(t.build_report)

    build_report = BuildReport(status_all)
    dag._logger.info(' DAG report:\n{}'.format(repr(build_report)))

    for client in dag.clients.values():
        client.close()

    return build_report


def parallel(dag):
    """Runs a DAG in parallel using the multiprocessing module
    """
    # TODO: Have to test this with other Tasks, especially the ones that use
    # clients - have to make sure they are serialized correctly
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
