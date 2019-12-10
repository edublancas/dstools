"""
DAG executors
"""
from multiprocessing import Pool, TimeoutError

import networkx as nx
from tqdm.auto import tqdm
from dstools.pipeline.Table import BuildReport
from dstools.pipeline.constants import TaskStatus


class Serial:
    """Runs a DAG serially
    """
    TASKS_CAN_CREATE_CHILD_PROCESSES = True
    STOP_ON_EXCEPTION = True

    def __init__(self, dag):
        self.dag = dag

    def __call__(self, **kwargs):
        status_all = []

        g = self.dag._to_graph()
        pbar = tqdm(nx.algorithms.topological_sort(g), total=len(g))

        for t in pbar:
            pbar.set_description('Building task "{}"'.format(t.name))

            try:
                t.build(**kwargs)
            except Exception as e:
                if self.dag._on_task_failure:
                    self.dag._on_task_failure(t)

                raise e
            else:
                if self.dag._on_task_finish:
                    self.dag._on_task_finish(t)

            status_all.append(t.build_report)

        build_report = BuildReport(status_all)
        self.dag._logger.info(' DAG report:\n{}'.format(repr(build_report)))

        for client in self.dag.clients.values():
            client.close()

        return build_report


class Parallel:
    """Runs a DAG in parallel using the multiprocessing module
    """
    # Tasks should not create child processes, see documention:
    # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Process.daemon
    TASKS_CAN_CREATE_CHILD_PROCESSES = False
    STOP_ON_EXCEPTION = False

    def __init__(self, dag):
        self.dag = dag

    def __call__(self, **kwargs):
        # TODO: Have to test this with other Tasks, especially the ones that use
        # clients - have to make sure they are serialized correctly
        done = []
        started = []

        # there might be up-to-date tasks, add them to done
        # FIXME: this only happens when the dag is already build and then
        # then try to build again (in the same session), if the session
        # is restarted even up-to-date tasks will be WaitingExecution again
        # this is a bit confusing, so maybe change WaitingExecution
        # to WaitingBuild?
        for name in self.dag:
            if self.dag[name]._status == TaskStatus.Executed:
                done.append(self.dag[name])

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
            # update task status for tasks in the done list
            for task in done:
                task = self.dag[task.name]
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
            for task_name in self.dag:
                # ignore tasks that are already started, I should probably add an
                # executing status but that cannot exist in the task itself,
                # maybe in the manaer?
                if (self.dag[task_name]._status == TaskStatus.WaitingExecution
                        and self.dag[task_name] not in started):
                    t = self.dag[task_name]
                    return t
                # there might be some up-to-date tasks, add them

            # if all tasks are done, stop
            if set([t.name for t in done]) == set(self.dag):
                raise StopIteration

        with Pool(processes=4) as pool:
            while True:
                try:
                    task = next_task()
                except StopIteration:
                    break
                else:
                    if task is not None:
                        res = pool.apply_async(
                            task.build, [], kwds=kwargs, callback=callback)
                        started.append(task)
                        print('started', task.name)
                        # time.sleep(3)
