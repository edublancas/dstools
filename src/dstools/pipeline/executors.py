"""
DAG executors
"""
import networkx as nx
from tqdm.auto import tqdm
from dstools.pipeline.Table import BuildReport


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
    dag._logger.info(' DAG report:\n{}'.format(repr(dag.build_report)))

    for client in dag.clients.values():
        client.close()

    return build_report
