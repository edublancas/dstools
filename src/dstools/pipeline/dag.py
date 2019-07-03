"""
DAG module
"""
import logging
from collections import OrderedDict
import subprocess
import tempfile
import networkx as nx

from dstools.pipeline.build_report import BuildReport
from dstools.pipeline.products import MetaProduct


class DAG:
    """A DAG is a collection of tasks with dependencies

    Attributes
    ----------
    build_report: BuildStatus
        A dict-like object with tasks as keys and BuildStatus objects for each
        task as values. str(BuildStatus) returns a table in plain text. This
        object is created after build() is run, otherwise is None
    """
    # TODO: remove the tasks, and tasks_by_name properties and use the
    # networkx.DiGraph structure directly to avoid having to re-build the
    # graph every time

    def __init__(self, name=None):
        self.tasks = []
        self.tasks_by_name = {}
        self.name = name
        self.logger = logging.getLogger(__name__)
        self.build_report = None

    @property
    def product(self):
        # We have to rebuild it since tasks might have been added
        return MetaProduct([t.product for t in self.tasks])

    def add_task(self, task):
        """Adds a task to the DAG
        """
        if task.name in self.tasks_by_name.keys():
            raise ValueError('DAGs cannot have Tasks with repeated names, '
                             f'there is a Task with name "{task.name}" '
                             'already')

        self.tasks.append(task)

        if task.name is not None:
            self.tasks_by_name[task.name] = task

    def to_graph(self, only_current_dag=False):
        G = nx.DiGraph()

        for task in self.tasks:
            G.add_node(task)

            if only_current_dag:
                G.add_edges_from([(up, task) for up
                                  in task.upstream if up.dag is self])
            else:
                G.add_edges_from([(up, task) for up in task.upstream])

        return G

    def render(self):
        """Render the graph
        """
        g = self.to_graph()

        dags = set([t.dag for t in g])

        # first render any other dags involved (this happens when some
        # upstream parameters come form other dags)
        for dag in dags:
            if dag is not self:
                dag._render_current()

        # then, render this dag
        self._render_current()

    def _render_current(self):
        g = self.to_graph(only_current_dag=True)

        for t in nx.algorithms.topological_sort(g):
            t.render()

    def build(self):
        """
        Runs the DAG in order so that all upstream dependencies are run for
        every task

        Returns
        -------
        DAGStats
            A dict-like object with tasks as keys and dicts with task
            status as values. str(DAGStats) returns a table in plain text
        """
        self.render()

        # attributes docs:
        # https://graphviz.gitlab.io/_pages/doc/info/attrs.html

        status_all = OrderedDict()

        for t in nx.algorithms.topological_sort(self.to_graph()):
            status_all[t] = t.build().build_report

        self.build_report = BuildReport.from_components(status_all)
        self.logger.info(f' DAG status:\n{self.build_report}')

        return self

    def plot(self):
        """Plot the DAG
        """
        self.render()

        G = self.to_graph()

        for n, data in G.nodes(data=True):
            data['color'] = 'red' if n.product.outdated() else 'green'
            data['label'] = n.short_repr()

        # https://networkx.github.io/documentation/networkx-1.10/reference/drawing.html
        # # http://graphviz.org/doc/info/attrs.html
        # NOTE: requires pygraphviz and pygraphviz
        G_ = nx.nx_agraph.to_agraph(G)
        path = tempfile.mktemp(suffix='.png')
        G_.draw(path, prog='dot', args='-Grankdir=LR')
        subprocess.run(['open', path])

    def status(self):
        """Returns the status of each node in the DAG
        """
        self.render()
        return [t.status() for t
                in nx.algorithms.topological_sort(self.to_graph())]

    # def __getitem__(self, key):
        # return self.tasks_by_name[key]

    def to_dict(self):
        """
        Convert the DAG to a dictionary where each key is a task
        """
        self.render()
        return {n.name: n for n in self.to_graph().nodes()}

    def __repr__(self):
        name = self.name if self.name is not None else 'Unnamed'
        return f'{type(self).__name__}: {name}'

    def short_repr(self):
        return repr(self)
