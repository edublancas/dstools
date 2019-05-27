"""
DAG module
"""
from collections import OrderedDict
import subprocess
import tempfile
import networkx as nx
from tabulate import tabulate

from dstools.pipeline.products import MetaProduct


class DAGStats(OrderedDict):

    def __str__(self):
        names = [t.name for t in self.keys()]
        content = [list(d.values()) for d in self.values()]
        total = sum([elapsed or 0 for run, elapsed in content])

        def prop(elapsed, total):
            if elapsed is None:
                return None
            else:
                return 100 * elapsed / total

        rows = [(name, run, elapsed, prop(elapsed, total))
                for name, (run, elapsed) in zip(names, content)]

        return tabulate(rows, headers=['Task name', 'Ran?', 'Elapsed',
                                       'Percentage'],
                        floatfmt='.2f')


class DAG:
    """A DAG is a collection of tasks with dependencies
    """

    def __init__(self, name=None):
        self.tasks = []
        self.tasks_by_name = {}
        self.name = name

    @property
    def product(self):
        # We have to rebuild it since tasks might have been added
        return MetaProduct([t.product for t in self.tasks])

    def add_task(self, task):
        """Adds a task to the DAG
        """
        self.tasks.append(task)

        if task.name is not None:
            self.tasks_by_name[task.name] = task

    def mk_graph(self, add_properties=False):
        """
        Return a networkx directed graph from declared tasks and declared
        upstream dependencies
        """
        G = nx.DiGraph()

        for t in self.tasks:
            G.add_node(t)
            G.add_edges_from([(up, t) for up in t.upstream])

        for t in nx.algorithms.topological_sort(G):
            t.render()

        if add_properties:
            for n, data in G.nodes(data=True):
                data['color'] = 'red' if n.product.outdated() else 'green'
                data['label'] = n.short_repr()

        return G

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
        # attributes docs:
        # https://graphviz.gitlab.io/_pages/doc/info/attrs.html
        G = self.mk_graph(add_properties=True)

        stats = DAGStats()

        for t in nx.algorithms.topological_sort(G):
            status = t.build()
            stats[t] = status

        return stats

    def plot(self):
        """Plot the DAG
        """
        # https://networkx.github.io/documentation/networkx-1.10/reference/drawing.html
        # # http://graphviz.org/doc/info/attrs.html
        # NOTE: requires pygraphviz and pygraphviz
        G = self.mk_graph(add_properties=True)
        G_ = nx.nx_agraph.to_agraph(G)
        path = tempfile.mktemp(suffix='.png')
        G_.draw(path, prog='dot', args='-Grankdir=LR')
        subprocess.run(['open', path])

    def status(self):
        """Returns the status of each node in the DAG
        """
        G = self.mk_graph(add_properties=True)
        return [t.status() for t in nx.algorithms.topological_sort(G)]

    # def __getitem__(self, key):
        # return self.tasks_by_name[key]

    def to_dict(self):
        """
        Convert the DAG to a dictionary where each key is a task
        """
        g = self.mk_graph()
        return {n.name: n for n in g.nodes()}

    def __repr__(self):
        name = self.name if self.name is not None else 'Unnamed'
        return f'{type(self).__name__}: {name}'

    def short_repr(self):
        return repr(self)
