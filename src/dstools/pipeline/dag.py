import subprocess
import tempfile
import networkx as nx
# https://networkx.github.io/documentation/networkx-1.10/reference/drawing.html
# # http://graphviz.org/doc/info/attrs.html
# NOTE: requires pygraphviz and pygraphviz


class DAGProduct:
    """
    A class that exposes a Product-like API for representing
    a DAG status
    """

    def __init__(self, dag):
        self.dag = dag

    def outdated(self):
        return (self.outdated_data_dependencies()
                or self.outdated_code_dependency())

    def outdated_data_dependencies(self):
        return any([t.product.outdated_data_dependencies()
                    for t in self.dag.tasks])

    def outdated_code_dependency(self):
        return any([t.product.outdated_code_dependency()
                    for t in self.dag.tasks])

    @property
    def timestamp(self):
        timestamps = [t.product.timestamp
                      for t in self.dag.tasks
                      if t.product.timestamp is not None]
        if timestamps:
            return max(timestamps)
        else:
            return None


class DAG:

    def __init__(self, name=None):
        self.tasks = []
        self.tasks_by_name = {}
        self.product = DAGProduct(self)
        self.name = name

    def add_task(self, task):
        self.tasks.append(task)

        if task.name is not None:
            self.tasks_by_name[task.name] = task

    def mk_graph(self):
        G = nx.DiGraph()

        for t in self.tasks:
            G.add_node(t)
            G.add_edges_from([(up, t) for up in t.upstream])

        for n, data in G.nodes(data=True):
            data['color'] = 'red' if n.product.outdated() else 'green'
            data['label'] = n.short_repr()

        return G

    def build(self):
        # attributes docs:
        # https://graphviz.gitlab.io/_pages/doc/info/attrs.html
        G = self.mk_graph()

        for t in nx.algorithms.topological_sort(G):
            t.build()

    def plot(self):
        G = self.mk_graph()
        G_ = nx.nx_agraph.to_agraph(G)
        path = tempfile.mktemp(suffix='.png')
        G_.draw(path, prog='dot', args='-Grankdir=LR')
        subprocess.run(['open', path])

    # def __getitem__(self, key):
    #     return self.tasks_by_name[key]

    def __repr__(self):
        name = self.name if self.name is not None else 'Unnamed'
        return f'{type(self).__name__}: {name}'

    def short_repr(self):
        return repr(self)
