import subprocess
import tempfile
import networkx as nx
# https://networkx.github.io/documentation/networkx-1.10/reference/drawing.html
# # http://graphviz.org/doc/info/attrs.html
# NOTE: requires pygraphviz and pygraphviz


class MetaProduct:

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
        return max([t.product.timestamp for t in self.dag.tasks])


class DAG:

    def __init__(self):
        self.tasks = []
        self.product = MetaProduct(self)

    def add_task(self, task):
        self.tasks.append(task)

    def mk_graph(self):
        G = nx.DiGraph()

        for t in self.tasks:
            G.add_node(t, color='red' if t.product.outdated() else 'green')

        for t in self.tasks:
            edges = [(up, t) for up in t.upstream]
            G.add_edges_from(edges)

        return G

    def build(self):
        G = self.mk_graph()

        for t in nx.algorithms.topological_sort(G):
            t.build()

    def plot(self):
        G = self.mk_graph()
        G_ = nx.nx_agraph.to_agraph(G)
        path = tempfile.mktemp(suffix='.png')
        G_.draw(path, prog='dot', args='')
        subprocess.run(['open', path])
