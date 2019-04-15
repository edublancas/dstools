import subprocess
import tempfile
import networkx as nx

_TASKS = []
_NON_END_TASKS = []


def build_all():
    for task in _TASKS:
        task._already_checked = False

        if task.is_end_task:
            task.build()


def plot():
    # https://networkx.github.io/documentation/networkx-1.10/reference/drawing.html
    # NOTE: requires pygraphviz and pygraphviz
    G = nx.DiGraph()

    for t in _TASKS:
        edges = [(up, t) for up in t.upstream]
        G.add_edges_from(edges)

    G_ = nx.nx_agraph.to_agraph(G)
    # https://graphviz.gitlab.io/_pages/pdf/dot.1.pdf
    # G_.graph_attr.update(rotate=20)
    path = tempfile.mktemp(suffix='.png')

    G_.draw(path, prog='dot', args='')

    subprocess.run(['open', path])
