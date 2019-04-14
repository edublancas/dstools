import networkx as nx
from dstools.pipeline import _TASKS
import matplotlib.pyplot as plt

# https://networkx.github.io/documentation/networkx-1.10/reference/drawing.html
G = nx.Graph()

for t in _TASKS:
    edges = [(up, t) for up in t.upstream]
    G.add_edges_from(edges)

nx.draw_networkx(G)
plt.show()
