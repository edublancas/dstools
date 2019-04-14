import networkx as nx
from dstools.pipeline import _TASKS
import matplotlib.pyplot as plt

# https://networkx.github.io/documentation/networkx-1.10/reference/drawing.html
G = nx.DiGraph()

for t in _TASKS:
    edges = [(up, t) for up in t.upstream]
    G.add_edges_from(edges)

nx.draw_networkx(G)
plt.show()


from networkx.algorithms import topological_sort

s = list(topological_sort(G))

pos = {s[0]: (0,0), s[-1]: (0, 20)}
fixed = [s[0], s[-1]]

nx.draw(G, nx.spring_layout(G, pos=pos, fixed=fixed))

nx.draw_planar(G)
plt.show()