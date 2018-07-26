from dstools import plot

import numpy as np
import matplotlib.pyplot as plt


x = np.random.rand(15, 10, 10)
y = x + 1

plt.plot(x[0])
plt.show()

# plot all elements
plot.grid_from_array(x, axis=0)
plt.show()


# plot 3 at random
plot.grid_from_array(x, axis=0, elements=3)
plt.show()

# plot 50% at random
plot.grid_from_array(x, axis=0, elements=0.5)
plt.show()

# plot specific elements
plot.grid_from_array(x, axis=0, elements=[2, 4, 6, 8, 10])
plt.show()


# you can pass other parameters, which are passed to the to the
# matplotlib.pyplot.subplots
plot.grid_from_array(x, axis=0, elements=3, sharex=False, figsize=(3, 3))
plt.show()

# plot x, y pairs
plot.grid_from_array([x, y], axis=0, group_names=('x', 'x+1'),
                     elements=[0, 2, 4], sharey=True)
plt.show()


# automatically setting figsize
plot.grid_from_array([x, y], axis=0, group_names=('x', 'x+1'),
                     elements=[0, 2, 4], sharey=True, auto_figsize=4)
plt.show()

plot.grid_from_array([x, y], axis=0, group_names=('x', 'x+1'),
                     elements=[0, 2, 4], sharey=True, auto_figsize=(4, 2))
plt.show()
