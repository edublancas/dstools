from dstools import plot

import numpy as np
import matplotlib.pyplot as plt


x = np.random.rand(15, 10, 10)

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
