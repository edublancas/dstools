from dstools import plot

import numpy as np
import matplotlib.pyplot as plt


x = np.random.rand(15, 10, 10)

plt.plot(x[0])
plt.show()


plot.grid(x, 0)
plt.show()

plot.grid(x, 0, [2, 4, 6, 8, 10])
plt.show()
