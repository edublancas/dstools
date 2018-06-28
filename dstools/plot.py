"""
Tools for matplotlib plotting
"""
import collections
from math import floor, ceil, sqrt
import matplotlib.pyplot as plt


def grid(data, axis, elements=None, **kwargs):
    def plotting_fn(data, label, ax):
        ax.plot(data)
        ax.set_title(label)

    def element_getter(data, i):
        slicer = [slice(None) for _ in range(data.ndim)]
        slicer[axis] = i
        return data[slicer]

    def label_getter(labels, i):
        return labels[i]

    if elements is None:
        elements = range(data.shape[axis])

    labels = range(data.shape[axis])

    make_grid_plot(plotting_fn, data, elements, element_getter, labels,
                   label_getter, **kwargs)

    plt.tight_layout()


def is_iter(obj):
    return isinstance(obj, collections.Iterable)


def grid_size(n_elements, max_cols=None):
    """Compute grid size for n_elements
    """
    total = len(n_elements)
    sq_value = sqrt(total)
    cols = int(floor(sq_value))
    rows = int(ceil(sq_value))
    rows = rows + 1 if rows * cols < len(n_elements) else rows

    if max_cols and cols > max_cols:
        rows = ceil(total/max_cols)
        cols = max_cols

    return rows, cols


def make_grid_plot(function, data, elements, element_getter,
                   labels=None, label_getter=None, sharex=True, sharey=True,
                   max_cols=None):
    """Make a grid plot
    """
    rows, cols = grid_size(elements, max_cols)

    fig, axs = plt.subplots(rows, cols, sharex=sharex, sharey=sharey)

    axs = axs if is_iter(axs) else [axs]

    if cols > 1:
        axs = [item for sublist in axs for item in sublist]

    for element, ax in zip(elements, axs):
        if label_getter is not None and labels is not None:
            function(data=element_getter(data, element),
                     label=label_getter(labels, element), ax=ax)
        else:
            function(data=element_getter(data, element), ax=ax)

    return fig
