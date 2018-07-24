"""
Tools for matplotlib plotting
"""
import numbers
import collections
import random

import matplotlib.pyplot as plt

from dstools.plot import util


def grid_from_array(data, axis, **kwargs):
    """Plot a grid from a numpy ndarray

    Parameters
    ----------
    data: numpy.ndarray
        The data to plot

    axis: int
        Axis that holds a single observation. e.g. if axis is 1 in a 3D array,
        then getting the ith element is done by: array[:, i, :]

    **kwargs
        kwargs passed to the generic grid function
    """

    def plotting_fn(data, label, ax):
        ax.plot(data)
        ax.set_title(label)

    def element_getter(data, i):
        slicer = [slice(None) for _ in range(data.ndim)]
        slicer[axis] = i
        return data[slicer]

    def label_getter(labels, i):
        return labels[i]

    grid(function=plotting_fn,
         data=data,
         element_getter=element_getter,
         all_elements=range(data.shape[axis]),
         labels=range(data.shape[axis]),
         label_getter=label_getter,
         **kwargs)

    plt.tight_layout()


def grid(function, data, element_getter, all_elements, labels, label_getter,
         elements=None, max_cols=None, **subplots_kwargs):
    """
    Utility function for building grid graphs with arbitrary plotting functions
    and data

    Parameters
    ----------
    function: callable
        Plotting finction, must accept a data (data for that plot), optional
        label (label for that plot) and ax (matplotlib Axes object for that
        plot) parameters

    data: anything
        Data to plot

    elements: list, float, int or None
        Which elements to plot, if a list plots the elements identified with
        every element on the list, if float it samples that percent of
        elements and plots it, if int samples that number of elements, if None
        plots all elements

    element_getter: callable
        A function that accepts a data (all the data) and an element (key for
        the current element) parameter and returns the data to be passed to
        the plotting function

    all_elements: list
        A list with the keys identifying every element in elements

    labels: dictionary, optional
        A dictionary mapping keys with labels

    subplots_kwargs: kwargs
        Other kwargs passed to the matplotlib.pyplot.subplots function
    """

    total_n_elements = len(all_elements)

    if elements is None:
        elements = all_elements

    elif isinstance(elements, numbers.Integral):
        if total_n_elements < elements:
            elements = total_n_elements

        elements = random.sample(all_elements, elements)

    elif isinstance(elements, numbers.Real):
        if not 0 < elements < 1:
            raise ValueError('If float, elements must be in (0, 1)')

        elements = random.sample(all_elements,
                                 int(total_n_elements * elements))

    elif isinstance(elements, collections.Iterable):
        pass

    else:
        raise ValueError(f'Invalid elements type ({type(elements)})')

    n_elements = len(elements)

    rows, cols = util.grid_size(n_elements, max_cols)

    fig, axs = plt.subplots(rows, cols, **subplots_kwargs)

    axs = axs if isinstance(axs, collections.Iterable) else [axs]

    if cols > 1:
        axs = [item for sublist in axs for item in sublist]

    for element, ax in zip(elements, axs):
        if label_getter is not None and labels is not None:
            function(data=element_getter(data, element),
                     label=label_getter(labels, element), ax=ax)
        else:
            function(data=element_getter(data, element), ax=ax)

    return fig
