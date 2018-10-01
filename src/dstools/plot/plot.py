"""
Tools for matplotlib plotting
"""
import logging
import numbers
import collections
import random
import itertools

import matplotlib.pyplot as plt

from dstools.plot import util


def flatten(l):
    return [item for sublist in l for item in sublist]


def grid_from_array(data, axis, group_names=None, labels=None,
                    auto_tight_layout=True, **kwargs):
    """Plot a grid from a numpy ndarray

    Parameters
    ----------
    data: numpy.ndarray or list
        The data to plot

    axis: int
        Axis that holds a single observation. e.g. if axis is 1 in a 3D array,
        then getting the ith element is done by: array[:, i, :]

    group_names: str, optional
        Names for every group in data (only valid when data is a list)

    **kwargs
        kwargs passed to the generic grid function
    """
    # if list check dimensions
    if isinstance(data, list):
        if len(set([d.shape[axis] for d in data])) != 1:
            raise ValueError('axis dimension for elements in data must be '
                             'equal')
        # if dimensions match, get the number of elements
        else:
            n = data[0].shape[axis]
    else:
        n = data.shape[axis]

    def plotting_fn(data, label, ax):
        ax.plot(data)
        ax.set_title(label)

    def element_getter(data, i, coords):
        """Get ith element from numpy.ndarray
        """
        row, col = coords

        if isinstance(data, list):
            data = data[col]

        slicer = [slice(None) for _ in range(data.ndim)]
        slicer[axis] = i
        return data[slicer]

    def label_getter(labels, i, coords):
        if isinstance(data, list) and group_names:
            row, col = coords
            return '{} ({})'.format(labels[i], group_names[col])
        else:
            return labels[i]

    grid(function=plotting_fn,
         data=data,
         element_getter=element_getter,
         all_elements=range(n),
         labels=labels if labels is not None else range(n),
         label_getter=label_getter,
         ax_per_element=(1 if not isinstance(data, list)
                         else len(data)),
         **kwargs)

    if auto_tight_layout:
        plt.tight_layout()


def grid(function, data, element_getter, all_elements, labels, label_getter,
         elements=None, max_cols=None, ax_per_element=1, auto_figsize=None,
         **subplots_kwargs):
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

    ax_per_element: int, optional
        How many axes creater per element, defaults to 1

    subplots_kwargs: kwargs
        Other kwargs passed to the matplotlib.pyplot.subplots function

    max_cols: int, optional
        Maximum number of columns, ignored if ax_per_element > 1
    """
    logger = logging.getLogger(__name__)

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

    # if more than one ax per sample, create repeating pattern, and ignore
    # max_cols
    if ax_per_element > 1:
        elements = flatten([[e] * ax_per_element for e in elements])
        max_cols = ax_per_element

    rows, cols = util.grid_size(int(n_elements * ax_per_element), max_cols)

    if isinstance(auto_figsize, numbers.Integral):
        subplots_kwargs['figsize'] = (cols * auto_figsize,
                                      rows * auto_figsize)
    elif isinstance(auto_figsize, tuple):
        subplots_kwargs['figsize'] = (cols * auto_figsize[0],
                                      rows * auto_figsize[1])

    logger.debug('Rows: {}, Cols: {}'.format(rows, cols))

    fig, axs = plt.subplots(rows, cols, **subplots_kwargs)

    axs = axs if isinstance(axs, collections.Iterable) else [axs]

    if cols > 1:
        axs = flatten(axs)

    # generate coordinates for evert ax
    coords_all = itertools.product(range(rows), range(cols))

    for element, ax, coords in zip(elements, axs, coords_all):

        logger.debug('Plotting in {}, {}'.format(*coords))

        if label_getter is not None and labels is not None:
            function(data=element_getter(data, element, coords),
                     label=label_getter(labels, element, coords),
                     ax=ax)
        else:
            function(data=element_getter(data, element, coords),
                     ax=ax)

    return fig
