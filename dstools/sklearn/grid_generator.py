# This python script generates several models given its name on
# sklearn
import itertools
from pydoc import locate
import dstools.sklearn._all_grids as _all_grids


def _generate_grid(model_parameters):
    # Iterate over keys and values
    parameter_groups = []
    for key in model_parameters:
        # Generate tuple key,value
        tuples = list(itertools.product([key], model_parameters[key]))
        parameter_groups.append(tuples)
    # Cross product over each group
    parameters = list(itertools.product(*parameter_groups))
    # Convert each result to dict
    dicts = [_tuples2dict(params) for params in parameters]
    return dicts


def _tuples2dict(tuples):
    return dict((x, y) for x, y in tuples)


def grid_from_classes(classes, size='small'):
    all_models = []
    for class_name in classes:
        _grid = getattr(_all_grids, size)
        # Get grid values for the given class
        values = _grid[class_name]
        # Generate cross product for all given values and return them as dicts
        grids = _generate_grid(values)
        # instantiate a model for each grid
        models = [locate(class_name)(**g) for g in grids]
        # add to acumulator
        all_models.extend(models)

    return all_models
