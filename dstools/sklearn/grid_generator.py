# This python script generates several models given its name on
# sklearn
from pydoc import locate
import dstools.sklearn._all_grids as _all_grids
from dstools.params.grid import make_grid


def grid_from_classes(classes, size='small'):
    all_models = []
    for class_name in classes:
        _grid = getattr(_all_grids, size)
        # Get grid values for the given class
        values = _grid[class_name]
        # Generate cross product for all given values and return them as dicts
        grids = make_grid(values)
        # instantiate a model for each grid
        models = [locate(class_name)(**g) for g in grids]
        # add to acumulator
        all_models.extend(models)

    return all_models
