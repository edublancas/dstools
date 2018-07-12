import itertools


def make_grid(model_parameters):
    """Generate all possible combinations of parameters given a dictionary
    """

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
