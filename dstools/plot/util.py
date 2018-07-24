from math import floor, ceil, sqrt


def grid_size(n_elements, max_cols=None):
    """Compute grid size for n_elements
    """
    sq_value = sqrt(n_elements)
    cols = int(floor(sq_value))
    rows = int(ceil(sq_value))
    rows = rows + 1 if rows * cols < n_elements else rows

    if max_cols and cols > max_cols:
        rows = ceil(n_elements/max_cols)
        cols = max_cols

    return rows, cols
