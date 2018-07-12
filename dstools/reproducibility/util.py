import datetime
import hashlib
from pathlib import Path


def make_path(*args, extension=None):
    return Path(*args, make_filename(extension))


def make_filename(extension=None):
    """Generate a filename with the current datetime
    """
    now = datetime.datetime.now()
    name = now.strftime('%d-%b-%Y@%H-%M-%S')

    if extension is not None:
        name += '.'+extension

    return name


def hash_array(a):
    """Hash a numpy array using sha1

    Notes
    -----
    http://stackoverflow.com/questions/5386694/fast-way-to-hash-numpy-objects-for-caching
    http://stackoverflow.com/questions/806151/how-to-hash-a-large-object-dataset-in-python
    """
    import numpy as np

    # conver to contigous in case the array has a different
    # representation
    a = np.ascontiguousarray(a)

    # get a view from the array, this will help produce different hashes
    # for arrays with same data but different shapes
    a = a.view(np.uint8)

    return hashlib.sha1(a).hexdigest()
