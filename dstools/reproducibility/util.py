import logging
import logging.config
import datetime
import hashlib
from pathlib import Path

import yaml


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


def make_logger_file(file):
    """

    Parameters
    ----------
    file:
        Path to file (as returned by __file__)
    """
    path_to_file = Path(file).absolute()
    project_dir = infer_project_dir_from_path_to_file(path_to_file)

    path_to_src = Path(project_dir, 'src')

    path_relative = Path(path_to_file).relative_to(path_to_src)
    path_to_logs = Path(project_dir, 'log')
    path_to_current = Path(path_to_logs, path_relative)

    # remove suffix if exists
    path_to_current = (path_to_current
                       .with_name(path_to_current.name
                                  .replace(path_to_current.suffix, '')))

    if not path_to_current.exists():
        path_to_current.mkdir(parents=True)

    filename = make_filename(extension='log')
    path_to_current_log = Path(path_to_current, filename)

    return str(path_to_current_log)


def infer_project_dir_from_path_to_file(path_to_file):
    idxs = [i for i, p in enumerate(path_to_file.parts) if p == 'src']

    if not len(idxs):
        raise ValueError("Couldn't infer project directory, no src directory "
                         "was found")

    idx = min(idxs)

    return Path(*path_to_file.parts[:idx])


def setup_logger(file, level=None):
    """Configure logging module, assumes logging config is
    in config/logger.yaml
    """
    path_to_file = Path(file).absolute()
    project_dir = infer_project_dir_from_path_to_file(path_to_file)
    path_to_logger_cfg = Path(project_dir, 'config', 'logger.yaml')

    with open(path_to_logger_cfg) as f:
        logging_config = yaml.load(f)

    logging_file = make_logger_file(file)

    logging_config['handlers']['file']['filename'] = logging_file

    if level is not None:
        logging_config['root']['level'] = level

    logging.config.dictConfig(logging_config)
