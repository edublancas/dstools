from pydoc import locate
from shlex import quote
import sys
import subprocess
import itertools
import logging
import logging.config
import datetime
import hashlib
from pathlib import Path

import yaml

from dstools.env import Env
from dstools.util import ensure_iterator, _unwrap_if_single_element


@ensure_iterator(param=('suffix', 'extension'))
def make_filename(suffix=None, extension=None, timestamp_separator='__'):
    """Generate filename(s) with the current datetime in ISO 8601 format
    """
    now = datetime.datetime.now()
    filename = now.strftime('%Y-%m-%dT%H-%M-%S')

    if suffix is None:
        names = [filename]
    else:
        names = [filename+timestamp_separator+suf for suf in suffix]

    if extension is None:
        res = names
    else:
        res = [n+'.'+e for n, e in itertools.product(names, extension)]

    return _unwrap_if_single_element(res)


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
    # FIXME; is this is in site-directories, I can still use the path
    # there to create a log directory relative to the home folder
    env = Env()

    path_to_file = Path(file).absolute()
    filename = path_to_file.name
    filename_no_ext = filename.replace(path_to_file.suffix, '')

    dirname = path_to_file.with_name(filename_no_ext)
    path_relative = Path(dirname).relative_to(env.path.home)

    path_to_log_dir = Path(env.path.home, 'log')
    path_to_log_local_dir = Path(path_to_log_dir, path_relative)

    if not path_to_log_local_dir.exists():
        path_to_log_local_dir.mkdir(parents=True)

    filename = make_filename(extension='log')
    path_to_log_local = Path(path_to_log_local_dir, filename)

    return path_to_log_local


def setup_logger(file, level=None):
    """Configure logging module, assumes logging config is
    in config/logger.yaml

    Parameters
    ----------
    file: str
        As returned from __file__
    """
    project_dir = Env().path.home
    path_to_logger_cfg = str(Path(project_dir, 'config', 'logger.yaml'))

    with open(path_to_logger_cfg) as f:
        logging_config = yaml.load(f)

    logging_file = make_logger_file(file)

    logging_config['handlers']['file']['filename'] = logging_file

    if level is not None:
        logging_config['root']['level'] = level

    logging.config.dictConfig(logging_config)


def _run_command(path, command):
    """Safely run command in certain path
    """
    if not Path(path).is_dir():
        raise ValueError('{} is not a directory'.format(path))

    command_ = 'cd {path} && {cmd}'.format(path=quote(path), cmd=command)

    out = subprocess.check_output(command_, shell=True)
    return out.decode('utf-8') .replace('\n', '')


def one_line_git_summary(path):
    """Get one line git summary"""
    return _run_command(path, 'git show --oneline -s')


def git_hash(path):
    """Get git hash"""
    return _run_command(path, 'git rev-parse HEAD')


def get_version(package_name):
    """Get package version
    """
    installation_path = sys.modules[package_name].__file__

    NON_EDITABLE = True if 'site-packages/' in installation_path else False

    if NON_EDITABLE:
        return locate('{package}.__version__'
                      .format(package_name=package_name))
    else:
        parent = str(Path(installation_path).parent)

        return one_line_git_summary(parent)


def git_hash_in_path(path):
    return one_line_git_summary(path)
