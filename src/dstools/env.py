"""
Environment management
"""
from itertools import chain
from pathlib import Path
from glob import iglob

from dstools.FrozenJSON import FrozenJSON
from dstools.path import PathManager


class Env:
    """
    Env provides a clean and consistent way of managing environment and
    configuration settings. Its simplest usage provides access to settings
    specified via an `env.yaml`.

    Settings managed by Env are intended to be runtime constant (they are NOT
    intended to be used as global variables). For example you might want
    to store database URIs. Storing sensitive information is discouraged as
    yaml files are plain text. Use `keyring` for that instead.

    Examples
    --------

    Basic usage

    >>> from dstools import Env
    >>> env = Env()
    >>> env.db.uri # traverse the yaml tree structure using dot notation
    >>> env.name # returns the environment name
    >>> env.path.home # returns an absolute path to the env file location
    >>> env.path.output # returns an ansolute path to the output folder
    >>> env.path.input # returns an ansolute path to the input folder
    >>> env.path.log # returns an ansolute path to the log folder

    """

    # singleton implementation based on:
    # https://gist.github.com/pazdera/1098129
    __instance = None

    @staticmethod
    def get_instance():

        if Env.__instance is None:
            raise Exception("Not instantiated")

        return Env.__instance

    def __init__(self, path_to_env=None):
        if Env.__instance is not None:
            raise Exception("Already instantiated")
        else:
            if path_to_env is None:
                path_to_env = find_env()

            if path_to_env is None:
                raise ValueError("Couldn't find env.yaml")

            self._env_content = FrozenJSON.from_yaml(path_to_env)

            self._name = _get_name(path_to_env)
            self._path = PathManager(path_to_env, self)

            Env.__instance = self

    def __dir__(self):
        return dir(self._env_content)

    @property
    def name(self):
        return self._name

    @property
    def path(self):
        return self._path

    def __getattr__(self, key):
        return getattr(self._env_content, key)

    @classmethod
    def _destroy(cls):
        cls.__instance = None


def find_env(max_levels_up=3):
    def levels_up(n):
        return chain.from_iterable(iglob('../' * i + '**')
                                   for i in range(n + 1))

    path_to_env = None

    for filename in levels_up(max_levels_up):
        p = Path(filename)

        if p.name == 'env.yaml':
            path_to_env = filename
            break

    return path_to_env


def load_config(config_file):
    """Loads configuration file, asumes file is in config/

    Parameters
    ----------
    file: str
        As returned from __file__
    """
    project_dir = Env.get_instance().project_dir
    return Path(project_dir, 'config', config_file).absolute()


def _get_name(path_to_env):
    filename = str(Path(path_to_env).name)

    err = ValueError('Wrong filename, must be either env.{name}.yaml '
                     'or env.yaml')

    elements = filename.split('.')

    if len(elements) == 2:
        # no name case
        env, _ = elements
        name = 'root'
    elif len(elements) == 3:
        # name
        env, name, _ = elements
    else:
        raise err

    if env != 'env':
        raise err

    return name
