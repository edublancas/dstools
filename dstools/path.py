"""
Path related methods
"""
from itertools import chain
from pathlib import Path
from glob import iglob

from dstools.FrozenJSON import FrozenJSON


class Env:
    """
    Based on: https://gist.github.com/pazdera/1098129
    """
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

            self._env = FrozenJSON.from_yaml(path_to_env)

            Env.__instance = self

    def __getattr__(self, key):
        return getattr(self._env, key)


def find_env(max_levels_up=3):
    def levels_up(n):
        return chain.from_iterable(iglob('../' * i + '**', recursive=False)
                                   for i in range(1, n + 1))

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


def infer_project_dir_from_file(file):
    path_to_file = Path(file).absolute()

    idxs = [i for i, p in enumerate(path_to_file.parts) if p == 'src']

    if not len(idxs):
        raise ValueError("Couldn't infer project directory, no src directory "
                         "was found")

    idx = min(idxs)

    return Path(*path_to_file.parts[:idx])
