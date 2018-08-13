"""
Path related methods
"""
from pathlib import Path


class PathManager:
    """
    Based on: https://gist.github.com/pazdera/1098129
    """
    __instance = None

    @staticmethod
    def get_instance():

        if PathManager.__instance is None:
            raise Exception("Not instantiated")

        return PathManager.__instance

    def __init__(self, file):
        if PathManager.__instance is not None:
            raise Exception("Already instantiated")
        else:
            self.project_dir = infer_project_dir_from_file(file)

            PathManager.__instance = self


def load_config(config_file):
    """Loads configuration file, asumes file is in config/

    Parameters
    ----------
    file: str
        As returned from __file__
    """
    project_dir = PathManager.get_instance().project_dir
    return Path(project_dir, 'config', config_file).absolute()


def infer_project_dir_from_file(file):
    path_to_file = Path(file).absolute()

    idxs = [i for i, p in enumerate(path_to_file.parts) if p == 'src']

    if not len(idxs):
        raise ValueError("Couldn't infer project directory, no src directory "
                         "was found")

    idx = min(idxs)

    return Path(*path_to_file.parts[:idx])
