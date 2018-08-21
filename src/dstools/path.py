from pathlib import Path


class PathManager:

    def __init__(self, path_to_env, env):
        self.env_dir = Path(path_to_env).resolve().parent

        self.home = self.env_dir / env.name

        if not self.home.is_dir():
            self.home.mkdir()

        self.data = self.home / 'data'
        self.log = self.home / 'log'
