from pathlib import Path


class PathManager:

    def __init__(self, path_to_env, env):
        self._home = Path(path_to_env).resolve().parent

        subdirectory = env.name if env.name != 'root' else ''

        self._input = self._build_absolute_path(env, 'input', subdirectory)
        self._output = self._build_absolute_path(env, 'output', subdirectory)
        self._log = self._build_absolute_path(env, 'log', subdirectory)

    def _build_absolute_path(self, env, key, subdirectory):
        _key = f'_path_to_{key}'
        path_to_key = env._env_content.get(f'{_key}')

        # if there is no path_to_{key} key, use default location
        if path_to_key is None:
            path_to_key_absolute = self._home / key / subdirectory
        else:
            path_to_key = Path(path_to_key).expanduser()

            if not path_to_key.is_absolute():
                raise ValueError(f'{_key} must be an absolute path')

            path_to_key_absolute = path_to_key

        if not path_to_key_absolute.is_dir():
            path_to_key_absolute.mkdir(parents=True)

        return path_to_key_absolute

    @property
    def home(self):
        """Project's home folder
        """
        return self._home

    @property
    def input(self):
        """Project's input folder
        """
        return self._input

    @property
    def output(self):
        """Project's output folder
        """
        return self._output

    @property
    def log(self):
        """Project's log folder
        """
        return self._log
