from pathlib import Path


class PathManager:

    def __init__(self, path_to_env, env):
        self.home = Path(path_to_env).resolve().parent

        subdirectory = env.name if env.name != 'root' else ''

        path_to_output = env._env_content.get('_path_to_output')

        # if there is no path_to_output key, use default location
        if path_to_output is None:
            self.output = self.home / 'output' / subdirectory

        else:
            path_to_output = Path(path_to_output).expanduser()

            if not path_to_output.is_absolute():
                raise ValueError('output.path must be an absolute path')

            self.output = path_to_output

        if not self.output.is_dir():
            self.output.mkdir()

        path_to_origin = env._env_content.get('_path_to_origin')

        # if there is no path_to_origin key, use default location
        if path_to_origin is None:
            self.origin = self.home / 'origin' / subdirectory

        else:
            path_to_origin = Path(path_to_origin).expanduser()

            if not path_to_origin.is_absolute():
                raise ValueError('_path_to_origin must be an absolute path')

            self.origin = path_to_origin

        if not self.origin.is_dir():
            self.origin.mkdir()
