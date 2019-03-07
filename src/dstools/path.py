from pathlib import Path


class PathManager:

    def __init__(self, path_to_env, env):
        self.home = Path(path_to_env).resolve().parent

        path_to_output = env._env_content.get('path_to_output')
        subdirectory = env.name if env.name != 'root' else ''

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
