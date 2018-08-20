from pathlib import Path
import shutil
import tempfile

from dstools import Env

temp = Path(tempfile.mkdtemp())
path_to_env = temp / 'env.yaml'

shutil.copy(Path('assets', 'env.yaml'), path_to_env)

env = Env(path_to_env)


shutil.rmtree()