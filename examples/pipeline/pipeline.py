%load_ext autoreload
%autoreload 2

import logging
from pathlib import Path

from dstools.pipeline.products import File
from dstools.pipeline.tasks import BashCommand, BashScript, PostgresScript, PythonScript
from dstools.pipeline import build_all
from dstools import Env


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


env = Env()

# TODO: be able to specify more than one product?
red = File(Path(env.path.input / 'raw' / 'red.csv'))
red_source = env.path.home / 'get_data.sh'
t1 = BashScript(red_source, red)

red_sample = File(Path(env.path.input / 'sample' / 'red.csv'))
red_sample_source = env.path.home / 'sample.py'
t2 = PythonScript(red_sample_source, red_sample)
t2.set_upstream(t1)

build_all()
