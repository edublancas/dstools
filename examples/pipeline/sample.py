"""
Sample red.csv and white.csv
"""

from pathlib import Path

import pandas as pd
from dstools import Env

env = Env()

red = pd.read_csv(Path(env.path.input / 'raw' / 'red.csv'), sep=';')
white = pd.read_csv(Path(env.path.input / 'raw' / 'white.csv'), sep=';')

red_sample = red.sample(frac=0.2)
white_sample = white.sample(frac=0.2)

output_path = Path(env.path.input / 'sample')
output_path.mkdir(exist_ok=True)

red_sample.to_csv(output_path / 'red.csv', index=False)
white_sample.to_csv(output_path / 'white.csv', index=False)
