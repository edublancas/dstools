"""
Sample red.csv and white.csv
"""

from pathlib import Path

import pandas as pd
from dstools import Env


def sample(product, upstream):
    env = Env()

    red = pd.read_csv(str(upstream['get_data']), sep=';')
    white = pd.read_csv(Path(env.path.input / 'raw' / 'white.csv'), sep=';')

    red_sample = red.sample(frac=0.2)
    white_sample = white.sample(frac=0.2)

    output_path = Path(env.path.input / 'sample')
    output_path.mkdir(exist_ok=True)

    red_sample.to_csv(str(product[0]), index=False)
    white_sample.to_csv(str(product[1]), index=False)
