import os
import pytest
from dstools.spec.DataSpec import DataSpec
from dstools.spec.NaiveSampler import NaiveSampler
import pandas as pd
import numpy as np


def test_naive_sampler():
    df = pd.DataFrame({
        'cat': ['a', 'a', 'b', 'c', 'd'],
        'num': np.random.rand(5)
    })

    spec = DataSpec.from_df(df)

    sampler = NaiveSampler(spec.spec)

    sampler.sample(n=10)