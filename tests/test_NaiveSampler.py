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


def test_naive_sampler_nas():
    size = 10000

    df = pd.DataFrame({
        'cat': np.random.choice(['a', 'b', 'c'], size=size),
        'num': np.random.random_sample(size=size)
    })

    for col in df:
        na_idx = np.random.choice([False, True], size=size)
        df.loc[na_idx, col] = np.nan

    spec = DataSpec.from_df(df)

    assert spec.spec['cat']['kind'] == 'categorical'
    np.testing.assert_approx_equal(spec.spec['cat']['nas_prop'],
                                   0.5,
                                   significant=2)
    assert set(spec.spec['cat']['values']) == {'a', 'b', 'c'}

    sampler = NaiveSampler(spec.spec)

    # TODO: test spec extracted values, test validate does not raise errors,
    # test sample properties

    sample = sampler.sample(n=1000)

    assert spec.validate(sample)
