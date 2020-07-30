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

    # validate generated spec
    assert spec.spec['cat']['kind'] == 'categorical'
    np.testing.assert_approx_equal(spec.spec['cat']['nas_prop'],
                                   0.5,
                                   significant=2)
    assert set(spec.spec['cat']['values']) == {'a', 'b', 'c'}

    assert spec.spec['num']['kind'] == 'numeric'
    np.testing.assert_approx_equal(spec.spec['num']['nas_prop'],
                                   0.5,
                                   significant=2)
    np.testing.assert_array_almost_equal(spec.spec['num']['range'], [0, 1],
                                         decimal=1)

    sampler = NaiveSampler(spec.spec)
    sample = sampler.sample(n=1000)
    assert spec.validate(sample)

    spec_sample = DataSpec.from_df(sample).spec

    # TODO: compare the rest of entries to the original spec
    assert spec_sample['cat']['kind'] == 'categorical'
    assert spec_sample['num']['kind'] == 'numeric'
