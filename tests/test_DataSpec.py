import os
import pytest
from dstools.spec.DataSpec import DataSpec
import pandas as pd
import numpy as np
from sklearn import datasets


def to_df(data):
    df = pd.DataFrame(data['data'])
    df.columns = data['feature_names']
    df['target'] = data['target']
    return df


@pytest.mark.parametrize('loader', [
    datasets.load_boston, datasets.load_diabetes, datasets.load_breast_cancer,
    datasets.load_iris, datasets.load_wine
])
def test_data_spec(loader):
    data = loader()

    df = to_df(data)

    spec = DataSpec.from_df(df)

    d = spec.to_dict()

    spec_from_d = DataSpec.from_dict(d)

    spec.to_yaml('spec.yaml')
    os.unlink('spec.yaml')

    spec.validate(df)
    spec_from_d.validate(df)


@pytest.mark.parametrize('values, expected_kind',
                         [(['a', 'b', 'c'], 'id'), ([1, 2, 3], 'id'),
                          (['a', 'b', 'c', 'c'], 'categorical'),
                          ([1, 2, 3, 3], 'categorical'),
                          ([1.1, 1.2, 1.3, 1.4], 'numeric')])
def test_infer_kind_id(values, expected_kind):
    df = pd.DataFrame({'column': values})
    spec = DataSpec.from_df(df).to_dict()
    assert spec['column']['kind'] == expected_kind


@pytest.mark.parametrize('values_for_spec, values_for_validate', [
    (np.random.rand(10), [1.1]),
    (['a', 'a', 'b'], ['c']),
    (['a', 'b', 'c'], ['d', 'd']),
])
def test_returns_not_valid_if_out_of_range(values_for_spec,
                                           values_for_validate):
    df = pd.DataFrame({'column': values_for_spec})
    spec = DataSpec.from_df(df)
    assert not spec.validate(pd.DataFrame({'column': values_for_validate}))


# def test_infer_kind_categorical_obj():
#     df = pd.DataFrame({'some_category': })
#     spec = DataSpec.from_df(df).to_dict()
#     assert spec['some_category']['kind'] == 'categorical'
