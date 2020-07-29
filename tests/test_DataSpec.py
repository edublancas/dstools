import os
import pytest
from dstools.spec.DataSpec import DataSpec, to_df
import pandas as pd
from sklearn import datasets


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


@pytest.mark.parametrize('values, expected_kind', [
    (['a', 'b', 'c'], 'id'),
    (['a', 'b', 'c', 'c'], 'categorical'),
    ([1, 2, 3, 3], 'categorical'),
    ([1.1, 1.2, 1.3, 1.4], 'numeric')
])
def test_infer_kind_id(values, expected_kind):
    df = pd.DataFrame({'column': values})
    spec = DataSpec.from_df(df).to_dict()
    assert spec['column']['kind'] == expected_kind


# def test_infer_kind_categorical_obj():
#     df = pd.DataFrame({'some_category': })
#     spec = DataSpec.from_df(df).to_dict()
#     assert spec['some_category']['kind'] == 'categorical'
