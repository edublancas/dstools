import pytest
import pandas as pd
from sklearn.datasets import load_wine

from dstools.guard import InputGuard, ColumnGuard


def to_df(data):
    df = pd.DataFrame(data['data'])
    df.columns = data['feature_names']
    df['target'] = data['target']
    return df


@pytest.fixture
def df():
    return to_df(load_wine())


def test_passthrough(df):
    pre = InputGuard()
    pre.fit(df)
    out = pre.transform(df)

    assert df is out


def test_extra_columns_strict(df):
    pre = InputGuard(strict=True)
    pre.fit(df)
    df['extra_col'] = 1

    with pytest.raises(ValueError) as excinfo:
        pre.transform(df)

    assert 'Columns during fit were' in str(excinfo.value)


def test_extra_columns_no_strict(df):
    pre = InputGuard(strict=False)
    pre.fit(df)
    df['extra_col'] = 1
    pre.transform(df)


def test_simple_case(df):
    # cols = df.columns.to_list()

    pre = InputGuard()
    pre.fit(df)

    df['target'] = df['target'].astype('category')

    with pytest.raises(ValueError) as excinfo:
        pre.transform(df)

    assert '"target". Expected int64, got category' in str(excinfo.value)


# TODO: test category with switched orders
# what happens if we pass a single observation? as type categorical wont have all info to know all values
# can to_categorical give different results? different order?


def test_column_guard():
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [1, 2, 3]})

    df_extra = df.copy()
    df_extra['z'] = 1

    ColumnGuard().fit_transform(df)

    # passing extra cols but not strict
    guard = ColumnGuard(strict=False)
    res = guard.fit(df).transform(df_extra)
    assert list(res) == ['x', 'y']

    guard = ColumnGuard(strict=True)

    with pytest.raises(ValueError):
        guard.fit(df).transform(df_extra)