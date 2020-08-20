import pytest
from dstools.caster import CategoricalCaster
import pandas as pd
from pandas.core.dtypes.dtypes import CategoricalDtype


def test_cast_categorical():
    train = pd.DataFrame({'cat': ['c', 'b', 'a', 'a', 'a', 'b']})
    test = pd.DataFrame({'cat': ['c']})

    caster = CategoricalCaster(cols=['cat'])
    caster.fit(train)

    out = caster.transform(test)

    assert out.cat.dtype == CategoricalDtype(['a', 'b', 'c'], ordered=False)


@pytest.mark.parametrize('method', ['fit', 'fit_transform'])
def test_raise_if_missing_and_strict(method):
    train = pd.DataFrame({'cat': ['c', 'b', 'a', 'a', 'a', 'b']})
    caster = CategoricalCaster(cols=['cat', 'another_cat'])

    with pytest.raises(ValueError) as excinfo:
        getattr(caster, method)(train)

    expected = ('CategoricalCaster was initialized with column "another_cat", '
                'but it does not appear in the data')
    assert expected in str(excinfo.value)


@pytest.mark.parametrize('method', ['fit', 'fit_transform'])
def test_skip_if_missing_and_not_strict(method):
    train = pd.DataFrame({'cat': ['c', 'b', 'a', 'a', 'a', 'b']})
    caster = CategoricalCaster(cols=['cat', 'another_cat'], strict=False)

    getattr(caster, method)(train)


@pytest.mark.parametrize('cast_int_to_float, expected', [
    [True, 'float64'],
    [False, 'int64'],
])
def test_int_are_casted_to_float(cast_int_to_float, expected):
    train = pd.DataFrame({'num': [1, 2, 3], 'non_num': ['a', 'b', 'c']})
    caster = CategoricalCaster(cols=[], cast_int_to_float=cast_int_to_float)
    caster.fit(train)

    out = caster.transform(train)
    dtypes = out.dtypes.to_dict()
    assert str(dtypes['num']) == expected
    assert str(dtypes['non_num']) == 'object'


def test_predefined_dtypes():
    train = pd.DataFrame({'num': ['a', 'b', 'c']})
    dtype = CategoricalDtype(categories=['a', 'c'], ordered=False)

    caster = CategoricalCaster(cols=['num'], predefined_dtypes={'num': dtype})
    caster.fit(train)
    out = caster.transform(train)
    assert out['num'].dtype == dtype
