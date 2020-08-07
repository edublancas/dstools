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
