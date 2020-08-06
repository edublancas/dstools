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
