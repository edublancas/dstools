import sqlite3

import pytest
import pandas as pd
import numpy as np

from dstools import profiling


@pytest.fixture
def conn():
    conn = sqlite3.connect('my.db')

    yield conn

    conn.close()


def test_simple(tmp_directory, conn):
    pd.DataFrame({'x': np.arange(100)}).to_sql('numbers', conn)

    sql = profiling.simple(relation='numbers',
                           mappings={'x': ['min', 'max']},
                           alias={'x': 'new_x'})

    df = pd.read_sql(sql, conn)

    assert df.iloc[0].to_dict() == {
        'min_new_x': 0,
        'max_new_x': 99,
        'n_rows': 100
    }


def test_agg(tmp_directory, conn):
    data = pd.DataFrame({'x': np.arange(100)})
    data['id'] = 0
    data.loc[:50, 'id'] = 1
    data.to_sql('numbers', conn)

    sql = profiling.agg(relation='numbers',
                        mappings={'x': ['count']},
                        alias={'x': 'new_x'},
                        group_by='id',
                        agg=['min', 'max'])

    df = pd.read_sql(sql, conn)

    assert df.iloc[0].to_dict() == {
        'min_count_new_x': 49,
        'min_n_rows': 49,
        'max_count_new_x': 51,
        'max_n_rows': 51
    }
