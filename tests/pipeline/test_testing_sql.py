from pathlib import Path

import numpy as np
import pandas as pd
from dstools.pipeline.clients import SQLAlchemyClient
from dstools import testing


def test_can_check_nulls(tmp_directory):
    client = SQLAlchemyClient('sqlite:///' + str(Path(tmp_directory, 'db.db')))

    df = pd.DataFrame({'no_nas': [1, 2, 1], 'nas': [1, np.nan, 1]})
    df.to_sql('my_table', client.engine)

    assert not testing.sql.nulls_in_columns(client, ['no_nas'], 'my_table')
    assert testing.sql.nulls_in_columns(client, ['nas'], 'my_table')
    assert testing.sql.nulls_in_columns(client, ['no_nas', 'nas'],
                                        'my_table')


def test_can_check_distinct(tmp_directory):
    client = SQLAlchemyClient('sqlite:///' + str(Path(tmp_directory, 'db.db')))

    df = pd.DataFrame({'no_nas': [1, 2, 1], 'nas': [1, np.nan, 1]})
    df.to_sql('my_table', client.engine)

    assert (testing.sql.distinct_values_in_column(client,
                                                  'no_nas',
                                                  'my_table') == {1, 2})

    assert (testing.sql.distinct_values_in_column(client,
                                                  'nas',
                                                  'my_table') == {1.0, None})


def test_can_check_duplicates(tmp_directory):
    client = SQLAlchemyClient('sqlite:///' + str(Path(tmp_directory, 'db.db')))

    df = pd.DataFrame({'duplicates': [1, 1], 'no_duplicates': [1, 2]})
    df.to_sql('my_table', client.engine)

    assert not testing.sql.duplicates_in_column(client, 'no_duplicates',
                                                'my_table')
    assert testing.sql.duplicates_in_column(client, 'duplicates', 'my_table')


def test_can_check_range(tmp_directory):
    client = SQLAlchemyClient('sqlite:///' + str(Path(tmp_directory, 'db.db')))

    df = pd.DataFrame({'x': [1, 2, 3, 4, 5, 1000]})
    df.to_sql('my_table', client.engine)

    assert testing.sql.range_in_column(client, 'x', 'my_table') == (1, 1000)
