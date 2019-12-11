from datetime import date

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from dstools.pipeline.tasks import PythonCallable, TaskGroup
from dstools.pipeline.tasks.TaskGroup import TaskGroup
from dstools.pipeline.products import File
from dstools.pipeline import DAG
from dstools.pipeline.util import ParamGrid, Interval


def get_data(product, dates, index):
    """
    Dummy code, in reality this would usually be a Task that pulls data
    from a database
    """
    dates_series = pd.date_range(start=dates[0], end=dates[1],
                                 closed='left', freq='D')
    values = np.random.rand(dates_series.shape[0])
    df = pd.DataFrame({'dates': dates_series, 'values': values})
    df.to_parquet(str(product))


start_date = date(year=2010, month=1, day=1)
end_date = date(year=2019, month=6, day=1)
delta = relativedelta(years=1)

name_prefix = 'get_data_'
# product must have the {{index}} placeholder
product = File('get_data_{{index}}.parquet')

dag = DAG()

tasks_all = []

from copy import deepcopy

for i, params in enumerate(ParamGrid({'dates': Interval(start_date, end_date, delta)}).zip()):
    params['index'] = i
    t = PythonCallable(get_data, deepcopy(product), dag, name_prefix+str(i),
                       params=params)
    tasks_all.append(t)

TaskGroup(tasks_all, True, 'get_data')

dag.render()

dag.plot()

dag.build()
