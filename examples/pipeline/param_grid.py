
from datetime import date

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from dstools.pipeline.tasks import PythonCallable
from dstools.pipeline.products import File
from dstools.pipeline import DAG
from dstools.pipeline.util import ParamGrid, Interval
from dstools.pipeline.helpers import make_task_group


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


dag = DAG()
product = File('get_data_{{index}}.parquet')

start_date = date(year=2010, month=1, day=1)
end_date = date(year=2019, month=6, day=1)
delta = relativedelta(years=1)

params_array = ParamGrid(
    {'dates': Interval(start_date, end_date, delta)}).zip()

make_task_group(task_class=PythonCallable,
                task_kwargs={'source': get_data, 'product': product},
                dag=dag,
                name_prefix='get_data',
                params_array=params_array)


dag.render()

dag.plot()

dag.build()
