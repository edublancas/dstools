"""
Example of parallel processing using partitioned datasets
"""
import time
import pyarrow as pa
from pyarrow import parquet as pq
from scipy import stats
import numpy as np
import pandas as pd

from pathlib import Path

from dstools.pipeline import DAG
from dstools.pipeline.tasks import PythonCallable, Null
from dstools.pipeline.products import File
from dstools.pipeline import executors

dag = DAG(executor=executors.Parallel)


def make_data(product):
    df = pd.DataFrame({'x': np.random.normal(0, 1, size=1000000)})
    df['partition'] = (df.index % 4).astype(int)
    df['group'] = (df.index % 2).astype(int)

    table = pa.Table.from_pandas(df)

    pq.write_to_dataset(table, str(product), partition_cols=['partition'])


t1 = PythonCallable(make_data, File('observations'), dag,
                    name='make_data')


def process_data(upstream, product):
    time.sleep(30)
    df = pd.read_parquet(str(upstream.first))
    pvalues = df.groupby('group').x.apply(lambda x: stats.normaltest(x)[1])
    pvalues = pd.DataFrame({'pvalue': pvalues})
    pvalues.to_parquet(str(product))


product = t1.product
format_ = 'partition={i}'
n = 4

# make sure output is a File
assert isinstance(product, File)

# instantiate null tasks
nulls = [Null(product=File(Path(str(t1.product), format_.format(i=i))),
              dag=dag,
              name=format_.format(i=i)) for i in range(n)]

Path('results').mkdir(exist_ok=True)

tasks = [PythonCallable(process_data, File('results/results_{i}'.format(i=i)), dag,
                        name='process_data_{i}'.format(i=i))
         for i in range(n)]

for null, task in zip(nulls, tasks):
    t1 >> null >> task


dag.build(force=True)

# UPDATE: upstream inside task.params is indeed the product (not the task)
# I forgot I made this change, it happens in Task._render_product,
# upstream in self.params only passes the product, have to clean up the
# rendering flow in Task, is confusing

# I have to clean up the task execution workflow, but works like this:
# - dag.render() makes sure all templated parameters are expanded
# - task.build() is executed on each task
# - some conditions are verified and if needed, the task actually runs via task.run()
# the logic has to come right before task.run() is executed.. but this won't
# work with tasks that need to render params in the source code like SQL
