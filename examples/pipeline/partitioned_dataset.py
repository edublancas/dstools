"""
Example of parallel processing using partitioned datasets
"""
import pyarrow as pa
from pyarrow import parquet as pq
from scipy import stats
import numpy as np
import pandas as pd

from dstools.pipeline import DAG
from dstools.pipeline.tasks import PythonCallable
from dstools.pipeline.products import File

dag = DAG()


def make_data(product):
    dfs = []

    for i in range(1000):
        df = pd.DataFrame({'x': np.random.normal(0, 1, size=1000)})
        df['i'] = i
        df['partition'] = i % 4
        dfs.append(df)

    df = pd.concat(dfs)

    table = pa.Table.from_pandas(df)

    pq.write_to_dataset(table, str(product), partition_cols=['partition'])


t1 = PythonCallable(make_data, File('observations'), dag,
                    name='make_data')


def process_data(upstream, product):
    df = pd.read_parquet(str(upstream['make_data']))
    pvalues = df.groupby('i').x.apply(lambda x: stats.normaltest(x)[1])
    pvalues = pd.DataFrame({'pvalue': pvalues})
    pvalues.to_parquet(str(product))


t2 = PythonCallable(process_data, File('results'), dag,
                    name='process_data')

t1 >> t2

dag.build()

from copy import copy
from glob import glob
from pathlib import Path

# need to instantiate new Files with the raw argument + partitions
t2.product._identifier._source.raw

# need to modify upstream so that upstream['make_data'] returns a different
# thing for each partitioned task, should return a path to a unique partition
# other upstream dependencies should stay intact
product = t2.upstream.to_dict()['make_data'].product._identifier._source.raw

# This is a good opportunity to think about the upstream semantics
# tasks have upstream tasks, which are declared using t1 >> t2, but
# tasks need access to their upstream tasks products, that's why
# they are passed an upstream parameter, but we are currently passing
# the actual upstream tasks as a parameter which seems too much
# since they only need the product, but passing only the product and calling
# it "upstream" might be confusing and calling it "upstream_products"
# seems too verbose. Passing the Task object makes object relations risky
# since any given downstream task has access to all attributes in their
# upstream task. For this partition things I can create a TaskDummy
# that is a stripped down version of a task, then I can think if this should
# be the standard behavior
# UPDATE: upstream inside task.params is indeed the product (not the task)
# I forgot I made this change, it happens in Task._render_product,
# upstream in self.params only passes the product, have to clean up the
# rendering flow in Task, is confusing


partitions = glob('{}/*'.format(product))
names = [Path(p).name for p in partitions]

t2s = [PythonCallable(t2._code._source, File(p), t2.dag, name=t2.name+'_'+n)
       for p, n in zip(partitions, names)]

# delete original t2 from the dag


# then I need to rewire things, this gets interesting since t1 should still
# be the only upstream for all copies of t2 but t2.upstream['make_data']
# will return different things, i think it's an ok behavior, the plot
# should be one t1 pointing to 4 t2s

# I have to substitute the upstream tasks right before Task.run executes
# since they might depend on the rendering logic, also, modifying them
# before execution will modify the DAG structure causing the plot
# function to plot a different thing, the idea is for the DAG to be the same
# but only to inject the upstream values, while they still point to the same,
# original upstream task.

# I have to clean up the task execution workflow, but works like this:
# - dag.render() makes sure all templated parameters are expanded
# - task.build() is executed on each task
# - some conditions are verified and if needed, the task actually runs via task.run()
# the logic has to come right before task.run() is executed.. but this won't
# work with tasks that need to render params in the source code like SQL

# Maybe a better solution is to just keep task.upstream to return the right
# thing (the original upstream task) to keep the DAG structure, but when
# upstream is passed for rendering, values should be substitued for dummy
# tasks, this has to happen in the task._render_product function


class TaskDummy:
    pass

    def __str__(self):
        return str(self.product)
