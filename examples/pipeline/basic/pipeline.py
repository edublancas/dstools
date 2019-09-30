# This example shows the most basic usage of the `dstools.pipeline` module

# +
from pathlib import Path
import tempfile

import pandas as pd
from IPython.display import Image

from dstools.pipeline import DAG
from dstools.pipeline.tasks import PythonCallable
from dstools.pipeline.products import File
# -

# A `DAG` is a workflow representation, it is a collection of `Tasks` that are executed in a given order

dag = DAG(name='my pipeline')


# Let's build the first tasks, they will just download some data, this pipeline is entirely declared in a single file to simplify things, a real pipeline will likely be splitted among several files.
#
# Tasks can be a lot of things (bash scripts, SQL scripts, etc), for this example they will be Python functions.

# +
# these function pull the data and save it, the product
# parameter is required in every Task. Each Task is assumed
# to save to disk, this can be many things, from a local
# file to a table in a remote db system

def get_red_wine_data(product):
    df = pd.read_csv('http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv',
                     sep=';', index_col=False)
    # producg is a File type so you have to cast it to a str
    df.to_csv(str(product))

def get_white_wine_data(product):
    df = pd.read_csv('http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-white.csv',
                    sep=';', index_col=False)
    df.to_csv(str(product))

def concat_data(upstream, product):
    red = pd.read_csv(str(upstream['red']))
    white = pd.read_csv(str(upstream['white']))
    df =  pd.concat([red, white])
    df.to_csv(str(product))



# +
# create a temporary directory to store data
tmp_dir = Path(tempfile.mkdtemp())

# convert our functions to Task objects, note
# that the product is a File object, which means
# this functions will create a file in the local filesystem
red_task = PythonCallable(get_red_wine_data,
                          product=File(tmp_dir / 'red.csv'),
                          dag=dag)

white_task = PythonCallable(get_white_wine_data,
                            product=File(tmp_dir / 'white.csv'),
                           dag=dag)

concat_task = PythonCallable(concat_data,
                            product=File(tmp_dir / 'all.csv'),
                            dag=dag)

# concat task depends on red and white task 
(red_task + white_task) >> concat_task
# -

path_to_image = dag.plot(open_image=False)
display(Image(filename=path_to_image))

# list all tasks in the dag
list(dag)

# get a task
dag['red']

# build the dag, it returns a summary of elapsed time on tasks
report = dag.build()
report

# dags avoid reduntant computations, if the code has not changed or any
# upstream dependency has not changed they won't run, the plot will
# show which tasks are up-to-date
path_to_image = dag.plot(open_image=False)
display(Image(filename=path_to_image))

# you can inspect any step getting the task
df = pd.read_csv(str(dag['red']))

df.head()


