"""
%load_ext autoreload
%autoreload 2
"""
import atexit
import logging
from pathlib import Path

import psycopg2

from dstools.pipeline.products import File
from dstools.pipeline.tasks import (BashCommand, BashScript, PythonCallable)
from dstools.pipeline import postgres as pg
from dstools.pipeline.dag import DAG
from dstools import testing
from dstools import Env
from dstools import mkfilename
from train import train_and_save_report
from download_dataset import download_dataset


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


env = Env()
home = env.path.home

pg.CONN = psycopg2.connect(dbname=env.db.dbname, host=env.db.host,
                           user=env.db.user, password=env.db.password)


@atexit.register
def close_conn():
    if not pg.CONN.closed:
        print('closing connection...')
        pg.CONN.close()


dag = DAG()

get_data_task = BashScript(home / 'get_data.sh',
                           File(env.path.input / 'raw' / 'red.csv'),
                           dag)


red_path = env.path.input / 'sample' / 'red.csv'
red_task = BashCommand('csvsql --db {db} --tables red --insert {path} '
                       '--overwrite',
                       pg.PostgresRelation(('public', 'red', 'table')),
                       dag,
                       params=dict(db=env.db.uri, path=red_path),)
red_task.set_upstream(get_data_task)

white_path = Path(env.path.input / 'sample' / 'white.csv')
white_task = BashCommand('csvsql --db {db} --tables white --insert {path} '
                         '--overwrite',
                         pg.PostgresRelation(('public', 'white', 'table')),
                         dag,
                         params=dict(db=env.db.uri, path=white_path))
white_task.set_upstream(get_data_task)


wine_task = pg.PostgresScript(home / 'sql' / 'create_wine.sql',
                              pg.PostgresRelation(('public', 'wine', 'table')),
                              dag)
wine_task.set_upstream(white_task)
wine_task.set_upstream(red_task)


dataset_task = pg.PostgresScript(home / 'sql' / 'create_dataset.sql',
                                 pg.PostgresRelation(
                                     ('public', 'dataset', 'table')),
                                 dag)
dataset_task.set_upstream(wine_task)


training_task = pg.PostgresScript(home / 'sql' / 'create_training.sql',
                                  pg.PostgresRelation(
                                      ('public', 'training', 'table')),
                                  dag)
training_task.set_upstream(dataset_task)


testing_table = pg.PostgresRelation(('public', 'testing', 'table'))
testing_table.tests = [testing.Postgres.no_nas_in_column('label')]
testing_task = pg.PostgresScript(home / 'sql' / 'create_testing.sql',
                                 testing_table, dag)

testing_task.set_upstream(dataset_task)


path_to_dataset = env.path.input / 'datasets'
kwargs = dict(path_to_dataset=path_to_dataset, conn=pg.CONN)
dataset_task = PythonCallable(download_dataset,
                              File(path_to_dataset / 'training.csv'),
                              dag, kwargs=kwargs)
dataset_task.set_upstream(training_task)
dataset_task.set_upstream(testing_task)


path_to_report = env.path.input / 'reports' / mkfilename('report.txt')
kwargs = dict(path_to_dataset=path_to_dataset,
              path_to_report=path_to_report)
train_task = PythonCallable(train_and_save_report, File(
    path_to_report), dag, kwargs=kwargs)
train_task.set_upstream(dataset_task)
train_task.set_upstream(dataset_task)

dag.plot()

# dag.build()
