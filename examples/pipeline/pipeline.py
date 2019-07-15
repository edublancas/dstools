"""
%load_ext autoreload
%autoreload 2
"""
import logging
from pathlib import Path

from dstools.pipeline.products import File
from dstools.pipeline.tasks import (BashCommand, BashScript, PythonCallable)
from dstools.pipeline import postgres as pg
from dstools.pipeline.dag import DAG
from dstools.pipeline.clients import SQLAlchemyClient
from dstools import testing
from dstools import Env
from dstools import mkfilename

from train import train_and_save_report
import util
from download_dataset import download_dataset
from sample import sample


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


env = Env()
home = env.path.home
path_to_sample = env.path.input / 'sample'

(env.path.input / 'raw').mkdir(exist_ok=True, parents=True)
path_to_sample.mkdir(exist_ok=True)

uri = util.load_db_uri()
pg.CONN = SQLAlchemyClient(uri)

dag = DAG()


get_data = BashCommand((home / 'get_data.sh').read_text(),
                       (File(env.path.input / 'raw' / 'red.csv'),
                        File(env.path.input / 'raw' / 'white.csv'),
                        File(env.path.input / 'raw' / 'names')),
                       dag, 'get_data',
                       split_source_code=False)

sample = PythonCallable(sample,
                        (File(env.path.input / 'sample' / 'red.csv'),
                         File(env.path.input / 'sample' / 'white.csv')),
                        dag, 'sample')
get_data >> sample

red_task = BashCommand(('csvsql --db {{uri}} --tables {{product.name}} --insert {{upstream["sample"][0]}} '
                        '--overwrite'),
                       pg.PostgresRelation(('public', 'red', 'table')),
                       dag, 'red',
                       params=dict(uri=uri),
                       split_source_code=False)
sample >> red_task

white_task = BashCommand(('csvsql --db {{uri}} --tables {{product.name}} --insert {{upstream["sample"][1]}} '
                          '--overwrite'),
                         pg.PostgresRelation(('public', 'white', 'table')),
                         dag, 'white',
                         params=dict(uri=uri),
                         split_source_code=False)
sample >> white_task


wine_task = pg.PostgresScript(home / 'sql' / 'create_wine.sql',
                              pg.PostgresRelation(('public', 'wine', 'table')),
                              dag, 'wine')
(red_task + white_task) >> wine_task


dataset_task = pg.PostgresScript(home / 'sql' / 'create_dataset.sql',
                                 pg.PostgresRelation(
                                     ('public', 'dataset', 'table')),
                                 dag, 'dataset')
wine_task >> dataset_task


training_task = pg.PostgresScript(home / 'sql' / 'create_training.sql',
                                  pg.PostgresRelation(
                                      ('public', 'training', 'table')),
                                  dag, 'training')
dataset_task >> training_task


testing_table = pg.PostgresRelation(('public', 'testing', 'table'))
testing_table.tests = [testing.Postgres.no_nas_in_column('label')]
testing_task = pg.PostgresScript(home / 'sql' / 'create_testing.sql',
                                 testing_table, dag, 'testing')

dataset_task >> testing_task


path_to_dataset = env.path.input / 'datasets'
params = dict(path_to_dataset=path_to_dataset)
download_task = PythonCallable(download_dataset,
                               File(path_to_dataset / 'training.csv'),
                               dag, 'download', params=params)
training_task >> download_task
testing_task >> download_task


path_to_report = env.path.input / 'reports' / mkfilename('report.txt')
params = dict(path_to_dataset=path_to_dataset,
              path_to_report=path_to_report)
train_task = PythonCallable(train_and_save_report, File(
    path_to_report), dag, 'train', params=params)
download_task >> train_task

# dag.plot()

stats = dag.build()

# print(str(stats))

pg.CONN.close()
