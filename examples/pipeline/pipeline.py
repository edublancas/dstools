"""
%load_ext autoreload
%autoreload 2
"""
import atexit
import logging
from pathlib import Path

import psycopg2

from dstools.pipeline.products import File
from dstools.pipeline.tasks import BashCommand, BashScript, PythonScript
from dstools.pipeline import build_all
from dstools.pipeline import postgres as pg
from dstools import Env


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


env = Env()
home = env.path.home

pg.CONN = psycopg2.connect(dbname=env.db.dbname, host=env.db.host,
                           user=env.db.user, password=env.db.password)


@atexit.register
def close_conn():
    print('closing connection...')
    pg.CONN.close()


# TODO: be able to specify more than one product?
get_data_task = BashScript(home / 'get_data.sh',
                           File(env.path.input / 'raw' / 'red.csv'))


red_path = env.path.input / 'sample' / 'red.csv'
sample_task = PythonScript(home / 'sample.py',
                           File(red_path))
sample_task.set_upstream(get_data_task)

# TODO: also have to rollback automatically when something goes wrong
# when runinng SQL code
# TODO: double check that the Product is matches what is expected in the
# code using regex
# TODO: convention over configuration, we can infer most parameters if we infer
# them, for example, if sql files follow a naming convention such as
# create_[kind]_[schema].[name], we can infer identifier, to we only
# need to pass the path to the file
# TODO: function to plot topological order
# TODO: write motivation in the readme file
# TODO: migrate this pipeline to the ds-template project
# NOTE: how to avoid having to carry the conn variable on each element?
# TODO: reset state after runing build all
# TODO: check exit status of each script, raise exception if any of them
# failed
# NOTE: parametrizing commands like this is unsafe, and they get logged
# exposing credentials (if they are passed via format args)
# TODO: create postgres mixing to share the CONN behavior
red_src = (f'csvsql --db {env.db.uri} --tables red --insert '
           f'{red_path}  --overwrite')
red_task = BashCommand(red_src,
                       pg.PostgresRelation(('public', 'red', 'table')))
red_task.set_upstream(sample_task)

white_path = Path(env.path.input / 'sample' / 'white.csv')
white_src = (f'csvsql --db {env.db.uri} --tables white --insert '
             f'{white_path}  --overwrite')
white_step = BashCommand(white_src,
                         pg.PostgresRelation(('public', 'white', 'table')))
white_step.set_upstream(sample_task)


wine_task = pg.PostgresScript(home / 'sql' / 'create_wine_view.sql',
                              pg.PostgresRelation(('public', 'wine', 'view')),
                              pg.CONN)
wine_task.set_upstream(white_step)


dataset_task = pg.PostgresScript(home / 'sql' / 'create_dataset.sql',
                                 pg.PostgresRelation(
                                     ('public', 'dataset', 'table')),
                                 pg.CONN)
dataset_task.set_upstream(wine_task)


training_task = pg.PostgresScript(home / 'sql' / 'create_training.sql',
                                  pg.PostgresRelation(
                                      ('public', 'training', 'table')),
                                  pg.CONN)
training_task.set_upstream(dataset_task)


testing_task = pg.PostgresScript(home / 'sql' / 'create_testing.sql',
                                 pg.PostgresRelation(
                                     ('public', 'testing', 'table')),
                                 pg.CONN)
testing_task.set_upstream(dataset_task)

build_all()
pg.CONN.close()
