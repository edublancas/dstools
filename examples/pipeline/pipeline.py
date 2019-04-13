"""
%load_ext autoreload
%autoreload 2
"""

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

conn = psycopg2.connect(dbname=env.db.dbname, host=env.db.host,
                        user=env.db.user, password=env.db.password)

# TODO: be able to specify more than one product?
red = File(Path(env.path.input / 'raw' / 'red.csv'))
red_source = env.path.home / 'get_data.sh'
t1 = BashScript(red_source, red)

red_sample_path = Path(env.path.input / 'sample' / 'red.csv')
red_sample = File(red_sample_path)
red_sample_source = env.path.home / 'sample.py'
t2 = PythonScript(red_sample_source, red_sample)
t2.set_upstream(t1)

# TODO: simplify postgres interface, maybe let the user build an identifier
# from a tuple, also the Relation/Identifier naming is kind of confusing.
# maybe merge identifier and relation. Create PostgresTable and PostgresView
# that will be Product subclasses and take a (schema, name) tuple as identifier
# or schema.name string, and use the current identifier objects under
# the hood
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
id_ = pg.PostgresIdentifierTable('public', 'red')
rel = pg.PostgresRelation(id_, conn)
cmd = f'csvsql --db $DB --tables red --insert "{red_sample_path}"  --overwrite'
t3 = BashCommand(cmd, rel)
t3.set_upstream(t2)

white_sample_path = Path(env.path.input / 'sample' / 'white.csv')
id_ = pg.PostgresIdentifierTable('public', 'white')
rel = pg.PostgresRelation(id_, conn)
cmd = f'csvsql --db $DB --tables white --insert "{white_sample_path}"  --overwrite'
t4 = BashCommand(cmd, rel)
t4.set_upstream(t2)


id_ = pg.PostgresIdentifierView('public', 'wine')
rel = pg.PostgresRelation(id_, conn)
source = env.path.home / 'sql' / 'create_wine_view.sql'
t5 = pg.PostgresScript(source, rel, conn)
t5.set_upstream(t4)


id_ = pg.PostgresIdentifierTable('public', 'dataset')
rel = pg.PostgresRelation(id_, conn)
source = env.path.home / 'sql' / 'create_dataset.sql'
t6 = pg.PostgresScript(source, rel, conn)
t6.set_upstream(t5)

id_ = pg.PostgresIdentifierTable('public', 'training')
rel = pg.PostgresRelation(id_, conn)
source = env.path.home / 'sql' / 'create_training.sql'
t7 = pg.PostgresScript(source, rel, conn)
t7.set_upstream(t6)

id_ = pg.PostgresIdentifierTable('public', 'testing')
rel = pg.PostgresRelation(id_, conn)
source = env.path.home / 'sql' / 'create_testing.sql'
t8 = pg.PostgresScript(source, rel, conn)
t8.set_upstream(t6)

build_all()
conn.close()
