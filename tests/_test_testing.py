import psycopg2
from dstools.pipeline import postgres as pg
from dstools.pipeline.dag import DAG
from dstools import Env
from dstools import testing

env = Env()

dag = DAG()

pg.CONN = psycopg2.connect(dbname=env.db.dbname, host=env.db.host,
                           user=env.db.user, password=env.db.password)

# script does not create anything
p = pg.PostgresRelation(('public', 'wine', 'table'))


fn = testing.Postgres.no_nas_in_column('alcohol')
fn(p)

pg.CONN.close()
