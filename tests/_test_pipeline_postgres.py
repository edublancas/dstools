import psycopg2
from dstools.pipeline import postgres as pg
from dstools.pipeline.dag import DAG
from dstools import Env

env = Env()

dag = DAG()

pg.CONN = psycopg2.connect(dbname=env.db.dbname, host=env.db.host,
                           user=env.db.user, password=env.db.password)

# script does not create anything
p = pg.PostgresRelation(('schema', 'name', 'table'))
pg.PostgresScript("SELECT * FROM table", p, dag)

# script creates more than one thing, but one declared
p = pg.PostgresRelation(('schema', 'name', 'table'))
pg.PostgresScript("""CREATE TABLE schema.name AS (SELECT * FROM a);
                     CREATE TABLE schema.name2 AS (SELECT * FROM b);
                  """, p, dag)

# product does not match
p = pg.PostgresRelation(('schema', 'name', 'table'))
pg.PostgresScript("CREATE TABLE schema.name2 AS (SELECT * FROM a);", p, dag)

pg.CONN.close()
