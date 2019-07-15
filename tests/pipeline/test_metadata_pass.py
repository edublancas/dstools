"""
Testing that upstream tasks metadata is available
"""
import subprocess
from pathlib import Path

from dstools.pipeline.dag import DAG
from dstools.pipeline.tasks import BashCommand
from dstools.pipeline.products import File
from dstools.pipeline import postgres as pg


def test_passing_upstream_and_product_in_bashcommand(tmp_directory):
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    kwargs = {'stderr': subprocess.PIPE,
              'stdout': subprocess.PIPE,
              'shell': True}

    ta = BashCommand(('echo a > {{product}} '), File(fa), dag,
                     'ta', {}, kwargs, False)
    tb = BashCommand(('cat {{upstream["ta"]}} > {{product}}'
                     '&& echo b >> {{product}} '), File(fb), dag,
                     'tb', {}, kwargs, False)
    tc = BashCommand(('cat {{upstream["tb"]}} > {{product}} '
                     '&& echo c >> {{product}}'), File(fc), dag,
                     'tc', {}, kwargs, False)

    ta >> tb >> tc

    dag.build()

    assert fc.read_text() == 'a\nb\nc\n'


def test_passing_upstream_and_product_in_postgres(pg_client):
    dag = DAG()

    conn = pg_client.raw_connection()
    cur = conn.cursor()
    cur.execute('drop table if exists public.series;')
    conn.commit()
    conn.close()

    ta_t = """begin;
              drop table if exists {{product}};
              create table {{product}} as
              select * from generate_series(0, 15) as n;
              commit;"""
    ta_rel = pg.PostgresRelation(('public', 'series', 'table'))
    ta = pg.PostgresScript(ta_t, ta_rel, dag, 'ta')

    dag.build()

    assert ta_rel.exists()
