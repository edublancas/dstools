import pytest
from dstools.pipeline import postgres as pg
from dstools.pipeline.dag import DAG

# strings


def test_warns_if_no_product_found(fake_conn):
    dag = DAG()

    p = pg.PostgresRelation(('schema', 'name', 'table'))
    t = pg.PostgresScript("SELECT * FROM {{product}}", p, dag, 't')
    t.render()

    with pytest.warns(UserWarning):
        t._validate()


def test_warns_if_creating_two_but_declared_one(fake_conn):
    dag = DAG()

    p = pg.PostgresRelation(('schema', 'name', 'table'))
    t = pg.PostgresScript("""CREATE TABLE {{product}} AS (SELECT * FROM a);
                          CREATE TABLE schema.name2 AS (SELECT * FROM b);
                         """, p, dag, 't')
    t.render()

    with pytest.warns(UserWarning):
        t._validate()


def test_warns_if_name_does_not_match(fake_conn):
    dag = DAG()
    p = pg.PostgresRelation(('schema', 'name', 'table'))
    t = pg.PostgresScript("""CREATE TABLE schema.name2 AS (SELECT * FROM a);
                          -- {{product}}
                          """, p,
                          dag, 't')
    t.render()

    with pytest.warns(UserWarning):
        t._validate()


# templates

# def test_warns_if_no_product_found_using_template(fake_conn):
#     dag = DAG()

#     p = pg.PostgresRelation(('schema', 'sales', 'table'))

#     with pytest.warns(UserWarning):
#         pg.PostgresScript(Template("SELECT * FROM {{name}}"), p, dag, 't',
#                           params=dict(name='customers'))

# comparing metaproduct
