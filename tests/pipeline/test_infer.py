import pytest
from dstools.pipeline.tasks import SQLScript
from dstools.pipeline.products import PostgresRelation

# strings


def test_warns_if_no_product_found(dag):
    p = PostgresRelation(('schema', 'name', 'table'))
    t = SQLScript("SELECT * FROM {{product}}", p, dag, 't')
    t.render()

    with pytest.warns(UserWarning):
        t._validate()


def test_warns_if_creating_two_but_declared_one(dag):
    p = PostgresRelation(('schema', 'name', 'table'))
    t = SQLScript("""CREATE TABLE {{product}} AS (SELECT * FROM a);
                          CREATE TABLE schema.name2 AS (SELECT * FROM b);
                         """, p, dag, 't')
    t.render()

    with pytest.warns(UserWarning):
        t._validate()


def test_warns_if_name_does_not_match(dag):
    p = PostgresRelation(('schema', 'name', 'table'))
    t = SQLScript("""CREATE TABLE schema.name2 AS (SELECT * FROM a);
                          -- {{product}}
                          """, p,
                  dag, 't')
    t.render()

    with pytest.warns(UserWarning):
        t._validate()


# templates

# def test_warns_if_no_product_found_using_template(fake_conn):
#     dag = DAG()

#     p = PostgresRelation(('schema', 'sales', 'table'))

#     with pytest.warns(UserWarning):
#         SQLScript(Template("SELECT * FROM {{name}}"), p, dag, 't',
#                           params=dict(name='customers'))

# comparing metaproduct
