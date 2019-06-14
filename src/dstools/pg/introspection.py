"""
Querying postgres schema metadata
"""
from jinja2 import Template


def _count_column_names_in_relations(schema, relations):
    """
    """
    sql = Template("""

    SELECT column_name, count(*)
    FROM information_schema.columns
    WHERE table_schema = '{{schema}}'
    AND table_name  IN ({{relations|join(', ') }})
    group by column_name
    having count(*) > 1

    """)

    return sql.render(schema=schema, relations=[f"'{r}'" for r in relations])


def count_column_names_in_relations(conn, schema, relations):
    sql = _count_column_names_in_relations(schema, relations)

    cursor = conn.cursor()
    cursor.execute(sql)
    res = cursor.fetchall()
    cursor.close()

    return res


def shared_columns_in_relations(conn, schema, relations):
    counts = count_column_names_in_relations(conn, schema, relations)

    return set([name for name, count in counts])