from dstools.pipeline.placeholders import SQLQuerySource


def test_can_parse_sql_docstring():
    source = SQLQuerySource('/* docstring */ SELECT * FROM customers')
    assert source.doc == ' docstring '


def test_can_parse_sql_docstring_from_unrendered_template():
    source = SQLQuerySource(
        '/* get data from {{table}} */ SELECT * FROM {{table}}')
    assert source.doc == ' get data from {{table}} '


def test_can_parse_sql_docstring_from_rendered_template():
    source = SQLQuerySource(
        '/* get data from {{table}} */ SELECT * FROM {{table}}')
    source.render({'table': 'my_table'})
    assert source.doc == ' get data from my_table '
