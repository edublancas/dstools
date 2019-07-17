from pathlib import Path

from dstools.templates.StrictTemplate import StrictTemplate
from dstools.templates import SQLStore

import pytest
from jinja2 import Environment, FileSystemLoader, StrictUndefined


def test_raises_error_if_missing_parameter():
    with pytest.raises(TypeError):
        StrictTemplate('SELECT * FROM {{table}}').render()


def test_raises_error_if_extra_parameter():
    with pytest.raises(TypeError):
        (StrictTemplate('SELECT * FROM {{table}}')
         .render(table=1, not_a_param=1))


def test_can_create_template_loaded_from_sql_store(tmp_directory):
    Path(tmp_directory, 'template.sql').write_text('{{file}}')

    store = SQLStore(None, tmp_directory)

    t = store.get_template('template.sql')

    assert t.render({'file': 'some file'})

    tt = StrictTemplate(t)

    assert tt.render({'file': 'some file'})


def test_strict_templates_initialized_from_jinja_template(path_to_assets):
    path = str(path_to_assets / 'templates')
    env = Environment(loader=FileSystemLoader(path), undefined=StrictUndefined)
    st = StrictTemplate(env.get_template('template.sql'))
    assert st.render({'file': 1})


def test_strict_templates_raises_error_if_not_strictundefined(path_to_assets):
    path = str(path_to_assets / 'templates')
    env = Environment(loader=FileSystemLoader(path))

    with pytest.raises(ValueError):
        StrictTemplate(env.get_template('template.sql'))


def test_strict_templates_initialized_from_strict_template(path_to_assets):
    path = str(path_to_assets / 'templates')
    env = Environment(loader=FileSystemLoader(path), undefined=StrictUndefined)
    st = StrictTemplate(env.get_template('template.sql'))
    assert StrictTemplate(st).render({'file': 1})
