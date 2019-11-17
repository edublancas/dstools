# TODO: these tests need clean up, is a merge from two files since
# StringPlaceholder was removed and its interface was implemented directly
# in StrictTemplate
from pathlib import Path
import tempfile

import pytest
from dstools.templates.StrictTemplate import StrictTemplate
from dstools.templates import SQLStore
from jinja2 import Template, Environment, FileSystemLoader, StrictUndefined


def test_verify_if_strict_template_is_literal():
    assert not StrictTemplate('no need for rendering').needs_render


def test_verify_if_strict_template_needs_render():
    assert StrictTemplate('I need {{params}}').needs_render


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


def test_string_identifier_initialized_with_path():

    si = StrictTemplate(Path('/path/to/file'), load_if_path=False).render({})

    assert str(si) == '/path/to/file'


def test_string_identifier_initialized_with_str():

    si = StrictTemplate('things').render({})

    # assert repr(si) == "StringPlaceholder('things')"
    assert str(si) == 'things'


def test_string_identifier_initialized_with_str_with_tags():

    si = StrictTemplate('{{key}}').render(params=dict(key='things'))

    # assert repr(si) == "StringPlaceholder('things')"
    assert str(si) == 'things'


def test_string_identifier_initialized_with_template_raises_error():

    with pytest.raises(ValueError):
        StrictTemplate(Template('{{key}}')).render(params=dict(key='things'))


def test_string_identifier_initialized_with_template_from_env():

    tmp = tempfile.mkdtemp()

    Path(tmp, 'template.sql').write_text('{{key}}')

    env = Environment(loader=FileSystemLoader(tmp), undefined=StrictUndefined)

    template = env.get_template('template.sql')

    si = StrictTemplate(template).render(params=dict(key='things'))

    assert str(si) == 'things'
