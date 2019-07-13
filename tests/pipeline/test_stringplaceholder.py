import tempfile

import pytest
from pathlib import Path
from dstools.pipeline.placeholders import StringPlaceholder
from jinja2 import Template, Environment, FileSystemLoader


def test_string_identifier_initialized_with_path():

    si = StringPlaceholder(Path('/path/to/file')).render({})

    assert str(si) == '/path/to/file'


def test_string_identifier_initialized_with_str():

    si = StringPlaceholder('things').render({})

    # assert repr(si) == "StringPlaceholder('things')"
    assert str(si) == 'things'


def test_string_identifier_initialized_with_str_with_tags():

    si = StringPlaceholder('{{key}}').render(params=dict(key='things'))

    # assert repr(si) == "StringPlaceholder('things')"
    assert str(si) == 'things'


def test_string_identifier_initialized_with_template_raises_error():

    with pytest.raises(ValueError):
        StringPlaceholder(Template('{{key}}')).render(params=dict(key='things'))


def test_string_identifier_initialized_with_template_from_env():

    tmp = tempfile.mkdtemp()

    Path(tmp, 'template.sql').write_text('{{key}}')

    env = Environment(loader=FileSystemLoader(tmp))

    template = env.get_template('template.sql')

    si = StringPlaceholder(template).render(params=dict(key='things'))

    assert str(si) == 'things'
