from pathlib import Path
from dstools.pipeline.identifiers import Placeholder
from jinja2 import Template


def test_string_identifier_initialized_with_path():

    si = Placeholder(Path('/path/to/file')).render()

    assert str(si) == '/path/to/file'


def test_string_identifier_initialized_with_str():

    si = Placeholder('things').render()

    # assert repr(si) == "Placeholder('things')"
    assert str(si) == 'things'


def test_string_identifier_initialized_with_str_with_tags():

    si = Placeholder('{{key}}').render(key='things')

    # assert repr(si) == "Placeholder('things')"
    assert str(si) == 'things'


def test_string_identifier_initialized_with_template_rendered():
    si = Placeholder(Template('{{key}}')).render(key='things')

    assert str(si) == 'things'
