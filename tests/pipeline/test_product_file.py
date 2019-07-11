from jinja2 import Template
from pathlib import Path
from dstools.pipeline.products import File


def test_file_initialized_with_str():
    f = File('/path/to/file')
    f.render({})
    assert str(f) == '/path/to/file'


def test_file_initialized_with_path():
    f = File(Path('/path/to/file'))
    f.render({})
    assert str(f) == '/path/to/file'


def test_file_initialized_with_template():
    f = File(Template('/path/to/{{name}}'))
    f.render(params=dict(name='file'))
    assert str(f) == '/path/to/file'
