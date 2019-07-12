import tempfile
from pathlib import Path

from dstools.pipeline.identifiers import ClientCode


def test_client_code_init_with_str():

    ci = ClientCode('SELECT * FROM {{name}}')
    ci.render(dict(name='table'))

    # repr(ci)
    assert str(ci) == 'SELECT * FROM table'


def test_client_code_init_with_path():
    _, filename = tempfile.mkstemp()
    filename = Path(filename)
    filename.write_text('SELECT * FROM {{name}}')

    ci = ClientCode(filename)
    ci.render(dict(name='table'))

    # repr(ci)
    assert str(ci) == 'SELECT * FROM table'


def test_client_code_renders():
    ci = ClientCode('SELECT * FROM {{name}}')
    ci.render(dict(name='table'))

    # repr(ci)
    assert str(ci) == 'SELECT * FROM table'
