import tempfile
from pathlib import Path

from dstools.pipeline.placeholders import StringPlaceholder


def test_client_code_init_with_str():

    ci = StringPlaceholder('SELECT * FROM {{name}}')
    ci.render(dict(name='table'))

    # repr(ci)
    assert str(ci) == 'SELECT * FROM table'


# no longer valid, StringPlaceholder does not load the contents
# def test_client_code_init_with_path():
#     _, filename = tempfile.mkstemp()
#     filename = Path(filename)
#     filename.write_text('SELECT * FROM {{name}}')

#     ci = StringPlaceholder(filename)
#     ci.render(dict(name='table'))

#     # repr(ci)
#     assert str(ci) == 'SELECT * FROM table'


def test_client_code_renders():
    ci = StringPlaceholder('SELECT * FROM {{name}}')
    ci.render(dict(name='table'))

    # repr(ci)
    assert str(ci) == 'SELECT * FROM table'
