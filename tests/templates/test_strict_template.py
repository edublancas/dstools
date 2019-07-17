from pathlib import Path

import pytest
from dstools.templates.StrictTemplate import StrictTemplate
from dstools.templates import SQLStore


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
