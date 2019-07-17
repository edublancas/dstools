import pytest
from dstools.templates.StrictTemplate import StrictTemplate


def test_raises_error_if_missing_parameter():
    with pytest.raises(TypeError):
        StrictTemplate('SELECT * FROM {{table}}').render()


def test_raises_error_if_extra_parameter():
    with pytest.raises(TypeError):
        (StrictTemplate('SELECT * FROM {{table}}')
         .render(table=1, not_a_param=1))
