import pytest
from dstools.templates.StrictTemplate import StrictTemplate

t = StrictTemplate("""

SELECT * FROM {{table}}

""")


def test_raises_error_if_missing_parameter():
    with pytest.raises(TypeError):
        t.render()


def test_raises_error_if_extra_parameter():
    with pytest.raises(TypeError):
        t.render(table=1, not_a_param=1)
