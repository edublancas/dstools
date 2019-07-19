import pytest
from dstools.pipeline.tasks.Params import Params

def test_can_get_first():
    p = Params()

    p['a'] = 0
    p['b'] = 1

    assert p.first == 0


def test_shows_warning_if_unused_parameters():
    p = Params()

    p['a'] = 0
    p['b'] = 1

    with pytest.warns(UserWarning):
        with p:
            p['a']


