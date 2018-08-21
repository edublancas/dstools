import pytest
from dstools.env import _get_name


def test_assigns_default_name():
    assert _get_name('path/to/env.yaml') == 'default'


def test_can_extract_name():
    assert _get_name('path/to/env.my_name.yaml') == 'my_name'


def test_raises_error_if_wrong_format():
    with pytest.raises(ValueError):
        _get_name('path/to/wrong.my_name.yaml')
