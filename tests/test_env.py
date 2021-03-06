import pytest
from dstools.env.env import _get_name, Env


def test_can_create_env_from_dict(cleanup_env):
    e = Env.from_dict({'a': 1})
    assert e.a == 1


def test_assigns_default_name():
    assert _get_name('path/to/env.yaml') == 'root'


def test_can_extract_name():
    assert _get_name('path/to/env.my_name.yaml') == 'my_name'


def test_raises_error_if_wrong_format():
    with pytest.raises(ValueError):
        _get_name('path/to/wrong.my_name.yaml')


def test_can_instantiate_env_if_located_in_current_dir(move_to_sample,
                                                       cleanup_env):
    Env()


def test_can_instantiate_env_if_located_in_child_dir(move_to_module,
                                                     cleanup_env):
    Env()
