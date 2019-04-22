import shutil
import os
import pytest
from pathlib import Path
import tempfile


def _path_to_tests():
    return Path(__file__).absolute().parent


@pytest.fixture(scope='session')
def path_to_tests():
    return _path_to_tests()


@pytest.fixture()
def tmp_example_directory():
    """Move to examples/pipeline/
    """
    old = os.getcwd()
    path = _path_to_tests() / '..' / 'examples' / 'pipeline'
    tmp = Path(tempfile.mkdtemp()) / 'content'

    # we have to add extra folder content/, otherwise copytree complains
    shutil.copytree(path, tmp)
    os.chdir(tmp)

    yield tmp

    os.chdir(old)


@pytest.fixture(scope='session')
def move_to_sample():
    old = os.getcwd()
    new = _path_to_tests() / 'assets' / 'sample'
    os.chdir(new)

    yield new

    os.chdir(old)


@pytest.fixture(scope='session')
def move_to_module():
    old = os.getcwd()
    new = _path_to_tests() / 'assets' / 'sample' / 'src' / 'pkg' / 'module'
    os.chdir(new)

    yield new

    os.chdir(old)


@pytest.fixture(scope='session')
def path_to_source_code_file():
    return (_path_to_tests() / 'assets' / 'sample' /
            'src' / 'pkg' / 'module' / 'functions.py')


@pytest.fixture(scope='session')
def path_to_env():
    return _path_to_tests() / 'assets' / 'sample' / 'env.yaml'
