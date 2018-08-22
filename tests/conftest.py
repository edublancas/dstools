import os
import pytest
from pathlib import Path


@pytest.fixture(scope='session')
def path_to_tests():
    return Path(__file__).absolute().parent


@pytest.fixture(scope='session')
def move_to_sample():
    old = os.getcwd()
    new = path_to_tests() / 'assets' / 'sample'
    os.chdir(new)

    yield new

    os.chdir(old)


@pytest.fixture(scope='session')
def move_to_module():
    old = os.getcwd()
    new = path_to_tests() / 'assets' / 'sample' / 'src' / 'pkg' / 'module'
    os.chdir(new)

    yield new

    os.chdir(old)


@pytest.fixture(scope='session')
def path_to_source_code():
    return (path_to_tests() / 'assets' / 'sample' /
            'src' / 'pkg' / 'module' / 'functions.py')


@pytest.fixture(scope='session')
def path_to_env():
    return path_to_tests() / 'assets' / 'sample' / 'env.yaml'
