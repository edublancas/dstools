import json
from os import environ
import shutil
import os
import pytest
from pathlib import Path
import tempfile
from dstools.pipeline.clients import SQLAlchemyClient
from dstools.pipeline.tasks import SQLScript
from dstools.pipeline.products import PostgresRelation
from dstools.pipeline.dag import DAG


def _path_to_tests():
    return Path(__file__).absolute().parent


@pytest.fixture(scope='session')
def path_to_tests():
    return _path_to_tests()


@pytest.fixture()
def tmp_directory():
    old = os.getcwd()
    tmp = tempfile.mkdtemp()
    os.chdir(tmp)

    yield tmp

    os.chdir(old)


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


@pytest.fixture(scope='session')
def path_to_assets():
    return _path_to_tests() / 'assets'


def _load_db_credentials():
    try:
        p = Path('~', '.auth', 'postgres-dstools.json').expanduser()

        with open(p) as f:
            db = json.load(f)
    except FileNotFoundError:
        db = json.loads(environ['DB_CREDENTIALS'])

    return db


@pytest.fixture(scope='session')
def pg_client():
    db = _load_db_credentials()

    client = SQLAlchemyClient(db['uri'])

    yield client

    client.close()


@pytest.fixture(scope='session')
def fake_conn():
    o = object()

    yield o


@pytest.fixture()
def dag():
    dag = DAG()

    db = _load_db_credentials()

    client = SQLAlchemyClient(db['uri'])

    dag.clients[SQLScript] = client
    dag.clients[PostgresRelation] = client

    yield dag

    client.close()
