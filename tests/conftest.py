import os

import pytest


@pytest.fixture
def tmp_directory(tmp_path):
    current = os.getcwd()

    os.chdir(tmp_path)

    yield

    os.chdir(current)
