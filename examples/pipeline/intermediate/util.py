import json
from os import environ
from pathlib import Path


def load_db_uri():
    try:
        p = Path('~', '.auth', 'postgres-dstools.json').expanduser()

        with open(p) as f:
            db = json.load(f)
    except FileNotFoundError:
        db = json.loads(environ['DB_CREDENTIALS'])

    return db['uri']
