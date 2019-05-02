import json
from os import environ
from pathlib import Path
import atexit
import psycopg2

CONN = None


@atexit.register
def close_conn():
    if CONN is not None:
        if not CONN.closed:
            print('closing connection...')
            CONN.close()


def load_db_credentials():
    try:
        p = Path('~', '.auth', 'postgres-dstools.json').expanduser()

        with open(p) as f:
            db = json.load(f)
    except FileNotFoundError:
        db = json.loads(environ['DB_CREDENTIALS'])

    return db


def open_db_conn():
    db = load_db_credentials()

    global CONN

    if CONN is None:
        conn = psycopg2.connect(dbname=db['dbname'],
                                host=db['host'],
                                user=db['user'],
                                password=db['password'])
        CONN = conn

    return CONN
