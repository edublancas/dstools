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


def open_db_conn():
    try:
        p = Path('~', '.auth', 'postgres-dstools.json').expanduser()

        with open(p) as f:
            db = json.load(f)
    except FileNotFoundError:
        # running on travis
        db = json.loads(environ['DB_CREDENTIALS'])
    else:
        # running locally
        global CONN

        if CONN is None:
            conn = psycopg2.connect(dbname=db['dbname'],
                                    host=db['host'],
                                    user=db['user'],
                                    password=db['password'])
            CONN = conn

    return CONN
