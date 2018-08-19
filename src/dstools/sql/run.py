from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


def run(sql, uri='auto'):
    '''
        Run a SQL statement on a db. This function does not return
        the SQL results and is intended to be used to submit jobs only.
    '''
    if uri == 'auto':
        from dstools.params import db_uri
        uri = db_uri

    en = create_engine(uri, poolclass=NullPool)
    with en.begin() as con:
        con.execute(sql)
    en.dispose()


def run_in_parallel(sqls, workers='auto'):
    '''
        Run SQL queries in parallel. sqls must be an interator.
        This function does not return the SQL results and is intended to be
        used to submit jobs only.
    '''
    # This first implementation is inefficient since it does not use
    # sqlalchemy pooling capabilities.
    from concurrent import futures

    MAX_WORKERS = 10
    if workers == 'auto':
        workers = min(len(sqls), MAX_WORKERS)
    else:
        workers = workers if workers <= MAX_WORKERS else MAX_WORKERS

    with futures.ThreadPoolExecutor(workers) as executor:
        res = executor.map(run, sqls)

    # iterate over the results to catchs any exceptions
    for r in res:
        r
