# inspired by airflow
import subprocess
from pathlib import Path
import time
import logging
from datetime import datetime

from dstools import Env

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_TASKS = []
_NON_END_TASKS = []


def build_all():
    for task in _TASKS:
        if task.is_end_task:
            task.build()


class Task:
    """A task represents a unit of work
    """

    def __init__(self, source_code, product):
        self._upstream = []
        self._already_checked = False

        self._product = product
        self._source_code = source_code
        self._logger = logging.getLogger(__name__)

        product.task = self

        _TASKS.append(self)

    @property
    def product(self):
        return self._product

    @property
    def is_end_task(self):
        return self not in _NON_END_TASKS

    @property
    def is_root_task(self):
        return not len(self._upstream)

    @property
    def source_code(self):
        return self._source_code

    @property
    def upstream(self):
        return self._upstream

    def run(self):
        raise NotImplementedError('You have to implement this method')

    def set_upstream(self, task):
        self._upstream.append(task)

        if task not in _NON_END_TASKS:
            _NON_END_TASKS.append(task)

    def build(self):

        # first get upstreams up to date
        for task in self._upstream:
            task.build()

        # bring this task up to date
        if not self._already_checked:
            self._build()

    def _build(self):

        # TODO: check if product exists, if not, just run, if yes, check
        # data and code dependencies

        outdated_data_deps = self.product.outdated_data_dependencies()
        outdated_code_dep = self.product.outdated_code_dependency()

        self._logger.info(f'-----\nChecking {repr(self)}....')

        if outdated_data_deps:
            self._logger.info(f'Outdated data deps...')
        else:
            self._logger.info(f'Up-to-date data deps...')

        if outdated_code_dep:
            self._logger.info(f'Outdated code dep...')
        else:
            self._logger.info(f'Up-to-date code dep...')

        if outdated_data_deps or outdated_code_dep:
            self._logger.info(f'Running {repr(self)}')

            # execute code...
            self.run()

            # TODO: should check if job ran successfully, if not, stop execution

            # update metadata
            self.product.timestamp = datetime.now().timestamp()
            self.product.stored_source_code = self.source_code
            self.product.save_metadata()

        else:
            self._logger.info('Up to date data deps, no need to run'
                              f' {repr(self)}')

        self._logger.info(f'-----\n')

        self._already_checked = True

    def __repr__(self):
        return f'{type(self).__name__}: {self.source_code}'


class Product:
    """A product is a change that the task triggers
    """

    def __init__(self, identifier):
        self._identifier = identifier

        self.timestamp, self.stored_source_code = self.fetch_metadata()

    @property
    def identifier(self):
        return self._identifier

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def stored_source_code(self):
        return self._stored_source_code

    @property
    def task(self):
        return self._task

    @task.setter
    def task(self, value):
        self._task = value

    @timestamp.setter
    def timestamp(self, value):
        self._timestamp = value

    @stored_source_code.setter
    def stored_source_code(self, value):
        self._stored_source_code = value

    def outdated_data_dependencies(self):
        outdated = any([up.product.timestamp > self.timestamp
                        for up in self.task.upstream])
        return outdated

    def outdated_code_dependency(self):
        return self.stored_source_code != self.task.source_code

    def fetch_metadata(self):
        raise NotImplementedError('You have to implement this method')

    def save_metadata(self):
        raise NotImplementedError('You have to implement this method')


class PostgresTask(Task):

    def run(self):
        print(f'Running: {self.source_code}')
        time.sleep(5)


class PostgresRelation(Product):
    pass


class File(Product):
    def __init__(self, identifier):
        self._identifier = identifier
        self._path_to_file = Path(self.identifier)
        self._path_to_stored_source_code = Path(str(self.path_to_file)
                                                + '.source')

        self.timestamp, self.stored_source_code = self.fetch_metadata()

    @property
    def path_to_file(self):
        return self._path_to_file

    @property
    def path_to_stored_source_code(self):
        return self._path_to_stored_source_code

    def fetch_metadata(self):
        if self.path_to_file.exists():
            timestamp = self.path_to_file.stat().st_mtime
        else:
            timestamp = None

        if self.path_to_stored_source_code.exists():
            stored_source_code = self.path_to_stored_source_code.read_text()
        else:
            stored_source_code = None

        return timestamp, stored_source_code

    def save_metadata(self):
        # timestamp automatically updates when the file is saved...

        self.path_to_stored_source_code.write_text(self.stored_source_code)


class BashTask(Task):
    def run(self):
        subprocess.call(self.source_code.split(' '))


env = Env()

red = File(Path(env.path.input / 'raw' / 'red.csv'))

# NOTE: running like this obscures the actual source code inside the file
# which might change. maybe I should pass the path to the file directlt
# or the actual source code, but then we will be limited to running things
# like bash SOMETHING (with no parameters). Maybe provided both options,
# to the user, for .sh files, makes sense to use the content as source code
# for commands that use libraries (i.e. psql), makes sense to use the
# command as source code
t1 = BashTask('bash get_data.sh', red)

red_sample = File(Path(env.path.input / 'sample' / 'red.csv'))
t2 = BashTask('python sample.py', red_sample)
t2.set_upstream(t1)

t2.build()

# TODO: have to decide how to deal with deleted files (timestamp is None)
# maybe I need another state, like a not found. in such case I always
# have to run. maybe add an exists() method so that the product checks if
# exists, if not, build parents and then bulild node

# get users

sql_get_users_v1 = """
CREATE TABLE users_ios AS
SELECT * FROM warehouse.users
WHERE platform = 'ios'
"""

get_users = PostgresTask(sql_get_users_v1, 'get users')
get_users._metadata = dict(timestamp=datetime(year=2019, month=4, day=8),
                           stored_source_code=sql_get_users_v1)

# get users target

sql_users_target_v1 = """
CREATE TABLE users_target AS
SELECT * FROM users_ios
WHERE account_is_open=true and is_active=true
"""

sql_users_target_v2 = """
CREATE TABLE users_target AS
SELECT * FROM users_ios
WHERE account_is_open=true
"""

get_users_target = PostgresTask(sql_users_target_v2, 'get users target')
get_users_target._metadata = dict(timestamp=datetime(year=2019, month=4,
                                                     day=9),
                                  stored_source_code=sql_users_target_v1)
get_users_target.set_upstream(get_users)

# get bots

sql_get_bots_v1 = """
CREATE TABLE bots AS
SELECT * from users_target
WHERE is_bot=true
"""

get_bots = PostgresTask(sql_get_bots_v1, 'get bots')
get_bots._metadata = dict(timestamp=datetime(year=2019, month=4, day=10),
                          stored_source_code=sql_get_bots_v1)
get_bots.set_upstream(get_users_target)

# get humans

get_humans_v1 = """
CREATE TABLE humans AS
SELECT * from users_target
WHERE is_bot=false
LIMIT 1000
"""

get_humans_v2 = """
CREATE TABLE humans AS
SELECT * from users_target
WHERE is_bot=false
LIMIT 2000
"""

get_humans = PostgresTask(get_humans_v2, 'get humans')
get_humans._metadata = dict(timestamp=datetime(year=2019, month=4, day=10),
                            stored_source_code=get_humans_v1)
get_humans.set_upstream(get_users_target)

# build dataset

build_dataset_v1 = """
CREATE TABLE dataset AS
SELECT *, 1 AS label FROM bots
UNION ALL
SELECT *, 0 AS label FROM humans
"""

build_dataset_v2 = """
CREATE TABLE dataset AS
SELECT *, 2 AS label FROM bots WHERE bot_class = 'money-fraud'
UNION ALL
SELECT *, 1 AS label FROM bots WHERE bot_class = 'political'
UNION ALL
SELECT *, 0 AS label FROM humans
"""

build_dataset = PostgresTask(build_dataset_v2, 'build dataset')

build_dataset._metadata = dict(timestamp=datetime(year=2019, month=4, day=11),
                               stored_source_code=build_dataset_v1)
build_dataset.set_upstream(get_humans)
build_dataset.set_upstream(get_bots)

build_features_v1 = """
CREATE TABLE dataset_w_features AS
SELECT
    likes_last_week > 100 AS liker
    average_session_last_month_minutes > 60 AS long_sessions
FROM dataset
"""

build_features = PostgresTask(build_features_v1, 'build features')

build_features._metadata = dict(timestamp=datetime(year=2019, month=4, day=11),
                                stored_source_code=build_features_v1)
build_features.set_upstream(build_dataset)

# maybe have a DAG concept, don't really like this thing. maybe a global
# list of tasks created
build_all()

# maybe support this syntax?
# t1 >> t2 >> t3 >> t4
# t3 >> t5
