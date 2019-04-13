# inspired by airflow
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_TASKS = []
_NON_END_TASKS = []


def build_all():
    for task in _TASKS:
        if task.is_end_task:
            task.build()


class Task:

    def __init__(self, source_code, identifier):
        self._upstream = []
        self._already_checked = False

        self._identifier = identifier
        self._source_code = source_code
        self._logger = logging.getLogger(__name__)

        _TASKS.append(self)

        self._metadata = self.fetch_metdata(self.identifier)

    @property
    def identifier(self):
        return self._identifier

    @property
    def is_end_task(self):
        return self not in _NON_END_TASKS

    @property
    def is_root_task(self):
        return not len(self._upstream)

    @property
    def timestamp(self):
        # TODO make sure metadata is always up to date
        return self._metadata['timestamp']

    @property
    def stored_source_code(self):
        return self._metadata['stored_source_code']

    @property
    def source_code(self):
        return self._source_code

    def fetch_metdata(self, identifier):
        raise NotImplementedError('You have to implement this method')

    def save_metdata(self, identifier, new_metadata):
        raise NotImplementedError('You have to implement this method')

    def run(self, identifier, source_code):
        raise NotImplementedError('You have to implement this method')

    def outdated_data_dependencies(self):
        outdated = any([up.timestamp > self.timestamp
                        for up in self._upstream])
        return outdated

    def outdated_code_dependency(self):
        return self.stored_source_code != self.source_code

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
        outdated_data_deps = self.outdated_data_dependencies()
        outdated_code_dep = self.outdated_code_dependency()

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
            self.run(self.identifier, self.source_code)

            # TODO: should check if job ran successfully, if not, stop execution

            # update metadata in the current object
            self._metadata['timestamp'] = datetime.now()
            self._metadata['stored_source_code'] = self.source_code

            # and in the external task
            self.save_metdata(self.identifier, self._metadata)
        else:
            self._logger.info('Up to date data deps, no need to run'
                              f' {repr(self)}')

        self._logger.info(f'-----\n')

        self._already_checked = True

    def __repr__(self):
        return f'{type(self).__name__}: {self.identifier}'


class PostgresTask(Task):

    def fetch_metdata(self, identifier):
        pass

    def save_metdata(self, identifier, new_metadata):
        pass

    def run(self, identifier, source_code):
        print(f'Running: {source_code}')
        time.sleep(5)


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
