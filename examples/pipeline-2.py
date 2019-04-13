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
