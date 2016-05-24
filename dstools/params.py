from dstools.util import load_yaml

try:
    config = load_yaml('config.yaml')
except Exception, e:
    pass

try:
    db_uri = ('{dialect}://{user}:{password}@{host}:{port}}/{database}'
              .format(**config['db']))
except Exception, e:
    pass
