from dstools.util import load_yaml

config = load_yaml('config.yaml')

db_uri = ('{dialect}://{user}:{password}@{host}:{port}}/{database}'
          .format(**config['db']))
