from dstools.util import load_yaml

config = load_yaml('config.yaml')

db_uri = ('{dialect}://{user}:{password}@{host}:5432/{database}'
          .format(**config['db']))
