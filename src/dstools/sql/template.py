import re


def _validate_token(token):
    '''
        Validate token before inserting it into an SQL script
    '''
    pattern = re.compile('^(\w|\.)+$')
    result = pattern.match(token)
    if result is None:
        raise ValueError('"{}" is not a valid argument'.format(token))


class SQLTemplate:
    '''
        Create SQL templates from strings:
            t = SQLTemplate('SELECT {col} FROM {table} LIMIT 1000')
            t.substitute(col='parcel_id', table='crime')
            'SELECT parcel_id FROM crime_ LIMIT 1000'

        The substitute method will take care of token validation. Tokens
         are not allowed to contain spaces to avoid SQL injection
    '''
    def __init__(self, sql):
        self.sql = sql

    def substitute(self, **kwargs):
        for k, v in kwargs.items():
            try:
                _validate_token(v)
            except:
                raise (ValueError("Value '{}' in key '{}' contains forbidden"
                                  " characters."
                                  .format(v, k)))

        return self.sql.format(**kwargs)
