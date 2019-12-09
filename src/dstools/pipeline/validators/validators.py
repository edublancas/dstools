from functools import partial, wraps


class Assert:

    def __init__(self):
        self.error_messages = []

    def __call__(self, expression, error_message):
        if not expression:
            self.error_messages.append(error_message)

    def __len__(self):
        return len(self.error_messages)

    def __iter__(self):
        for msg in self.error_messages:
            yield msg

    def __repr__(self):
        return 'Assert oject with {} error messages'.format(len(self))

    def __str__(self):
        if not self.error_messages:
            return 'No errors found'
        elif len(self.error_messages) == 1:
            return '1 error found: {}'.format(self.error_messages[0])
        else:
            return ('{} Errors found: \n * {}'
                    .format(len(self.error_messages),
                            '\n * '.join(self.error_messages)))


def validator(fn):

    # TODO: verify fn signature

    @wraps(fn)
    def wrapped(**kwargs):
        if 'assert_' in kwargs:
            raise TypeError('Do not include the assert_ parameter in '
                            'validator functions')

        if 'data' in kwargs:
            raise TypeError('Do not include the data parameter in '
                            'validator functions')

        return partial(fn, **kwargs)

    return wrapped


@validator
def validate_schema(assert_, data, schema, ignore_extra_cols=True):
    """Check if a data frame complies with a schema
    """
    cols = set(data.columns)
    expected = set(schema)
    missing = expected - cols

    msg = ('Invalid columns. Missing columns: {missing}.'
           .format(missing=missing))

    # validate column names
    if ignore_extra_cols:
        assert_(not missing, msg)
    else:
        unexpected = cols - expected

        msg_w_unexpected = ('{msg} Unexpected columns {unexpected}'
                            .format(msg=msg, unexpected=unexpected))

        assert_(cols == expected, msg_w_unexpected)

    # validate column types (as many as you can)
    dtypes = data.dtypes.astype(str).to_dict()

    for name, dtype in dtypes.items():
        expected = schema.get(name)

        if expected is not None:
            msg = ('Wrong dtype for column "{name}". '
                   'Expected: "{expected}". Got: "{dtype}"'
                   .format(name=name, expected=expected, dtype=dtype))
            assert_(dtype == expected, msg)

    return assert_


def data_frame_validator(df, validators):
    """

    Examples
    --------
    >>> import pandas as pd
    >>> import numpy as np
    >>> df = pd.DataFrame({'x': np.random.rand(10), 'y': np.random.rand(10)})
    >>> data_frame_validator(df,
    ...                     [validate_schema(schema={'x': 'int', 'z': 'int'})])
    """
    assert_ = Assert()

    for validator in validators:
        validator(assert_=assert_, data=df)

    if len(assert_):
        raise AssertionError(str(assert_))
