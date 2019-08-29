"""
If a Task B is said to have Task A as a dependencies, it means that the
Product of A should be used by B in some way (e.g. Task A produces a table
and Task B pivots it), placeholders help avoid redundancy when building tasks,
if you declare that Product A is "schema"."table", the use of placeholders
prevents "schema"."table" to be explicitely declared in B, since B depends
on A, information from A is passed to B.

They are not intended to be used by the user, since Task and Product objects
implicitely initialize them from strings
"""
from pathlib import Path
import inspect

from dstools.templates.StrictTemplate import StrictTemplate

class TemplatedPlaceholder:
    """
    There are two types of placeholders, templated strings (whose
    parameteters are rendered using jinja templates and then executed) and
    native Python code (a callable object), which has no render logic, but
    still needs to offer the same API for compatibility, this class only
    helps identify which placeholders are from the first class since in some
    execution points, they need to be treated differently (e.g. for templated
    placeholders it is possible to check whether any passed parameter is
    unused at rendering time, since python code does not have a render step,
    this is not possible)

    """
    pass


class StringPlaceholder(TemplatedPlaceholder):
    """
    StringPlaceholders are StrictTemplates that store its rendered version
    in the same object so it can later be accesed
    """

    def __init__(self, source):
        if isinstance(source, Path):
            source = str(source)

        self._source = StrictTemplate(source)
        self._rendered_value = None

    @property
    def _rendered(self):
        if self._rendered_value is None:
            raise RuntimeError('Tried to read {} {} without '
                               'rendering first'
                               .format(type(self).__name__,
                                       repr(self)))

        return self._rendered_value

    def render(self, params, **kwargs):
        self._rendered_value = self._source.render(params, **kwargs)
        return self

    def __repr__(self):
        return 'Placeholder({})'.format(self._source.raw)

    def __str__(self):
        return self._rendered


class ClientCodePlaceholder(StringPlaceholder):
    """An object that represents client code

    Notes
    -----
    This is really just a StrictTemplate object that stores its rendered
    version in the same object and raises an Exception if attempted
    """

    def __init__(self, source):
        # the only difference between this and the original placeholder
        # is how they treat pathlib.Path
        self._source = StrictTemplate(source)
        self._rendered_value = None


class SQLRelationPlaceholder(TemplatedPlaceholder):
    """An identifier that represents a database relation (table or view)
    """

    def __init__(self, source):
        if len(source) != 3:
            raise ValueError('{} must be initialized with 3 elements, '
                             'got: {}'
                             .format(type(self).__name__, len(source)))

        schema, name, kind = source

        if schema is None:
            raise ValueError('schema cannot be None')

        if name is None:
            raise ValueError('name cannot be None')

        if kind not in ('view', 'table'):
            raise ValueError('kind must be one of ["view", "table"] '
                             'got "{}"'.format(kind))

        self._source = StrictTemplate(name)
        self._rendered_value = None

        self._kind = kind
        self._schema = schema

    @property
    def schema(self):
        return self._schema

    @property
    def name(self):
        if self._rendered_value is None:
            raise RuntimeError('Tried to read {} {} without '
                               'rendering first'
                               .format(type(self).__name__, repr(self)))

        return self._rendered_value

    @property
    def kind(self):
        return self._kind

    # FIXME: THIS SHOULD ONLY BE HERE IF POSTGRES

    def _validate_rendered_value(self):
        value = self._rendered_value
        if len(value) > 63:
            url = ('https://www.postgresql.org/docs/current/'
                   'sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS')
            raise ValueError(f'"{value}" exceeds maximum length of 63 '
                             f' (length is {len(value)}), '
                             f'see: {url}')

    @property
    def _rendered(self):
        if self._rendered_value is None:
            raise RuntimeError('Tried to read {} {} without '
                               'rendering first'
                               .format(type(self).__name__, repr(self)))

        if self.schema:
            return f'"{self.schema}"."{self._rendered_value}"'
        else:
            return f'"{self._rendered_value}"'

    def render(self, params, **kwargs):
        self._rendered_value = self._source.render(params, **kwargs)
        self._validate_rendered_value()
        return self

    def __str__(self):
        return self._rendered

    def __repr__(self):
        return ('Placeholder("{}"."{}")'
                .format(self.schema, self._source.raw, self.kind))


class PythonCodePlaceholder:

    def __init__(self, source):
        if not callable(source):
            raise TypeError(f'{type(self).__name__} must be initialized'
                            'with a Python callable, got '
                            f'"{type(source).__name__}"')

        self._source = source
        self._source_as_str = inspect.getsource(source)

        self._params = None

    def render(self, params, **kwargs):
        # FIXME: we need **kwargs for compatibility, but they are not used,
        # think what's the best thing to do
        # TODO: verify that params match function signature
        self._params = params

    def __repr__(self):
        return 'Placeholder({})'.format(self._source.raw)

    def __str__(self):
        return self._source_as_str
