"""
If Task B has Task A as a dependency, it means that the
Product of A should be used by B in some way (e.g. Task A produces a table
and Task B pivots it), placeholders help avoid redundancy when building tasks,
if you declare that Product A is "schema"."table", the use of placeholders
prevents "schema"."table" to be explicitely declared in B, since B depends
on A, this information from A is passed to B. Placeholders fill that purpose,
they are placeholders that will be filled at rendering time so that
parameteters are only declared once.

They serve a second, more advanced use case. It is recommended for Tasks to
have no parameters and be fully declared by specifying their code, product
and upstream dependencies. However, there is one use case where parameters
are useful: batch processing and parallelization. For example, if we are
operating on a 10-year databse, a single task might take too long, but we
could split the data in 1-year chunks and process them in parallel, in such
use case we could create 10 task instances, each one with a different year
parameters and process them independently. So, apart from upstream and product
placeholders, arbitrary parameters can also be placeholders.

These classes are not intended to be used by the end user, since Task and
Product objects create placeholders from strings.
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
    StringPlaceholders templated strings (using StrictTemplates) that store
    its rendered version in the same object so it can later be accesed,
    if a pathlib.Path object is used as source, it is casted to str. If the
    contents of the file represent the placeholder's content, use
    ClientCodePlaceholder instead
    """

    def __init__(self, source):
        if isinstance(source, Path):
            source = str(source)

        self._source = StrictTemplate(source)
        self._rendered_value = None

        # if source is literal, rendering without params should work, this
        # allows this template to be used without having to render the dag
        # first
        if self._source.is_literal:
            self.render({})

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

    @property
    def doc_short(self):
        return None

    @property
    def loc(self):
        return None


class ClientCodePlaceholder(StringPlaceholder):
    """
    An object that represents client code, if a pathlib.Path object is passed,
    its contents are read and interpreted as the placeholder's content

    Notes
    -----
    This is really just a StrictTemplate object that stores its rendered
    version in the same object and raises an Exception if attempted. It also
    passes some of its attributes
    """

    def __init__(self, source):
        # the only difference between this and the original placeholder
        # is how they treat pathlib.Path
        self._source = StrictTemplate(source)
        self._rendered_value = None

        # if source is literal, rendering without params should work, this
        # allows this template to be used without having to render the dag
        # first
        if self._source.is_literal:
            self.render({})

    @property
    def doc(self):
        return self._source.doc

    @property
    def doc_short(self):
        return self._source.doc_short

    @property
    def loc(self):
        return str(self._source.path)

    @property
    def path(self):
        return self._source.path


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

        # if source is literal, rendering without params should work, this
        # allows this template to be used without having to render the dag
        # first
        if self._source.is_literal:
            self.render({})

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
        _, self._source_lineno = inspect.getsourcelines(source)

        self._params = None
        self._loc = inspect.getsourcefile(source)

    def render(self, params, **kwargs):
        # FIXME: we need **kwargs for compatibility, but they are not used,
        # think what's the best thing to do
        # TODO: verify that params match function signature
        self._params = params

    def __repr__(self):
        return 'Placeholder({})'.format(self._source.raw)

    def __str__(self):
        return self._source_as_str

    @property
    def doc(self):
        return self._source.__doc__

    @property
    def doc_short(self):
        if self.doc is not None:
            return self.doc.split('\n')[0]
        else:
            return None

    @property
    def loc(self):
        return '{}:{}'.format(self._loc, self._source_lineno)


class LiteralCodePlaceholder:
    def __init__(self, source):
        if isinstance(source, Path):
            self._source = source.read_text()
            self._path = source
        else:
            self._source = source
            self._path = None

    def render(self, params, **kwargs):
        pass

    def __str__(self):
        return self._source

    @property
    def doc(self):
        return ''

    @property
    def doc_short(self):
        return ''

    @property
    def loc(self):
        return ''

    @property
    def path(self):
        return self._path
