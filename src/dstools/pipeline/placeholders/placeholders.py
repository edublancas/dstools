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
import abc
import warnings
import re
from pathlib import Path
import inspect

from dstools.pipeline.products import Product
from dstools.templates.StrictTemplate import StrictTemplate
from dstools.exceptions import SourceInitializationError
from dstools.sql import infer

# FIXME: move diagnose to here, task might need this as well, since
# validation may involve checking against the product, but we can replace
# this behabior for an after-render validation, and just pass the product
# as parameter maybe from the Task? the task should not do this
# FIXME: remove opt from StrictTemplate.render


"""
Notes: there should be a hierarchy here, source should be the top level
implementation and it should use another class to represent its "value",
which can be either a placeholder or a literal. this value should have
the same interface namely: offer a way to know if rendering is needed
(if its a placeholder or not), we need to get rid of _rendered_value,
as it does not make sense for literals, the way to access the value
should be the same, the only different would be that in Placeholders
this value wont be available until the placeholder is rendered
"""


class FilePlaceholder:
    """
    Placeholders are jinja2 templates that hold their rendered
    values after Placeholder.render is called, they are used in
    source and Product objects to hold values that are be filled
    after a DAG is rendered
    """

    def __init__(self, source):
        self.template = StrictTemplate(source)
        self._value = None

        # FIXME: some sources cannot be literals! query can be but not script
        # it needs the product as placeholder
        # if source is literal, assign the rendered value to the raw value
        if self.template.is_literal:
            # self.render({})
            self._value = str(self.template)

    @property
    def value(self):
        if self._value is None:
            raise RuntimeError('Tried to read {} {} without '
                               'rendering first'
                               .format(type(self).__name__,
                                       repr(self)))

        return self._value

    def render(self, params, **kwargs):
        self._value = self.template.render(params, **kwargs)
        return self

    def __repr__(self):
        return 'Placeholder({})'.format(self.template.raw)

    def __str__(self):
        return str(self.value)

    @property
    def loc(self):
        return self.template.path

    @property
    def safe(self):
        if self._value is None:
            return self.template.raw
        else:
            return self._value

    @property
    def path(self):
        return self.template.path


class StringPlaceholder(FilePlaceholder):
    """
    Same as FilePlaceholder but cast their argument to str before init,
    so a Path will be interpreted literally instead of loading the file
    """

    def __init__(self, source):
        super().__init__(str(source))

    @property
    def path(self):
        return None


class FileLiteral:
    def __init__(self, source):
        if isinstance(source, Path):
            self.path = str(source)
            self.value = source.read_text()
        else:
            self.path = None
            self.value = source

    def __str__(self):
        return self.value

    @property
    def safe(self):
        return self.value


class StringLiteral:
    def __init__(self, source):
        self.path = None
        self.value = str(source)

    @property
    def loc(self):
        return None

    def __str__(self):
        return self.value

    @property
    def safe(self):
        return self.value


class Source(abc.ABC):

    def __init__(self, value):
        self.value = self.VALUECLASS(value)

    @property
    @abc.abstractmethod
    def doc(self):
        pass

    @property
    @abc.abstractmethod
    def doc_short(self):
        pass

    @property
    @abc.abstractmethod
    def language(self):
        pass

    @property
    @abc.abstractmethod
    def needs_render(self):
        pass

    @property
    @abc.abstractmethod
    def loc(self):
        pass

    def render(self, params, **kwargs):
        self.value.render(params, **kwargs)
        self._post_render_validation(self.value.value, params)

    # optional validation
    def _post_render_validation(self, rendered_value, params):
        pass

    # NOTE: should I require source ojects to implement __str__?
    # task.source_code does str(task.source), but I tihik the implementation
    # will be always str(self.value)


class SQLSourceMixin:
    """A source representing SQL source code
    """

    @property
    def doc(self):
        regex = r'^\s*\/\*([\w\W]+)\*\/[\w\W]*'
        match = re.match(regex, self.value.safe)
        return '' if match is None else match.group(1)

    @property
    def doc_short(self):
        return self.doc.split('\n')[0]

    @property
    def language(self):
        return 'sql'

    @property
    def loc(self):
        return None

    @property
    def needs_render(self):
        return True

    def __str__(self):
        return str(self.value)


class SQLScriptSource(SQLSourceMixin, Source):
    """
    A SQL (templated) script, it is expected to make a persistent change in
    the database (by using the CREATE statement), its validation verifies
    that, if no persistent changes should be validated use SQLQuerySource
    instead

    An object that represents SQL source, if a pathlib.Path object is passed,
    its contents are read and interpreted as the placeholder's content

    Notes
    -----
    This is really just a StrictTemplate object that stores its rendered
    version in the same object and raises an Exception if attempted. It also
    passes some of its attributes
    """
    VALUECLASS = FilePlaceholder

    def __init__(self, value):
        super().__init__(value)

        if self.value.template.is_literal:
            raise SourceInitializationError(
                '{} cannot be initialized with literals as'
                'they are meant to create a persistent '
                'change in the database, they need to '
                'include the {} placeholder'
                .format(self.__class__.__name__, '{{product}}'))

        # FIXME: validate {{product}} exists, does this also catch
        # {{product['key']}} ?

    def _post_render_validation(self, rendered_value, params):
        """Analyze code and warn if issues are found
        """
        # print(params)
        infered_relations = infer.created_relations(rendered_value)
        # print(infered_relations)

        if isinstance(params['product'], Product):
            actual_rel = {params['product']._identifier}
        else:
            # metaproduct
            actual_rel = {p._identifier for p in params['product']}

        infered_len = len(infered_relations)
        # print(infered_len)
        actual_len = len(actual_rel)

        # print(set(infered_relations) != set(actual_rel),
        #         set(infered_relations) ,set(actual_rel))

        if not infered_len:
            warnings.warn('It seems like your task "{task}" will not create '
                          'any tables or views but the task has product '
                          '"{product}"'
                          .format(task='some task',
                                  product=params['product']))

        elif infered_len != actual_len:
            warnings.warn('It seems like your task "{task}" will create '
                          '{infered_len} relation(s) but you declared '
                          '{actual_len} product(s): "{product}"'
                          .format(task='some task',
                                  infered_len=infered_len,
                                  actual_len=actual_len,
                                  product=params['product']))
        # parsing infered_relations is still WIP
        # elif set(infered_relations) != set(infered_relations):
        #         warnings.warn('Infered relations ({}) did not match products'
        #                       ' {}'
        #                       .format(infered_relations, actual_len))


class SQLQuerySource(SQLSourceMixin, Source):
    """
    Templated SQL query, it is not expected to make any persistent changes in
    the database (in contrast with SQLScriptSource), so its validation is
    different
    """
    VALUECLASS = FilePlaceholder

    # TODO: validate this is a SELECT statement
    # a query needs to return a result
    pass


class SQLRelationPlaceholder(StringPlaceholder):
    """An identifier that represents a database relation (table or view)
    """

    def __init__(self, source):
        if len(source) != 3:
            raise ValueError('{} must be initialized with 3 elements, '
                             'got: {}'
                             .format(type(self).__name__, len(source)))

        schema, name, kind = source

        if schema is None:
            # raise ValueError('schema cannot be None')
            schema = ''

        if name is None:
            raise ValueError('name cannot be None')

        if kind not in ('view', 'table'):
            raise ValueError('kind must be one of ["view", "table"] '
                             'got "{}"'.format(kind))

        # ignore double quotes (will be added if needed)
        if schema:
            schema = schema.replace('"', '')

        name = name.replace('"', '')

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
        return ('SQLRelationPlaceholder("{}"."{}")'
                .format(self.schema, self._source.raw, self.kind))

    def __eq__(self, other):
        return (self.schema == other.schema
                and self.name == other.name
                and self.kind == other.kind)

    def __hash__(self):
        return hash((self.schema, self.name, self.kind))


class PythonCallableSource(Source):
    """A source that holds a Python callable
    """

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

    @property
    def needs_render(self):
        return False

    @property
    def language(self):
        return 'python'


class GenericSource(Source):
    """
    Generic (untemplated) source, the simplest type of source, it does
    not render, perform any kind of parsing nor validation
    """
    VALUECLASS = StringLiteral

    def __str__(self):
        return str(self.value)

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
    def needs_render(self):
        return False

    @property
    def language(self):
        return None


class FileLiteralSource(Source):
    """
    Generic (untemplated) source, the simplest type of source, it does
    not render, perform any kind of parsing nor validation
    """
    VALUECLASS = FileLiteral

    def __str__(self):
        return str(self.value)

    @property
    def doc(self):
        return ''

    @property
    def doc_short(self):
        return ''

    @property
    def loc(self):
        return self.value.path

    @property
    def needs_render(self):
        return False

    @property
    def language(self):
        return None


class GenericTemplatedSource(GenericSource):
    VALUECLASS = StringPlaceholder

    def __str__(self):
        return str(self.value)

    @property
    def doc(self):
        return ''

    @property
    def doc_short(self):
        return ''

    @property
    def loc(self):
        return ''

    # FIXME: this is not part of source but currently used in notebook
    @property
    def path(self):
        return None

    @property
    def needs_render(self):
        return True

    @property
    def language(self):
        return None
