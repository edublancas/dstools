import logging
from pathlib import Path
import re

from dstools.exceptions import RenderError

from numpydoc.docscrape import NumpyDocString
import jinja2
from jinja2 import Environment, meta, Template, UndefinedError


# TODO: rename this to placeholder
class StrictTemplate:
    """
    A jinja2 Template-like object that adds the following features:

        * template.raw - Returns the raw template used for initialization
        * template.location - Returns a path to the Template object if available
        * strict - will not render if missing or extra parameters
        * It also keeps the rendered value for later access

    Note that this does not implement the full jinja2.Template API
    """

    def __init__(self, source, load_if_path=True):
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))

        if isinstance(source, Path) and not load_if_path:
            source = str(source)

        if isinstance(source, Path):
            self._path = source
            self._raw = source.read_text()
            self._source = Template(self._raw,
                                    undefined=jinja2.StrictUndefined)
        elif isinstance(source, str):
            self._path = None
            self._raw = source
            self._source = Template(self._raw,
                                    undefined=jinja2.StrictUndefined)

        elif isinstance(source, Template):
            path = Path(source.filename)

            if source.environment.undefined != jinja2.StrictUndefined:
                raise ValueError('StrictTemplate can only be initialized '
                                 'from jinja2.Templates whose undefined '
                                 'parameter is set to '
                                 'jinja2.StrictUndefined, set it explicitely '
                                 'either in the Template or Environment '
                                 'constructors')

            if not path.exists():
                raise ValueError('Could not load raw source from '
                                 'jinja2.Template, this usually happens '
                                 'when Templates are initialized directly '
                                 'from a str, only Templates loaded from '
                                 'the filesystem are supported, using a '
                                 'jinja2.Environment will fix this issue, '
                                 'if you want to create a template from '
                                 'a string pass it directly to this '
                                 'constructor')

            self._path = path
            self._raw = path.read_text()
            self._source = source
        elif isinstance(source, StrictTemplate):
            self._path = source.path
            self._raw = source.raw
            self._source = source.source
        else:
            raise TypeError('{} must be initialized with a Template, '
                            'StrictTemplate, pathlib.Path or str, '
                            'got {} instead'
                            .format(type(self).__name__,
                                    type(source).__name__))

        self.declared = self._get_declared()

        self.is_literal = self._check_is_literal()

        self._value = self.raw if self.is_literal else None

        # dynamically set the docstring
        # self.__doc__ = self._parse_docstring()

    @property
    def value(self):
        if self._value is None:
            raise RuntimeError('Tried to read {} {} without '
                               'rendering first'
                               .format(type(self).__name__,
                                       repr(self)))

        return self._value

    @property
    def source(self):
        """jinja2.Template object
        """
        # TODO: rename this to template
        return self._source

    @property
    def raw(self):
        """A string with the raw jinja2.Template contents
        """
        return self._raw

    @property
    def path(self):
        """The location of the raw object

        Notes
        -----
        None if initialized with a str or with a jinja2.Template created
        from a str
        """
        return self._path

    def _check_is_literal(self):
        """
        Returns true if the template is a literal and does not need any
        parameters to render
        """
        env = self.source.environment

        # check if the template has the variable or block start string
        # is there any better way of checking this?
        return ((env.variable_start_string not in self.raw)
                and env.block_start_string not in self.raw)

    def __str__(self):
        return self.value

    def __repr__(self):
        return '{}("{}")'.format(type(self).__name__, self.safe)

    def _get_declared(self):
        if self.raw is None:
            raise ValueError('Cannot find declared values is raw is None')

        env = Environment()

        # this accepts None and does not break!
        ast = env.parse(self.raw)
        declared = meta.find_undeclared_variables(ast)
        return declared

    def diagnose(self):
        """Prints some diagnostics
        """
        found = self.declared
        docstring_np = NumpyDocString(self.docstring())
        documented = set([p[0] for p in docstring_np['Parameters']])

        print('The following variables were found in the template but are '
              f'not documented: {found - documented}')

        print('The following variables are documented but were not found in '
              f'the template: {documented - found}')

        return documented, found

    def render(self, params, optional=None):
        """
        """
        optional = optional or {}
        optional = set(optional)

        passed = set(params.keys())

        missing = self.declared - passed
        extra = passed - self.declared - optional

        if missing:
            raise RenderError('in {}, missing required '
                              'parameters: {}, params passed: {}'
                              .format(repr(self), missing, params))

        if extra:
            raise RenderError('in {}, unused parameters: {}, params '
                              'declared: {}'
                              .format(repr(self), extra, self.declared))

        try:
            self._value = self.source.render(**params)
            return self.value
        except UndefinedError as e:
            raise RenderError('in {}, jinja2 raised an UndefinedError, this '
                              'means the template is using an attribute '
                              'or item that does not exist, the original '
                              'traceback is shown above. For jinja2 '
                              'implementation details see: '
                              'http://jinja.pocoo.org/docs/latest'
                              '/templates/#variables'
                              .format(repr(self))) from e

    @property
    def safe(self):
        if self._value is None:
            return self.raw
        else:
            return self._value

    # __getstate__ and __setstate__ are needed to make this picklable

    def __getstate__(self):
        state = self.__dict__.copy()
        # _logger and _source are not pickable, so we remove them and build
        # them again in __setstate__
        del state['_logger']
        del state['_source']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))
        self._source = Template(self._raw,
                                undefined=jinja2.StrictUndefined)


# FIXME: remove this
class StringPlaceholder(StrictTemplate):
    def __init__(self, source):
        super().__init__(source, False)


class SQLRelationPlaceholder:
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

    @property
    def safe(self):
        return '"{}"."{}"'.format(self.schema, self._source.raw, self.kind)

    def __eq__(self, other):
        return (self.schema == other.schema
                and self.name == other.name
                and self.kind == other.kind)

    def __hash__(self):
        return hash((self.schema, self.name, self.kind))
