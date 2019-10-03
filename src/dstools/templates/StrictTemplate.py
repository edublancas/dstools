import logging
from pathlib import Path
import re

from dstools.exceptions import RenderError

from numpydoc.docscrape import NumpyDocString
import jinja2
from jinja2 import Environment, meta, Template, UndefinedError


class StrictTemplate:
    """
    A jinja2 Template-like object that adds the following features:

        * template.raw - Returns the raw template used for initialization
        * template.location - Returns a path to the Template object if available
        * strict - will not render if missing or extra parameters
        * docstring parsing

    Note that this does not implement the full jinja2.Template API
    """

    def __init__(self, source):
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))
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

        # dynamically set the docstring
        # self.__doc__ = self._parse_docstring()
        self._doc = self._parse_docstring()

    @property
    def doc(self):
        return self._doc

    @property
    def doc_short(self):
        return self.doc.split('\n')[0]

    @property
    def source(self):
        """jinja2.Template object
        """
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

    def _parse_docstring(self):
        """Finds the docstring at the beginning of the source
        """
        # [any whitespace] /* [capture] */ [any string]
        regex = r'^\s*\/\*([\w\W]+)\*\/[\w\W]*'
        match = re.match(regex, self.raw)
        return '' if match is None else match.group(1)

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
        return self.raw

    def __repr__(self):
        return '{}("{}")'.format(type(self).__name__, str(self))

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
            return self.source.render(**params)
        except UndefinedError as e:
            raise RenderError('in {}, jinja2 raised an UndefinedError, this '
                              'means the template is using an attribute '
                              'or item that does not exist, the original '
                              'traceback is shown above. For jinja2 '
                              'implementation details see: '
                              'http://jinja.pocoo.org/docs/latest'
                              '/templates/#variables'
                              .format(repr(self))) from e

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
