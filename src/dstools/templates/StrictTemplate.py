# import logging
from pathlib import Path
import re

from dstools.pipeline.exceptions import RenderError

from numpydoc.docscrape import NumpyDocString
import jinja2
from jinja2 import Environment, meta, Template


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
        if isinstance(source, Path):
            self._path = source
            self._raw = source.read_text()
            self._source = Template(self._raw,
                                    undefined=jinja2.StrictUndefined)
        elif isinstance(source, str):
            self._path = None
            self._raw = source
            self._source = Template(source,
                                    undefined=jinja2.StrictUndefined)

        elif isinstance(source, Template):
            path = Path(source.filename)

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
            self._source = Template(self._raw,
                                    undefined=jinja2.StrictUndefined)
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

        # dynamically set the docstring
        # self.__doc__ = self._parse_docstring()

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
        return self._raw

    def _parse_docstring(self):
        """Finds the docstring at the beginning of the source
        """
        # [any whitespace] /* [capture] */ [any string]
        regex = r'^\s*\/\*([\w\W]+)\*\/[\w\W]*'
        match = re.match(regex, self.raw)
        return '' if match is None else match.group(1)

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
            raise RenderError('Error rendering template {}, missing required '
                              'arguments: {}, got params {}'
                              .format(repr(self), missing, params))

        if extra:
            raise RenderError('Got unexpected arguments {}, '
                              'declared arguments are {}'
                              .format(extra, self.declared))

        return self.source.render(**params)
