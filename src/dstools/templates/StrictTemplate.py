import logging
from pathlib import Path
import re

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

    def __init__(self, source, conn=None):

        if isinstance(source, str):
            self.source = Template(source,
                                   undefined=jinja2.StrictUndefined)
            self.raw = source
            self.location = '[Loaded from str]'
        elif isinstance(source, Template):
            location = Path(source.filename)
            if not location.exists():
                raise ValueError('Could not load raw source from '
                                 'jinja2.Template, this usually happens '
                                 'when Templates are initialized directly '
                                 'from a str, only Templates loaded from '
                                 'the filesystem are supported, using a '
                                 'jinja2.Environment will fix this issue, '
                                 'if you want to create a template from '
                                 'a string pass it directly to this '
                                 'constructor')

            self.source = source
            self.raw = location.read_text()
            self.location = location

        self.declared = self._get_declared()

        self.conn = conn
        self.logger = logging.getLogger(__name__)

        # dynamically set the docstring
        self.__doc__ = self._parse_docstring()

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
        env = Environment()
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
            raise TypeError('Error rendering template {}, missing required '
                            'arguments: {}, got params {}'
                            .format(repr(self), missing, params))

        if extra:
            raise TypeError(f'Got unexpected arguments {extra}')

        return self.source.render(**params)
