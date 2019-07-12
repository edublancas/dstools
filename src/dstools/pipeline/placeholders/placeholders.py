"""
User only have to know about DAG, Task and Product, the rest are used
internally

DAG: collection of tasks - makes sure tasks are executed in order
Task: unit of work, it has associated code and product, has a name
    which can be infered from the code and lives in a DAG, it also
    specifies runtime parameters for the code, injected at the right
    time, it also specifies how to run the code
Product: spefies a persistent object in disk such as a File or an
object in a database, they are lazy evaluated so they can be templates
that are rendered and passed to its corresponding task

---

Code: there is only two types of code PythonCode and ClientCode.
    they use the function specified from the Task to execute itself,
    they have an intermediate "rendered" state where they fill their
    parameters but wait execution until it is the right time,
    they also provide other things such as finding its source code,
    validation, normalization, etc
"""
from pathlib import Path
import inspect


from dstools.templates import StrictTemplate
from functools import total_ordering
import warnings

from dstools.pipeline.sql import SQLRelationKind
from jinja2 import Template


class Placeholder:
    """
    StringPlaceholders are StrictTemplates that store its rendered version
    in the same object so it can later be accesed
    """

    def __init__(self, template):
        if isinstance(template, Path):
            template = str(template)

        self._template = StrictTemplate(template)
        self._rendered = None

    @property
    def rendered(self):
        if self._rendered is None:
            raise RuntimeError('Tried to read Placeholder {} without '
                               'rendering first'.format(repr(self)))

        return self._rendered

    def render(self, params, **kwargs):
        self._rendered = self._template.render(params, **kwargs)
        return self

    def __repr__(self):
        return '{}({})'.format(type(self).__name__, self._template.raw)

    def __str__(self):
        return self.rendered



class PythonCode:

    def __init__(self, code_init_obj):
        if not callable(code_init_obj):
            raise TypeError(f'{type(self).__name__} must be initialized'
                            'with a Python callable, got '
                            f'"{type(code_init_obj).__name__}"')

        self._code_init_obj = code_init_obj
        self._code_init_obj_as_str = inspect.getsource(code_init_obj)
        self._location = None

        self._params = None

    @property
    def code_init_obj(self):
        return self._code_init_obj

    def __str__(self):
        return self._code_init_obj_as_str

    @property
    def locaion(self):
        return self._locaion

    def render(self, params, **kwargs):
        # FIXME: we need **kwargs for compatibility, but they are not used,
        # think what's the best thing to do
        # TODO: verify that params match function signature
        self._params = params

    def run(self):
        self.code_init_obj(**self._params)


class ClientCode:
    """An object that represents client code

    Notes
    -----
    This is really just a StrictTemplate object that stores its rendered
    version in the same object and raises an Exception if attempted
    """

    def __init__(self, template):
        self._template = StrictTemplate(template)
        self._rendered = None

    @property
    def rendered(self):
        if self._rendered is None:
            raise RuntimeError('Tried to read Placeholder {} without '
                               'rendering first'.format(repr(self)))

        return self._rendered

    def render(self, params, **kwargs):
        self._rendered = self._template.render(params, **kwargs)
        return self

    def __repr__(self):
        return '{}({})'.format(type(self).__name__, self._template.raw)

    def __str__(self):
        return self.rendered


@total_ordering
class SQLIdentifier:
    """An identifier that represents a database relation (table or view)
    """
    # FIXME: make this a subclass of Identifier, and add hooks

    def __init__(self, schema, name, kind):
        self.needs_render = isinstance(name, Template)
        self.rendered = False

        if kind not in (SQLRelationKind.view, SQLRelationKind.table):
            raise ValueError('kind must be one of ["view", "table"] '
                             f'got "{kind}"')

        self.kind = kind
        self.schema = schema
        self.name = name

        if not self.needs_render:
            self._validate_name()

    # FIXME: THIS SHOULD ONLY BE HERE IF POSTGRES
    def _validate_name(self):
        if len(self.name) > 63:
            url = ('https://www.postgresql.org/docs/current/'
                   'sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS')
            raise ValueError(f'"{self.name}" exceeds maximum length of 63 '
                             f' (length is {len(self.name)}), '
                             f'see: {url}')

    def render(self, params, **kwargs):
        if self.needs_render:
            if not self.rendered:
                self.name = self.name.render(params, **kwargs)
                self.rendered = True
            else:
                warnings.warn(f'Trying to render {repr(self)}, with was'
                              ' already rendered, skipping render...')

        return self

    def __str__(self):
        if self.schema:
            return f'"{self.schema}"."{self.name}"'
        else:
            return f'"{self.name}"'

    def __repr__(self):
        return f'"{self.schema}"."{self.name}" (PG{self.kind.capitalize()})'

    def __eq__(self, other):
        """Compare schema.name to set order"""
        return str(self) == str(other)

    def __lt__(self, other):
        return str(self) < str(other)
