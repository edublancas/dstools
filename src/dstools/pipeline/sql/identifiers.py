from functools import total_ordering
import warnings

from dstools.pipeline.sql import SQLRelationKind

from jinja2 import Template


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

    def __call__(self):
        if self.needs_render and not self.rendered:
            raise RuntimeError('Attempted to read Identifier '
                               f'{repr(self)} '
                               '(which was initialized with '
                               'a jinja2.Template object) wihout '
                               'rendering the DAG first, call '
                               'dag.render() on the dag before reading '
                               'the identifier or initialize with a str '
                               'object')
        else:
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
