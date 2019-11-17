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
from dstools.templates.Placeholder import Placeholder
from dstools.exceptions import SourceInitializationError
from dstools.sql import infer

# FIXME: move diagnose to here, task might need this as well, since
# validation may involve checking against the product, but we can replace
# this behabior for an after-render validation, and just pass the product
# as parameter maybe from the Task? the task should not do this
# FIXME: remove opt from Placeholder.render


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
"""
Same as FilePlaceholder but cast their argument to str before init,
so a Path will be interpreted literally instead of loading the file
"""

"""
Placeholders are jinja2 templates that hold their rendered
values after Placeholder.render is called, they are used in
source and Product objects to hold values that are be filled
after a DAG is rendered
"""


class Source(abc.ABC):

    def __init__(self, value):
        self.value = Placeholder(value)
        self._post_init_validation(self.value)

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
    def needs_render(self):
        return self.value.needs_render

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

    def _post_init_validation(self, value):
        pass

    def __str__(self):
        return str(self.value)


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
    This is really just a Placeholder object that stores its rendered
    version in the same object and raises an Exception if attempted. It also
    passes some of its attributes
    """

    def _post_init_validation(self, value):
        if not value.needs_render:
            raise SourceInitializationError(
                '{} cannot be initialized with literals as '
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
    # TODO: validate this is a SELECT statement
    # a query needs to return a result
    pass


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
    Generic source, the simplest type of source, it does not perform any kind
    of parsing nor validation
    """
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
    def language(self):
        return None
