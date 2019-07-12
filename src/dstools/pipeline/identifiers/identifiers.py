
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


from dstools.templates import StrictTemplate, Placeholder

from jinja2 import Template


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
    def __init__(self, code_init_obj):
        if isinstance(code_init_obj, Path):
            self._location = code_init_obj
            self._placeholder = Placeholder(code_init_obj.read_text())
        elif isinstance(code_init_obj, str):
            self._location = '[Loaded from str]'
            self._placeholder = Placeholder(code_init_obj)
        elif isinstance(code_init_obj, (Template, StrictTemplate)):
            self._location = None
            self._placeholder = Placeholder(code_init_obj)
        else:
            raise TypeError('Invalid type, initializer must be '
                            'pathlib.Path, str or a Template-like object')

    @property
    def code_init_obj(self):
        return self._placeholder.content

    @property
    def location(self):
        return self._location

    def render(self, params, **kwargs):
        self._placeholder.render(params, **kwargs)

    def __str__(self):
        return str(self._placeholder)

    def __repr__(self):
        return '{}({})'.format(type(self).__name__, repr(self._placeholder))
