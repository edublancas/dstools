from pathlib import Path
import inspect
import warnings

from dstools.pipeline.identifiers.Identifier import Identifier

from jinja2 import Template


class StringIdentifier(Identifier):
    """Identifier that represents a str object
    """

    def after_init_hook(self):
        # warn if object is not string, Path is ok too...
        if not self.needs_render and not (isinstance(self.s, str)
                                          or isinstance(self.s, Path)):
            # if no Template passed but parameter is not str, cast...
            warnings.warn('Initialized StringIdentifier with non-string '
                          f'object "{self.s}" type: '
                          f'{type(self.s)}, casting to str...')
            self.s = str(self.s)

    def after_render_hook(self):
        pass


class CodeIdentifier(Identifier):
    """
    A CodeIdentifier represents a piece of code in various forms:
    a Python callable, a language-agnostic string, a path to a soure code file
    or a jinja2.Template
    """

    def after_init_hook(self):
        valid_type = (callable(self.s)
                      or isinstance(self.s, str)
                      or isinstance(self.s, Path)
                      or isinstance(self.s, Template))
        if not valid_type:
            TypeError('Code must be a callable, str, pathlib.Path or '
                      f'jinja2.Template, got {type(self.s)}')

    @property
    def source(self):
        if callable(self.s):
            # TODO: i think this doesn't work sometime and dill has a function
            # that covers more use cases, check
            return inspect.getsource(self())
        elif isinstance(self.s, Path):
            return self().read_text()
        elif isinstance(self.s, Template) or isinstance(self.s, str):
            return self()
        else:
            TypeError('Code must be a callable, str, pathlib.Path or '
                      f'jinja2.Template, got {type(self)}')

    def after_render_hook(self):
        pass
