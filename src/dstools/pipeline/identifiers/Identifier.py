"""
Identifiers are used by products to represent their persistent
representations, for example product, for example, File uses it to represent
the path to a file. Identifiers are lazy-loaded, they can be initialized
with a jinja2.Template and rendered before task execution, which makes
passing metadata between products and its upstream tasks possible.
"""
import warnings

from jinja2 import Template


class Identifier:
    """Identifier abstract class
    """

    def __init__(self, s):
        self.needs_render = isinstance(s, Template)
        self.rendered = False
        self.s = s

        self.after_init_hook()

    def after_init_hook(self):
        raise NotImplementedError('Identifier subclasses must implement this '
                                  'method')

    def after_render_hook(self):
        raise NotImplementedError('Identifier subclasses must implement this '
                                  'method')

    def __str__(self):
        return self()

    def __repr__(self):
        return f'{type(self)}({self.s})'

    def render(self, params):
        if self.needs_render:
            if not self.rendered:
                self.s = self.s.render(params)
                self.rendered = True
                self.after_render_hook()
            else:
                warnings.warn(f'Trying to render {repr(self)}, with was'
                              ' already rendered, skipping render...')

        return self

    def __call__(self):
        """Identifiers must be called from here
        """
        if self.needs_render:
            if not self.rendered:
                raise RuntimeError('Attempted to read Identifier '
                                   f'{repr(self)} '
                                   '(which was initialized with '
                                   'a jinja2.Template object) wihout '
                                   'rendering the task first, call '
                                   'task.render() before reading '
                                   'the identifier or initialize with a str '
                                   'object, if this is part of a DAG, '
                                   'the task should render automatically')
            return self.s
        else:
            return self.s
