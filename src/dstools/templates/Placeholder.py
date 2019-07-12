from dstools.templates import StrictTemplate
from pathlib import Path


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
