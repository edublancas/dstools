from dstools.templates import StrictTemplate
from pathlib import Path

# TODO: rename to Placeholder
# FIXME: use SQLTemplate if initialized with a template-
# since it is more strict for rendering
# and gives better error messages, rename SQLTemplate to Template


class Placeholder:
    """Identifier that a lazy Template
    """

    def __init__(self, content):
        self._validate_content(content)

        if self._can_cast_to_str(content):
            self._content = StrictTemplate(str(content))
            self._content_raw = str(content)
        else:
            self._content = content
            self._content_raw = None

        self._content_rendered = None

    @property
    def content(self):
        """Returns the object that was used to initialize the Placeholder
        """
        return self._content

    @property
    def content_rendered(self):
        """Returns the Placeholder's rendered version (str), must call
        Placeholder.render first if initialized with a Template
        """
        if self._content_rendered is None:
            raise RuntimeError('Tried to read Placeholder {} without '
                               'rendering first'.format(repr(self)))
        return self._content_rendered

    @property
    def content_raw(self):
        """Returns the raw Placeholder, if initialized with a str or
        a pathlib.Path this is the same as content_rendered, if initialized
        with a Template, this will be the raw Template
        """
        raise NotImplementedError('Missing implementation')
        # return self._content_raw

    def render(self, params):
        # FIXME: raise error if initialized with str but passed
        # render params?
        if hasattr(self.content, 'render'):
            self._content_rendered = self.content.render(params)

        return self

    def _can_cast_to_str(self, content):
        # if initialized with Path or str, it is already "rendered"
        return isinstance(content, Path) or isinstance(content, str)

    def _validate_content(self, content):
        # warn if object is not string, Path is ok too...
        if not (hasattr(content, 'render')
                or self._can_cast_to_str(content)):
            raise TypeError('{} must be initialized with a Template a '
                            'pathlib.Path or str, got {} instead'
                            .format(type(self).__name__,
                                    type(content).__name__))

    def __repr__(self):
        return '{}({})'.format(type(self).__name__, repr(self.content))

    def __str__(self):
        return self.content_rendered
