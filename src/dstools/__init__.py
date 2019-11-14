from dstools.env import Env
from dstools.reproducibility.util import make_filename as mkfilename

__version__ = '0.11'

# Set default logging handler to avoid "No handler found" warnings.
import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())


__all__ = ['Env', 'mkfilename']
