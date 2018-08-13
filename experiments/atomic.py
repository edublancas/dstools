"""
Sometimes I need to run atomic operations that depend on third-party
resources, I want similar DB commit functionality
"""
import logging
import random

logging.basicConfig(level=logging.DEBUG)


def funky_function(x, p=0.9):

    i = random.randint(0, 9)

    if i < int(p * 10):
        return x + 1
    else:
        raise ValueError('hehe')


def funky_function_revert(x, p=0.9):

    i = random.randint(0, 9)

    if i < int(p * 10):
        return x + 1
    else:
        raise ValueError('hehe')


class AtomicFunction:

    def __init__(self, fn, fn_revert=None, *args, **kwargs):
        self.fn = fn
        self.fn_revert = fn_revert
        self.args = args
        self.kwargs = kwargs
        self.logger = logging.getLogger(__name__)

    def __call__(self):
        try:
            self.fn(*self.args, **self.kwargs)
        except Exception as e:
            self.logger.exception('Error when running function...')

            try:
                self.fn_revert(*self.args, **self.kwargs)
            except Exception as e:
                self.logger.exception('Error when running revert function...')
            else:
                self.logger.exception('Successfully ran revert function...')

            return False

        return True

funky_function(1, p=0.5)


atomic = AtomicFunction(fn=funky_function, fn_revert=funky_function_revert, x=1, p=0.5)
atomic()
