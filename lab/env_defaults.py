import collections.abc
import logging

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)


# TODO: keep track of nested keys so we can say things like
# db.postgres.uri will be overwritten
def update(d, u):
    """
    https://stackoverflow.com/a/3233356/709975
    """
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


defaults = {'a': 1, 'b': 2, 'c': {'nested': 3}}
current = {'b': 3, 'c': {'nested': 30}}

defaults

update(defaults, current)



defaults_k = set(defaults)
current_k = set(current)

logger.info('a')

# loaded from defaults
defaults_k - current_k


# overwritten
defaults_k & current_k


{**defaults, **current}
