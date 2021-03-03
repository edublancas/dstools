from jinja2 import PackageLoader, Environment, StrictUndefined

_env = Environment(loader=PackageLoader('dstools', 'profiling'),
                   undefined=StrictUndefined)


def simple(relation, mappings, alias):
    """

    >>> mappings = {'col': ['min', 'max']}
    >>> alias = {'col': 'new_name'}
    """
    t = _env.get_template('simple.sql')
    return t.render(mappings=mappings,
                    alias=alias,
                    relation=relation,
                    group_by=None)


def agg(relation, mappings, alias, group_by, agg, return_all=False):
    """

    >>> mappings = {'col': ['min', 'max']}
    >>> alias = {'col': 'new_name'}
    >>> group_by = 'col_id'
    >>> agg = ['min', 'max']
    """
    t = _env.get_template('agg.sql')
    return t.render(relation=relation,
                    mappings=mappings,
                    alias=alias,
                    group_by=group_by,
                    agg=agg,
                    return_all=return_all)
