from jinja2 import PackageLoader, Environment, StrictUndefined

_env = Environment(loader=PackageLoader('dstools', 'profiling'),
                   undefined=StrictUndefined)


def profile(relation,
            mappings,
            alias,
            group_by=None,
            agg=None,
            return_all=False):
    if group_by:
        return _agg(relation, mappings, alias, group_by, agg, return_all)
    else:
        if not agg:
            raise ValueError(
                'agg contain at least one value if passing group_by')
        return _simple(relation, mappings, alias)


def _simple(relation, mappings, alias):
    """

    >>> mappings = {'col': ['min', 'max']}
    >>> alias = {'col': 'new_name'}
    """
    t = _env.get_template('simple.sql')
    return t.render(mappings=mappings,
                    alias=alias,
                    relation=relation,
                    group_by=None)


def _agg(relation, mappings, alias, group_by, agg, return_all=False):
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
