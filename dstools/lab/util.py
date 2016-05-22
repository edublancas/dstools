from collections import defaultdict


def top_k(elements, key, k, descending=True):
    '''
        Get top k from elements based on a function
        key can be either a str of a function to extract the key
    '''
    if isinstance(key, str):
        key_str = key

        def key(x):
            return x[key_str]

    return sorted(elements, key=key, reverse=descending)[:k]


def group_by(data, criteria):
    '''
        Group objects given a function or key
    '''
    if isinstance(criteria, str):
        criteria_str = criteria

        def criteria(x):
            return x[criteria_str]

    res = defaultdict(list)
    for element in data:
        key = criteria(element)
        res[key].append(element)
    return res
