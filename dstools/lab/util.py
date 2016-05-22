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
