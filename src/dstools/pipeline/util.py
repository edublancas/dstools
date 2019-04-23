from difflib import Differ


def diff_strings(a, b):
    d = Differ()
    result = d.compare(a.splitlines(keepends=True),
                       b.splitlines(keepends=True))
    return ''.join(result)
