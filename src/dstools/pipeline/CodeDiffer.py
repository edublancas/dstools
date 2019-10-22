"""
Utils for comparing source code
"""
from difflib import Differ

try:
    import sqlparse
except ImportError:
    sqlparse = None


try:
    import autopep8
except ImportError:
    autopep8 = None


def normalize_null(code):
    return code


def normalize_sql(code):
    if not sqlparse:
        raise ImportError('sqlparse is required for normalizing SQL code')

    return None if code is None else sqlparse.format(code,
                                                     keyword_case='upper',
                                                     identifier_case='lower',
                                                     strip_comments=True,
                                                     reindent=True,
                                                     indent_with=4)


def normalize_python(code):
    if not autopep8:
        raise ImportError('autopep8 is required for normalizing Python code')

    return None if code is None else autopep8.fix_code(code)


def diff_strings(a, b):
    """Compute the diff between two strings
    """
    d = Differ()

    if a is None and b is None:
        return '[Both a and b are None]'

    out = ''

    if a is None:
        out += '[a is None]\n'
    elif b is None:
        out += '[a is None]\n'

    a = '' if a is None else a
    b = '' if b is None else b

    result = d.compare(a.splitlines(keepends=True),
                       b.splitlines(keepends=True))
    out += ''.join(result)

    return out


class CodeDiffer:
    LANGUAGES = ['python', 'sql']
    NORMALIZERS = {None: normalize_null, 'python': normalize_python,
                   'sql': normalize_sql}

    def code_is_different(self, a, b, language=None):
        normalizer = self._get_normalizer(language)

        a = normalizer(a)
        b = normalizer(b)

        return a != b

    def get_diff(self, a, b, language=None):
        normalizer = self._get_normalizer(language)

        a = normalizer(a)
        b = normalizer(b)

        diff = diff_strings(a, b)

        if language is not None:
            diff = '[Code was normalized]\n' + diff

        return diff

    def _get_normalizer(self, language):
        if language in self.NORMALIZERS:
            return self.NORMALIZERS[language]
        else:
            return normalize_null
