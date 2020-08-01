"""
For inspiration:

import parso
from parso.utils import parse_version_string
from parso.python.tokenize import tokenize

v = parse_version_string('3')
list(tokenize('[1,2,3]', v))

mod = parso.parse('x = 1')
mod.children
"""

import pytest

from dstools.parser.lexer import Lexer
from dstools.parser.tokens import Integer, BinaryOperator, Assignment, Name, Operator, String


@pytest.mark.parametrize('code, tokens', [
    ('1+1', [Integer(1), BinaryOperator('+'),
             Integer(1)]),
    ('number <- 42', [Name('number'),
                      Assignment('<-'),
                      Integer(42)]),
    ('c(1,2)',
     [Operator('c('),
      Integer(1),
      Operator(','),
      Integer(2),
      Operator(')')]),
    ('list()', [Operator('list'),
                Operator('('), Operator(')')]),
    ('list(5  , 6)', [
        Operator('list'),
        Operator('('),
        Integer(5),
        Operator(','),
        Integer(6),
        Operator(')')
    ]),
    ('list(a=100)', [
        Operator('list'),
        Operator('('),
        Name('a'),
        Operator('='),
        Integer(100),
        Operator(')')
    ]),
    ('"hello"', [String('hello')]),
])
def test_lexer(code, tokens):
    assert list(Lexer(code)) == tokens
