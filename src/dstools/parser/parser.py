import itertools
from dstools.parser.lexer import Lexer
from dstools.parser.tokens import Integer, BinaryOperator, Assignment, Name, Operator


def get_slicer(elements, size):
    elements = iter(elements)

    while True:
        slice_ = list(itertools.islice(elements, size))

        if not slice_:
            return

        if len(slice_) == size - 1:

            slice_.append(None)

        yield slice_


class AST:
    def __init__(self, children):
        self.children = children


class Node:
    pass


class Expression(Node):
    def __init__(self, left, op, right):
        self.left = left
        self.op = op
        self.right = right

    def __repr__(self):
        return '{} {} {}'.format(self.left, self.op, self.right)


class ListNode(Node):
    def __init__(self, tokens):
        self.elements = [value for value, comma in get_slicer(tokens, size=2)]

    def to_python(self):
        return [e.value for e in self.elements]


class DictionaryNode(Node):
    def __init__(self, tokens):
        self.elements = [
            (key, value)
            for key, equal, value, comma in get_slicer(tokens, size=4)
        ]

    def to_python(self):
        return {key.value: value.value for key, value in self.elements}


def build_node(tokens):
    if tokens[0] == Operator('list'):
        elements = tokens[2:-1]

        if isinstance(elements[0], Name):
            return DictionaryNode(elements)
        else:
            return ListNode(elements)

    else:
        raise SyntaxError('Must be a list object')


class Parser:
    def __init__(self, code):
        self.tokens = list(Lexer(code))
        self.pos = 0

    @property
    def current_token(self):
        return self.tokens[self.pos]

    @property
    def next_token(self):
        return self.tokens[self.pos + 1]

    def get_tail(self, exclude_next=True):
        return self.tokens[self.pos + 1 + int(exclude_next):]

    def parse(self):
        if not isinstance(self.current_token, Name):
            raise SyntaxError('First token must be a valid name')

        if not isinstance(self.next_token, Assignment):
            raise SyntaxError('Second token must be an assignment')

        return Expression(self.current_token, self.next_token,
                          build_node(self.get_tail()))
