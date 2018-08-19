# feature generation using pandas
import pandas as pd
from itertools import combinations
import operator as op


df = pd.read_csv('train.csv', index_col='id')

pair = list(list(combinations(df.columns, 2))[0])
sub = df[pair]
sub


def fn(row):
    return reduce(op.mul, row)

sub.apply(fn, axis=1)
sub


def interact(df, columns, fn, name):
    sub = df[columns]
    res = sub.apply(lambda row: reduce(fn, row), axis=1)
    res.name = '{}_{}_{}'.format(columns[0], name, columns[1])
    return res

res = interact(df, ['age', 'fare'], op.mul, name='over')
