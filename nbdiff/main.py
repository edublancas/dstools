import ast
from pathlib import Path

from ploomber import DAG
from ploomber.tasks import NotebookRunner
from ploomber.products import File

params_a = {'hyperparameters': {'a': 1, 'b': 2}, 'number': 2}
params_b = {'hyperparameters': {'a': 10, 'b': 200}, 'number': 2}

dag = DAG()
NotebookRunner(Path('nb.py'),
               File('output/a.ipynb'),
               dag,
               params=params_a,
               name='a')
NotebookRunner(Path('nb.py'),
               File('output/b.ipynb'),
               dag,
               params=params_b,
               name='b')

dag.build()

import nbformat

a = nbformat.read('output/a.ipynb', as_version=nbformat.NO_CONVERT)
b = nbformat.read('output/b.ipynb', as_version=nbformat.NO_CONVERT)


def tag_map(nb, ignore=None):
    ignore = ignore or ['parameters', 'injected-parameters']
    return {
        c.metadata.tags[0]: c
        for c in nb.cells
        if len(c.metadata.tags) == 1 and c.metadata.tags[0] not in ignore
    }


a = tag_map(a)
b = tag_map(b)


def process_key(key, a, b):
    cell_a = a[key]
    cell_b = b[key]

    for out_a, out_b in zip(cell_a['outputs'], cell_b['outputs']):
        process_output(out_a, out_b)


def process_output(a, b):
    obj_a = ast.literal_eval(a['data']['text/plain'])
    obj_b = ast.literal_eval(b['data']['text/plain'])
    diff(obj_a, obj_b)


def diff(a, b):
    print(a, b)


process_key('hyperparameters', a, b)
