import base64
from glob import glob
from pathlib import Path
from collections import defaultdict
import shutil

from dstools.pipeline.products import File


def safe_remove(path):
    if path.exists():
        if path.is_file():
            path.unlink()
        else:
            shutil.rmtree(path)


def image_bytes2html(data):
    fig_base64 = base64.encodebytes(data)
    img = fig_base64.decode("utf-8")
    html = '<img src="data:image/png;base64,' + img + '"></img>'
    return html


def clean_up_files(dag, interactive=True):
    # get products that generate Files
    paths = [Path(str(t.product)) for t in dag.values()
             if isinstance(t.product, File)]
    # each file generates a .source file, also add it
    paths = [(p, Path(str(p) + '.source')) for p in paths]
    # flatten list
    paths = [p for tup in paths for p in tup]

    # get parents
    parents = set([p.parent for p in paths])

    # map parents to its files
    parents_map = defaultdict(lambda: [])

    for p in paths:
        parents_map[str(p.parent)].append(str(p))

    extra_all = []

    # for every parent, find the extra files
    for p in parents:
        existing = glob(str(p) + '/*')
        products = parents_map[str(p)]

        extra = set(existing) - set(products)
        extra_all.extend(list(extra))

    for p in extra_all:
        if interactive:
            answer = input('Delete {} ? (y/n)'.format(p))

            if answer == 'y':
                safe_remove(p)
                print('Deleted {}'.format(p))
