try:
    import papermill as pm
except ImportError:
    pm = None

try:
    import jupytext
except ImportError:
    jupytext = None

try:
    import nbformat
except ImportError:
    nbformat = None

from dstools.pipeline.placeholders import ClientCodePlaceholder
from dstools.pipeline.products import File
from dstools.pipeline.tasks.Task import Task


def _to_ipynb(source, extension):
    """Convert to jupyter notebook via jupytext
    """
    nb = jupytext.reads(source, fmt={'extension': '.py'})

    # what if the file does not contain kernelspec info?

    # tag the first cell as "parameters" for papermill to inject them
    nb.cells[0]['metadata']['tags'] = ["parameters"]


    path_to_ipynb.write_text(nbformat.v4.writes(nb))


class NotebookRunner(Task):
    """Run a notebook using papermill
    """
    CODECLASS = ClientCodePlaceholder
    PRODUCT_CLASSES_ALLOWED = (File, )
    PRODUCT_IN_CODE = False

    def __init__(self, code, product, dag, name=None, params=None,
                 papermill_params=None):
        params = params or {}
        self.papermill_params = papermill_params or {}
        super().__init__(code, product, dag, name, params)

    def run(self):
        # FIXME: do not use this, since it can be templated code,
        # use rendered version, just save it in a tmp file or file-like
        # object
        path_to_in = str(self._code.path)
        path_to_out = str(self.product)
        # if pointing to a py file, convert to ipynb first using jupytext

        # papermill only allows JSON serializable parameters
        self.params['product'] = str(self.params['product'])

        _ = pm.execute_notebook(path_to_in, path_to_out,
                                parameters=self.params,
                                **self.papermill_params)

        # if output format other than ipynb, convert using nbconvert
