from tempfile import mktemp
from pathlib import Path

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

try:
    import nbconvert
except ImportError:
    nbconvert = None


from dstools.pipeline.placeholders import ClientCodePlaceholder
from dstools.pipeline.products import File
from dstools.pipeline.tasks.Task import Task


def _to_ipynb(source, extension):
    """Convert to jupyter notebook via jupytext
    """
    nb = jupytext.reads(source, fmt={'extension': extension})

    # what if the file does not contain kernelspec info?

    # tag the first cell as "parameters" for papermill to inject them
    nb.cells[0]['metadata']['tags'] = ["parameters"]

    out = mktemp()
    Path(out).write_text(nbformat.v4.writes(nb))

    return out


def _from_ipynb(path_to_nb, extension):
    # TODO: support more extensions, also pass custom parameters
    ext_map = {'.html': nbconvert.HTMLExporter}
    path = Path(path_to_nb)

    nb = nbformat.v4.reads(path.read_text())
    content, _ = nbconvert.export(ext_map[extension], nb,
                                  exclude_input=True)

    path.write_text(content)

    return content


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
        path_to_out = str(self.product)

        source = str(self._code)
        ext_in = Path(self._code.path).suffix
        ext_out = Path(path_to_out).suffix

        # need to convert to ipynb using jupytext
        if ext_in != '.ipynb':
            path_to_in = _to_ipynb(source, ext_in)
        else:
            # otherwise just save rendered code in a tmp file
            path_to_in = mktemp()
            Path(path_to_in).write_text(source)

        # papermill only allows JSON serializable parameters
        self.params['product'] = str(self.params['product'])

        _ = pm.execute_notebook(path_to_in, path_to_out,
                                parameters=self.params,
                                **self.papermill_params)

        # if output format other than ipynb, convert using nbconvert
        # and overwrite
        if ext_out != '.ipynb':
            _from_ipynb(path_to_out, ext_out)
