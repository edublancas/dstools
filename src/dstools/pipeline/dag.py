"""
DAG module

A DAG is collection of tasks that makes sure they are executed in
the right order
"""
from pathlib import Path
import warnings
import logging
import collections
import subprocess
import tempfile

try:
    import importlib.resources as importlib_resources
except ImportError:
    # backported
    import importlib_resources


try:
    import mistune
except ImportError:
    mistune = None

import networkx as nx
from tqdm.auto import tqdm
from jinja2 import Template

from dstools.pipeline.Table import Table, BuildReport
from dstools.pipeline.products import MetaProduct
from dstools.pipeline.util import image_bytes2html
from dstools.pipeline.CodeDiffer import CodeDiffer
from dstools.pipeline import resources


class DAG(collections.abc.Mapping):
    """A DAG is a collection of tasks with dependencies

    Parameters
    ----------
    differ: CodeDiffer
        An object to determine whether two pieces of code are the same and
        to output a diff, defaults to CodeDiffer() (default parameters)

    Attributes
    ----------
    build_report: BuildStatus
        A dict-like object with tasks as keys and BuildStatus objects for each
        task as values. str(BuildStatus) returns a table in plain text. This
        object is created after build() is run, otherwise is None
    """
    # TODO: use the networkx.DiGraph structure directly to avoid having to
    # re-build the graph every time

    def __init__(self, name=None, clients=None, differ=None):
        self._dict = {}
        self.name = name or 'No name'
        self.differ = differ or CodeDiffer()
        self._logger = logging.getLogger(__name__)
        self.build_report = None

        self._clients = clients or {}
        self._rendered = False

    @property
    def product(self):
        # We have to rebuild it since tasks might have been added
        return MetaProduct([t.product for t in self.values()])

    @property
    def clients(self):
        return self._clients

    def render(self, show_progress=True, force=False):
        """Render the graph
        """
        g = self._to_graph()

        def unique(elements):
            elements_unique = []
            for elem in elements:
                if elem not in elements_unique:
                    elements_unique.append(elem)
            return elements_unique

        dags = unique([t.dag for t in g])

        # first render any other dags involved (this happens when some
        # upstream parameters come form other dags)
        for dag in dags:
            if dag is not self:
                dag._render_current(show_progress, force)

        # then, render this dag
        self._render_current(show_progress, force)

        return self

    def build(self):
        """
        Runs the DAG in order so that all upstream dependencies are run for
        every task

        Returns
        -------
        DAGStats
            A dict-like object with tasks as keys and dicts with task
            status as values. str(DAGStats) returns a table in plain text
        """
        self.render()

        # attributes docs:
        # https://graphviz.gitlab.io/_pages/doc/info/attrs.html

        status_all = []

        g = self._to_graph()
        pbar = tqdm(nx.algorithms.topological_sort(g), total=len(g))

        for t in pbar:
            pbar.set_description('Building task "{}"'.format(t.name))
            t.build()
            status_all.append(t.build_report)

        self.build_report = BuildReport(status_all)
        self._logger.info(' DAG status:\n{}'.format(self.build_report))

        for client in self.clients.values():
            client.close()

        return self.build_report

    def status(self, **kwargs):
        """Returns a table with tasks status
        """
        self.render()

        return Table([t.status(**kwargs)
                      for k, t in self._dict.items()])

    def to_dict(self, include_plot=False):
        """Returns a dict representation of the dag's Tasks,
        only includes a few attributes.

        Parameters
        ----------
        include_plot: bool, optional
            If True, the path to a PNG file with the plot in "_plot"
        """
        d = {name: task.to_dict() for name, task in self._dict.items()}

        if include_plot:
            d['_plot'] = self.plot(open_image=False)

        return d

    def to_markup(self, path=None, fmt='html'):
        """Returns a str (md or html) with the pipeline's description
        """
        if fmt not in ['html', 'md']:
            raise ValueError('fmt must be html or md, got {}'.format(fmt))

        status = self.status().to_format('html')
        path_to_plot = Path(self.plot(open_image=False))
        plot = image_bytes2html(path_to_plot.read_bytes())

        template_md = importlib_resources.read_text(resources, 'dag.md')
        out = Template(template_md).render(plot=plot, status=status, dag=self)

        # TODO: syntax highlighting
        # https://github.com/lepture/mistune#renderer
        if fmt == 'html':
            if not mistune:
                raise ImportError('mistune is required to export to HTML')

            out = mistune.markdown(out, escape=False)

        if path is not None:
            Path(path).write_text(out)

        return out

    def plot(self, open_image=True, path=None):
        """Plot the DAG
        """
        # FIXME: add tests for this
        self.render()

        if not path:
            path = tempfile.mktemp(suffix='.png')

        G = self._to_graph()

        for n, data in G.nodes(data=True):
            data['color'] = 'red' if n.product._outdated() else 'green'
            data['label'] = n._short_repr()

        # https://networkx.github.io/documentation/networkx-1.10/reference/drawing.html
        # # http://graphviz.org/doc/info/attrs.html
        # NOTE: requires pygraphviz and pygraphviz
        G_ = nx.nx_agraph.to_agraph(G)
        G_.draw(path, prog='dot', args='-Grankdir=LR')

        if open_image:
            subprocess.run(['open', path])

        return path

    def _render_current(self, show_progress, force):
        # only render the first time this is called, this means that
        # if the dag is modified, render won't have an effect, DAGs are meant
        # to be all set before rendering, but might be worth raising a warning
        # if trying to modify an already rendered DAG
        if not self._rendered or force:
            g = self._to_graph(only_current_dag=True)

            tasks = nx.algorithms.topological_sort(g)

            if show_progress:
                tasks = tqdm(tasks, total=len(g))

            for t in tasks:
                if show_progress:
                    tasks.set_description('Rendering DAG "{}"'
                                          .format(self.name))

                with warnings.catch_warnings(record=True) as warnings_:
                    try:
                        t.render()
                    except Exception as e:
                        raise type(e)('While rendering a Task in {}, check '
                                      'the full '
                                      'traceback above for details'
                                      .format(self)) from e

                if warnings_:
                    messages = [str(w.message) for w in warnings_]
                    warning = ('Task "{}" had the following warnings:\n\n{}'
                               .format(repr(t), '\n'.join(messages)))
                    warnings.warn(warning)

                self._rendered = True

    def _add_task(self, task):
        """Adds a task to the DAG
        """
        if task.name in self._dict.keys():
            raise ValueError('DAGs cannot have Tasks with repeated names, '
                             'there is a Task with name "{}" '
                             'already'.format(task.name))

        if task.name is not None:
            self._dict[task.name] = task
        else:
            raise ValueError('Tasks must have a name, got None')

    def _to_graph(self, only_current_dag=False):
        """
        Converts the DAG to a Networkx DiGraph object. Since upstream
        dependencies are not required to come from the same DAG,
        this object might include tasks that are not included in the current
        object
        """
        G = nx.DiGraph()

        for task in self.values():
            G.add_node(task)

            if only_current_dag:
                G.add_edges_from([(up, task) for up
                                  in task.upstream.values() if up.dag is self])
            else:
                G.add_edges_from([(up, task) for up in task.upstream.values()])

        return G

    def __getitem__(self, key):
        return self._dict[key]

    def __iter__(self):
        # TODO: raise a warning if this any of this dag tasks have tasks
        # from other tasks as dependencies (they won't show up here)
        for name in self._dict.keys():
            yield name

    def __len__(self):
        return len(self._dict)

    def __repr__(self):
        return '{}("{}")'.format(type(self).__name__, self.name)

    def _short_repr(self):
        return repr(self)

    # IPython integration
    # https://ipython.readthedocs.io/en/stable/config/integrating.html

    def _ipython_key_completions_(self):
        return list(self)

    # __getstate__ and __setstate__ are needed to make this picklable

    def __getstate__(self):
        state = self.__dict__.copy()
        # _logger is not pickable, so we remove them and build
        # them again in __setstate__
        del state['_logger']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = logging.getLogger('{}.{}'.format(__name__,
                                                       type(self).__name__))
