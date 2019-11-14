"""
Task abstract class

A Task is a unit of work, it has associated source code and
a product (a persistent object such as a table in a database),
it has a name (which can be infered from the source code filename)
and lives in a DAG

[WIP] On subclassing Tasks

Implementation details:

* params (dict), upstream (Param object)

Required:

* params vs constructor parameters
* params on render vs params on run
* Implementing Task.run (using the _code object, product, TaskBuildError)

Optional:

* Validating PRODUCT_CLASSES_ALLOWED
* Validating SOURCECLASS
* Validating upstream, product and params in code
* Using a client parameter
* The language property

"""
import inspect
import abc
import traceback
from copy import copy
import logging
from datetime import datetime
from dstools.pipeline.products import Product, MetaProduct
from dstools.pipeline.dag import DAG
from dstools.exceptions import TaskBuildError, RenderError
from dstools.pipeline.tasks.TaskGroup import TaskGroup
from dstools.pipeline.constants import TaskStatus
from dstools.pipeline.tasks.Upstream import Upstream
from dstools.pipeline.placeholders import (ClientCodePlaceholder,
                                           TemplatedPlaceholder)
from dstools.pipeline.Table import Row
from dstools.util import isiterable

import humanize


class Task(abc.ABC):
    """A task represents a unit of work
    """
    SOURCECLASS = ClientCodePlaceholder
    PRODUCT_CLASSES_ALLOWED = None
    PRODUCT_IN_CODE = True

    def __init__(self, source, product, dag, name=None, params=None):
        """
        All subclasses must implement the same constuctor to keep the API
        consistent, optional parameters after "params" are ok

        Parameters
        ----------
        source: str or pathlib.Path
            Source code for the task, for tasks that do not take source code
            as input (such as PostgresCopy), this can be other thing. The
            source can be a template and can make references to any parameter
            in "params", "upstream" parameters or its own "product", not all
            Tasks have templated source (templating code is mostly used by
            Tasks that take SQL source code as input)
        product: Product
            The product that this task will create upon completion
        dag: DAG
            The DAG holding this task
        name: str
            A name for this task, if None a default will be assigned
        params: dict
            Extra parameters passed to the task on rendering (if templated
            source) or during execution (if not templated source)
        """
        self._upstream = Upstream()
        self._params = params or {}

        self.build_report = None

        self._source = self.SOURCECLASS(source)

        if isinstance(product, Product):
            self._product = product

            if self.PRODUCT_CLASSES_ALLOWED is not None:
                if not isinstance(self._product, self.PRODUCT_CLASSES_ALLOWED):
                    raise TypeError('{} only supports the following product '
                                    'classes: {}, got {}'
                                    .format(type(self).__name__,
                                            self.PRODUCT_CLASSES_ALLOWED,
                                            type(self._product).__name__))
        else:
            # if assigned a tuple/list of products, create a MetaProduct
            self._product = MetaProduct(product)

            if self.PRODUCT_CLASSES_ALLOWED is not None:
                if not all(isinstance(p, self.PRODUCT_CLASSES_ALLOWED)
                           for p in self._product):
                    raise TypeError('{} only supports the following product '
                                    'classes: {}, got {}'
                                    .format(type(self).__name__,
                                            self.PRODUCT_CLASSES_ALLOWED,
                                            type(self._product).__name__))

        # if passed a name, just use it
        if name is not None:
            self._name = name
        # otherwise, try to infer it from the product
        else:
            # temporary assign a Name, since repr depends on it and will
            # be needed if render fails
            self._name = None

            # at this point the product has not been rendered but we can do
            # so if it only depends on params and not on upstream, try it
            try:
                self._render_product()
            except RenderError:
                raise RenderError('name can only be None if the Product '
                                  'does not depend on upstream parameters, '
                                  'in which case, the Task gets assigned '
                                  'the same name as the Product')
            else:
                self._name = self.product.name

        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))

        self.product.task = self

        dag._add_task(self)
        self.dag = dag
        self.client = None

        self._status = TaskStatus.WaitingRender

        self._on_finish = None
        self._on_failure = None

    @property
    def language(self):
        # this is used for determining how to normalize code before comparing
        return None

    @property
    def name(self):
        """A str that represents the name of the task
        """
        return self._name

    @property
    def source(self):
        """
        A code object which represents what will be run upn task execution,
        for tasks that do not take source code as parameter (such as
        PostgresCopy), the source object will be a different thing
        """
        return self._source

    @property
    def source_code(self):
        """
        A str with the source for that this task will run on execution, if
        templated, it is only available after rendering
        """
        return str(self.source)

    @property
    def product(self):
        """The product this task will create upon execution
        """
        return self._product

    @property
    def upstream(self):
        """{task names} -> [task objects] mapping for upstream dependencies
        """
        # always return a copy to prevent global state if contents
        # are modified (e.g. by using pop)
        return copy(self._upstream)

    @property
    def params(self):
        """
        dict that holds the parameter that will be passed to the task upon
        execution. Before rendering, this will only hold parameters passed
        in the Task constructor. After rendering, this will hold new keys:
        "product" contained the rendered product and "upstream" holding
        upstream parameters if there is any
        """
        return self._params

    @property
    def _lineage(self):
        """
        Set with task names of all the dependencies for this task
        (including dependencies of dependencies)
        """
        # if no upstream deps, there is no lineage
        if not len(self.upstream):
            return None
        else:
            # retrieve lineage: upstream tasks + lineage from upstream tasks
            up = list(self.upstream.keys())
            lineage_up = [up._lineage for up in self.upstream.values() if
                          up._lineage]
            lineage = up + [task for lineage in lineage_up for task in lineage]
            return set(lineage)

    @property
    def on_finish(self):
        """
        Callable to be executed after this task is built successfully
        (passes Task as parameter)
        """
        return self._on_finish

    @on_finish.setter
    def on_finish(self, value):
        self._on_finish = value

    @property
    def on_failure(self):
        """
        Callable to be executed if task fails (passes Task as first parameter
        and the exception as second parameter)
        """
        return self._on_failure

    @on_failure.setter
    def on_failure(self, value):
        self._on_failure = value

    @abc.abstractmethod
    def run(self):
        pass

    def build(self, force=False):
        """Run the task if needed by checking its dependencies

        Returns
        -------
        dict
            A dictionary with keys 'run' and 'elapsed'
        """

        # NOTE: should i fetch metadata here? I need to make sure I have
        # the latest before building

        self._logger.info(f'-----\nChecking {repr(self)}....')

        run = False
        elapsed = 0

        # check dependencies only if the product exists and there is metadata
        if self.product.exists() and self.product.metadata is not None:
            outdated_data_deps = self.product._outdated_data_dependencies()
            outdated_code_dep = self.product._outdated_code_dependency()

            if outdated_data_deps:
                run = True
                self._logger.info('Outdated data deps...')
            else:
                self._logger.info('Up-to-date data deps...')

            if outdated_code_dep:
                run = True
                self._logger.info('Outdated code dep...')
            else:
                self._logger.info('Up-to-date code dep...')
        else:
            self._logger.info('Product does not exist...')
            run = True

        if run or force:
            if force:
                self._logger.info('Forcing...')

            self._logger.info(f'Running {repr(self)}')

            then = datetime.now()

            try:
                self.run()
            except Exception as e:
                tb = traceback.format_exc()

                if self.on_failure:
                    try:
                        self.on_failure(self, tb)
                    except Exception:
                        self._logger.exception('Error executing on_failure '
                                               'callback')
                raise e

            now = datetime.now()
            elapsed = (now - then).total_seconds()
            self._logger.info(f'Done. Operation took {elapsed:.1f} seconds')

            # update metadata
            self.product.timestamp = datetime.now().timestamp()
            self.product.stored_source_code = self.source_code
            self.product.save_metadata()

            # TODO: also check that the Products were updated:
            # if they did not exist, they must exist now, if they alredy
            # exist, timestamp must be recent equal to the datetime.now()
            # used. maybe run fetch metadata again and validate?

            if not self.product.exists():
                raise TaskBuildError(f'Error building task "{self}": '
                                     'the task ran successfully but product '
                                     f'"{self.product}" does not exist yet '
                                     '(task.product.exist() returned False)')

            if self.on_finish:
                try:
                    if 'client' in inspect.getfullargspec(self.on_finish).args:
                        self.on_finish(self, client=self.client)
                    else:
                        self.on_finish(self)

                except Exception as e:
                    raise TaskBuildError('Exception when running on_finish '
                                         'for task {}: {}'.format(self, e))

        else:
            self._logger.info(f'No need to run {repr(self)}')

        self._logger.info('-----\n')

        self._status = TaskStatus.Executed

        for t in self._get_downstream():
            t._update_status()

        self.build_report = Row({'name': self.name, 'Ran?': run,
                                 'Elapsed (s)': elapsed, })

        return self

    def render(self):
        """
        Renders code and product, all upstream tasks must have been rendered
        first, for that reason, this method will usually not be called
        directly but via DAG.render(), which renders in the right order
        """
        self._render_product()

        self.params['product'] = self.product

        params = copy(self.params)

        # most parameters are required, if upstream is not used, it should not
        # have any dependencies, if any param is not used, it should not
        # exist, the product should exist only for specific cases
        opt = set(('product',)) if not self.PRODUCT_IN_CODE else set()
        try:
            # if this task has upstream dependencies, render using the
            # context manager, which will raise a warning if any of the
            # dependencies is not used, otherwise just render, also
            # check if the code is a TemplatedPlaceholder, for other
            # types of code objects we cannot determine parameter
            # use at render time
            if (params.get('upstream')
                    and isinstance(self.source, TemplatedPlaceholder)):
                with params.get('upstream'):
                    self.source.render(params, optional=opt)
            else:
                self.source.render(params, optional=opt)
        except Exception as e:
            raise type(e)('Error rendering code from Task "{}", '
                          ' check the full traceback above for details'
                          .format(repr(self), self.params)) from e

        self._status = (TaskStatus.WaitingExecution if not self.upstream
                        else TaskStatus.WaitingUpstream)

    def set_upstream(self, other):
        if isiterable(other) and not isinstance(other, DAG):
            for o in other:
                self._upstream[o.name] = o
        else:
            self._upstream[other.name] = other

    def plan(self):
        """Shows a text summary of what this task will execute
        """

        plan = f"""
        Input parameters: {self.params}
        Product: {self.product}

        Source code:
        {self.source_code}
        """

        print(plan)

    def status(self, return_code_diff=False):
        """Prints the current task status
        """
        p = self.product

        data = {}

        data['name'] = self.name

        if p.timestamp is not None:
            dt = datetime.fromtimestamp(p.timestamp)
            date_h = dt.strftime('%b %d, %y at %H:%M')
            time_h = humanize.naturaltime(dt)
            data['Last updated'] = '{} ({})'.format(time_h, date_h)
        else:
            data['Last updated'] = 'Has not been run'

        data['Outdated dependencies'] = p._outdated_data_dependencies()
        outd_code = p._outdated_code_dependency()
        data['Outdated code'] = outd_code

        if outd_code and return_code_diff:
            data['Code diff'] = (self.dag
                                 .differ
                                 .get_diff(p.stored_source_code,
                                           self.source_code,
                                           language=self.language))
        else:
            outd_code = ''

        data['Product'] = str(self.product)
        data['Doc (short)'] = self.source.doc_short
        data['Location'] = self.source.loc

        return Row(data)

    def to_dict(self):
        """
        Returns a dict representation of the Task, only includes a few
        attributes
        """
        return dict(name=self.name, product=str(self.product),
                    source_code=self.source_code)

    def _render_product(self):
        params_names = list(self.params)

        # add upstream product identifiers to params, if any
        if self.upstream:
            self.params['upstream'] = Upstream({n: t.product for n, t
                                                in self.upstream.items()})

        # render the current product
        try:
            # using the upstream products to define the current product
            # is optional, using the parameters passed in params is also
            # optional
            self.product.render(copy(self.params),
                                optional=set(params_names + ['upstream']))
        except Exception as e:
            raise type(e)('Error rendering Product from Task "{}", '
                          ' check the full traceback above for details'
                          .format(repr(self), self.params)) from e

    def _get_downstream(self):
        downstream = []
        for t in self.dag.values():
            if self in t.upstream.values():
                downstream.append(t)
        return downstream

    def _update_status(self):
        if self._status == TaskStatus.WaitingUpstream:
            all_upstream_executed = all([t._status == TaskStatus.Executed
                                         for t in self.upstream.values()])

            if all_upstream_executed:
                self._status = TaskStatus.WaitingExecution

    def __rshift__(self, other):
        """ a >> b is the same as b.set_upstream(a)
        """
        other.set_upstream(self)
        # return other so a >> b >> c works
        return other

    def __add__(self, other):
        """ a + b means TaskGroup([a, b])
        """
        if isiterable(other) and not isinstance(other, DAG):
            return TaskGroup([self] + list(other))
        else:
            return TaskGroup((self, other))

    def __repr__(self):
        return f'{type(self).__name__}: {self.name} -> {repr(self.product)}'

    def __str__(self):
        return str(self.product)

    def _short_repr(self):
        def short(s):
            max_l = 30
            return s if len(s) <= max_l else s[:max_l - 3] + '...'

        return f'{short(self.name)} -> \n{self.product._short_repr()}'

    # __getstate__ and __setstate__ are needed to make this picklable

    def __getstate__(self):
        state = self.__dict__.copy()
        # _logger is not pickable, so we remove them and build
        # them again in __setstate__
        del state['_logger']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))
