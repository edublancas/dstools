"""
Task abstract class

A Task is a unit of work, it has associated source code and
a product (a persistent object such as a table in a database),
it has a name (which can be infered from the source code filename)
and lives in a DAG
"""
from copy import copy
import logging
from datetime import datetime
from dstools.pipeline.tasks import util
from dstools.pipeline.products import Product, MetaProduct
from dstools.pipeline.dag import DAG
from dstools.exceptions import TaskBuildError, RenderError
from dstools.pipeline.tasks.TaskGroup import TaskGroup
from dstools.pipeline.tasks.TaskStatus import TaskStatus
from dstools.pipeline.tasks.Params import Params
from dstools.pipeline.placeholders import (ClientCodePlaceholder,
                                           TemplatedPlaceholder)
from dstools.pipeline.Table import Row
from dstools.util import isiterable

import humanize


class Task:
    """A task represents a unit of work

    """
    CODECLASS = ClientCodePlaceholder
    PRODUCT_CLASSES_ALLOWED = None
    PRODUCT_IN_CODE = True

    def __init__(self, code, product, dag, name=None, params=None):
        if self.PRODUCT_CLASSES_ALLOWED is not None:
            if not isinstance(product, self.PRODUCT_CLASSES_ALLOWED):
                raise TypeError('{} only supports the following product '
                                'classes: {}, got {}'
                                .format(type(self).__name__,
                                        self.PRODUCT_CLASSES_ALLOWED,
                                        type(product).__name__))

        self._upstream = Params()

        self.params = params or {}
        self.build_report = None

        self._code = self.CODECLASS(code)

        if isinstance(product, Product):
            self._product = product
        else:
            # if assigned a tuple/list of products, create a MetaProduct
            self._product = MetaProduct(product)

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

        self._status = TaskStatus.WaitingRender

    @property
    def name(self):
        return self._name

    @property
    def source_code(self):
        return str(self._code)

    @property
    def product(self):
        return self._product

    @property
    def upstream(self):
        """{'task_name': task} dict
        """
        # always return a copy to prevent global state if contents
        # are modified (e.g. by using pop)
        return copy(self._upstream)

    def run(self):
        raise NotImplementedError('You have to implement this method')

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

            self.run()

            now = datetime.now()
            elapsed = (now - then).total_seconds()
            self._logger.info(f'Done. Operation took {elapsed:.1f} seconds')

            # TODO: should check if job ran successfully, if not,
            # stop execution

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
                    and isinstance(self._code, TemplatedPlaceholder)):
                with params.get('upstream'):
                    self._code.render(params, optional=opt)
            else:
                self._code.render(params, optional=opt)
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
            data['Code diff'] = util.diff_strings(p.stored_source_code,
                                                  self.source_code)
        else:
            outd_code = ''

        data['Product'] = str(self.product)
        data['Doc (short)'] = self._code.doc_short
        data['Location'] = self._code.loc

        return Row(data)

    def _render_product(self):
        params_names = list(self.params)

        # add upstream product identifiers to params, if any
        if self.upstream:
            self.params['upstream'] = Params({n: t.product for n, t
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
