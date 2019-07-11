"""
Task abstract class
"""
import traceback
from copy import copy
import logging
from datetime import datetime
from dstools.pipeline.tasks import util
from dstools.pipeline.products import Product, MetaProduct
from dstools.pipeline.build_report import BuildReport
from dstools.pipeline.dag import DAG
from dstools.pipeline.exceptions import TaskBuildError
from dstools.pipeline.tasks.TaskGroup import TaskGroup
from dstools.pipeline.identifiers import ClientCode
from dstools.util import isiterable


class Task:
    """A task represents a unit of work

    """
    CODECLASS = ClientCode
    PRODUCT_CLASSES_ALLOWED = None

    def __init__(self, code, product, dag, name=None, params=None):
        if self.PRODUCT_CLASSES_ALLOWED is not None:
            if not isinstance(product, self.PRODUCT_CLASSES_ALLOWED):
                raise TypeError('{} only supports the following product '
                                'classes: {}, got {}'
                                .format(type(self).__name__,
                                        self.PRODUCT_CLASSES_ALLOWED,
                                        type(product).__name__))

        self._upstream = {}

        self.params = params or {}
        self.build_report = None

        self._code = self.CODECLASS(code)

        if isinstance(product, Product):
            self._product = product
        else:
            self._product = MetaProduct(product)

        self._name = name or product.name

        self._logger = logging.getLogger(__name__)

        self.product.task = self

        dag.add_task(self)
        self.dag = dag

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
        return self._upstream

    def run(self):
        raise NotImplementedError('You have to implement this method')

    def set_upstream(self, other):
        if isiterable(other) and not isinstance(other, DAG):
            for o in other:
                self._upstream[o.name] = o
        else:
            self._upstream[other.name] = other

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
        elapsed = None

        # check dependencies only if the product exists and there is metadata
        if self.product.exists() and self.product.metadata is not None:
            outdated_data_deps = self.product.outdated_data_dependencies()
            outdated_code_dep = self.product.outdated_code_dependency()

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
            self.product.pre_save_metadata_hook()
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

        self.build_report = BuildReport(run=run, elapsed=elapsed)

        return self

    def status(self):
        """Prints the current task status
        """
        p = self.product

        outd_code = p.outdated_code_dependency()

        out = ''

        if p.timestamp is not None:
            dt = (datetime
                  .fromtimestamp(p.timestamp).strftime('%b %m, %y at %H:%M'))
            out += f'* Last updated: {dt}\n'
        else:
            out += f'* Timestamp is None\n'

        out += f'* Oudated data dependencies: {p.outdated_data_dependencies()}'
        out += f'\n* Oudated code dependency: {outd_code}'

        if outd_code:
            out += '\n\nCODE DIFF\n*********\n'
            out += util.diff_strings(p.stored_source_code, self.source_code)
            out += '\n*********'

        print(out)
        return out

    def render(self):
        """
        Renders code and product, all upstream tasks must have been rendered
        first, for that reason, this method will usually not be called
        directly but via DAG.render(), which renders in the right order
        """
        # add upstream product identifiers to params, if any
        if self.upstream:
            self.params['upstream'] = {n: t.product.identifier for n, t
                                       in self.upstream.items()}

        # render the current product
        try:
            self.product.render(copy(self.params))
        except Exception as e:
            traceback.print_exc()
            raise RuntimeError(f'Error rendering product {repr(self.product)} '
                               f'from task {repr(self)} with params '
                               f'{self.params}. Exception: {e}')

        self.params['product'] = self.product.identifier

        self._code.render(copy(self.params))

    def __repr__(self):
        return f'{type(self).__name__}: {self.name} -> {repr(self.product)}'

    def short_repr(self):
        def short(s):
            max_l = 30
            return s if len(s) <= max_l else s[:max_l-3]+'...'

        return f'{short(self.name)} -> \n{short(self.product.short_repr())}'

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
