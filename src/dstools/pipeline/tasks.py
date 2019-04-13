from dstools.pipeline import _TASKS, _NON_END_TASKS

import subprocess
from pathlib import Path
import logging
from datetime import datetime


class Task:
    """A task represents a unit of work
    """

    def __init__(self, source_code, product):
        self._upstream = []
        self._already_checked = False

        self._product = product

        if isinstance(source_code, Path):
            self._source_code = source_code.read_text()
            self._path_to_source_code = source_code
        else:
            self._source_code = source_code
            self._path_to_source_code = None

        self._logger = logging.getLogger(__name__)

        product.task = self

        _TASKS.append(self)

    @property
    def product(self):
        return self._product

    @property
    def is_end_task(self):
        return self not in _NON_END_TASKS

    @property
    def is_root_task(self):
        return not len(self._upstream)

    @property
    def source_code(self):
        return self._source_code

    @property
    def path_to_source_code(self):
        return self._path_to_source_code

    @property
    def upstream(self):
        return self._upstream

    def run(self):
        raise NotImplementedError('You have to implement this method')

    def set_upstream(self, task):
        self._upstream.append(task)

        if task not in _NON_END_TASKS:
            _NON_END_TASKS.append(task)

    def build(self):

        # first get upstreams up to date
        for task in self._upstream:
            task.build()

        # bring this task up to date
        if not self._already_checked:
            self._build()

    def _build(self):
        # NOTE: should i fetch metadata here? I need to make sure I have
        # the latest before building

        self._logger.info(f'-----\nChecking {repr(self)}....')

        run = False

        # check dependencies only if the product exists and there is metadata
        if self.product.exists() and self.product.metadata is not None:
            outdated_data_deps = self.product.outdated_data_dependencies()
            outdated_code_dep = self.product.outdated_code_dependency()

            if outdated_data_deps:
                run = True
                self._logger.info(f'Outdated data deps...')
            else:
                self._logger.info(f'Up-to-date data deps...')

            if outdated_code_dep:
                run = True
                self._logger.info(f'Outdated code dep...')
            else:
                self._logger.info(f'Up-to-date code dep...')
        else:
            self._logger.info(f'Product does not exist...')
            run = True

        if run:
            self._logger.info(f'Running {repr(self)}')

            self.run()

            # TODO: should check if job ran successfully, if not, stop execution

            # update metadata
            self.product.timestamp = datetime.now().timestamp()
            self.product.stored_source_code = self.source_code
            self.product.save_metadata()

        else:
            self._logger.info(f'No need to run {repr(self)}')

        self._logger.info(f'-----\n')

        self._already_checked = True

    def __repr__(self):
        return f'{type(self).__name__}'


class BashCommand(Task):
    """A task taht runs bash command
    """
    def run(self):
        subprocess.call(self.source_code.split(' '))

    def __repr__(self):
        return f'{type(self).__name__}: {self.source_code}'


class ScriptTask(Task):
    """A task that runs a generic script
    """
    _INTERPRETER = None

    def __init__(self, source_code, product):
        if not isinstance(source_code, Path):
            raise ValueError(f'{type(self).__name__} must be called with '
                             'a pathlib.Path object in the source_code '
                             'parameter')

        super().__init__(source_code, product)

    def run(self):
        if self._INTERPRETER is None:
            raise ValueError(f'{type(self).__name__}: subclasses must '
                             'declare an interpreter')

        subprocess.call([self._INTERPRETER, self.path_to_source_code])

    def __repr__(self):
        return f'{type(self).__name__}: {self.path_to_source_code}'


class BashScript(ScriptTask):
    """A task that runs a bash script
    """
    _INTERPRETER = 'bash'


class PythonScript(ScriptTask):
    """A task that runs a bash script
    """
    _INTERPRETER = 'python'
