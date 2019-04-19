import inspect
import shlex
import subprocess
from subprocess import CalledProcessError
from pathlib import Path
import logging
from datetime import datetime


class Task:
    """A task represents a unit of work

    Parameters
    ----------
    code: callable, Path, str
    """
    def __init__(self, code, product, dag, name=None):
        self._upstream = []

        self._code = code
        self._product = product

        self._set_name(name)
        self._set_source_code()

        self._logger = logging.getLogger(__name__)

        product.task = self

        dag.add_task(self)

    def _set_name(self, name):
        if name is not None:
            self._name = name
        elif callable(self.code):
            self._name = self.code.__name__
        elif isinstance(self.code, str):
            if len(self.code) < 60:
                self._name = self.code
            else:
                self._name = self.code[:60]+'...'
        elif isinstance(self.code, Path):
            self._name = self.code

    def _set_source_code(self):
        if callable(self.code):
            # TODO: i think this doesn't work sometime and dill has a function
            # that covers more use cases, check
            self._source_code = inspect.getsource(self.code)
        elif isinstance(self.code, str):
            self._source_code = self.code
        elif isinstance(self.code, Path):
            self._source_code = self.code.read_text()

    @property
    def name(self):
        return self._name

    @property
    def source_code(self):
        return self._source_code

    @property
    def product(self):
        return self._product

    @property
    def code(self):
        return self._code

    @property
    def upstream(self):
        return self._upstream

    def run(self):
        raise NotImplementedError('You have to implement this method')

    def set_upstream(self, task):
        self._upstream.append(task)

    def build(self, force=False):
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

            self.run()

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

        else:
            self._logger.info(f'No need to run {repr(self)}')

        self._logger.info('-----\n')

    def __repr__(self):
        return f'{type(self).__name__}: {self.name}'


class BashCommand(Task):
    """A task taht runs bash command
    """

    def __init__(self, code, product, dag, name=None, params=None):
        super().__init__(code, product, dag, name)
        self._params = params

    def run(self):
        # quote params to make them safe
        quoted = {k: shlex.quote(str(v)) for k, v in self._params.items()}
        source_code = self.source_code.format(**quoted)
        res = subprocess.run(shlex.split(source_code),
                             stderr=subprocess.PIPE,
                             stdout=subprocess.PIPE)

        if res.returncode != 0:
            # log source code without expanded params
            self._logger.info(f'{self.source_code} returned stdout: '
                              f'{res.stdout} and stderr: {res.stderr} '
                              f'and exit status {res.returncode}')
            raise CalledProcessError(res.returncode, self.source_code)


class ScriptTask(Task):
    """A task that runs a generic script
    """
    _INTERPRETER = None

    def __init__(self, code, product, dag, name=None):
        if not isinstance(code, Path):
            raise ValueError(f'{type(self).__name__} must be called with '
                             'a pathlib.Path object in the code '
                             'parameter')

        super().__init__(code, product, dag, name)

    def run(self):
        if self._INTERPRETER is None:
            raise ValueError(f'{type(self).__name__}: subclasses must '
                             'declare an interpreter')

        subprocess.run([self._INTERPRETER,
                        shlex.quote(str(self.source_code))],
                       stderr=subprocess.PIPE,
                       stdout=subprocess.PIPE,
                       check=True)


class BashScript(ScriptTask):
    """A task that runs a bash script
    """
    _INTERPRETER = 'bash'


class PythonScript(ScriptTask):
    """A task that runs a python script
    """
    _INTERPRETER = 'python'


class PythonCallable(Task):
    def __init__(self, code, product, dag, name=None, kwargs={}):
        super().__init__(code, product, dag, name)
        self.kwargs = kwargs

    def run(self):
        self.code(**self.kwargs)
