import shlex
import subprocess
from subprocess import CalledProcessError
from pathlib import Path
import logging
from datetime import datetime


class Task:
    """A task represents a unit of work
    """
    # FIXME: source_code can really be many things, a path to a file
    # a string with source code, a python callabel, rename
    def __init__(self, source_code, product, dag, name=None):
        self.name = name

        self._upstream = []

        self._product = product

        if isinstance(source_code, Path):
            self._source_code = source_code.read_text()
            self._path_to_source_code = source_code
        else:
            self._source_code = source_code
            self._path_to_source_code = None

        if self.path_to_source_code is None and self.name is None:
            ValueError('If you pass the code directly (instead of a Path '
                       'object you have to provide a name in the constructor')

        self._logger = logging.getLogger(__name__)

        product.task = self

        dag.add_task(self)

    @property
    def product(self):
        return self._product

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
        if self.path_to_source_code is not None:
            return f'{type(self).__name__}: {self.path_to_source_code}'
        else:
            return f'{type(self).__name__}: {self.name}'


class BashCommand(Task):
    """A task taht runs bash command
    """

    def __init__(self, source_code, product, dag, name=None, params=None):
        super().__init__(source_code, product, dag, name)
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

    def __init__(self, source_code, product, dag, name=None):
        if not isinstance(source_code, Path):
            raise ValueError(f'{type(self).__name__} must be called with '
                             'a pathlib.Path object in the source_code '
                             'parameter')

        super().__init__(source_code, product, dag, name)

    def run(self):
        if self._INTERPRETER is None:
            raise ValueError(f'{type(self).__name__}: subclasses must '
                             'declare an interpreter')

        subprocess.run([self._INTERPRETER,
                        shlex.quote(str(self.path_to_source_code))],
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
    def __init__(self, source_code, product, dag, name=None, kwargs={}):
        super().__init__(source_code, product, dag, name)
        self.kwargs = kwargs

    def run(self):
        self.source_code(**self.kwargs)
