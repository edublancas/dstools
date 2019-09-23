"""
Task implementations

A Task is a unit of work that produces a persistent change (Product)
such as a bash or a SQL script
"""
import shlex
import subprocess
from subprocess import CalledProcessError
import logging
from dstools.pipeline.tasks.Task import Task
from dstools.pipeline.placeholders import PythonCodePlaceholder


class BashCommand(Task):
    """A task that runs an inline bash command
    """

    def __init__(self, code, product, dag, name=None, params=None,
                 subprocess_run_kwargs={'stderr': subprocess.PIPE,
                                        'stdout': subprocess.PIPE,
                                        'shell': True},
                 split_source_code=False):
        super().__init__(code, product, dag, name, params)
        self.split_source_code = split_source_code
        self.subprocess_run_kwargs = subprocess_run_kwargs
        self._logger = logging.getLogger(__name__)

    def run(self):
        source_code = (shlex.split(self.source_code) if self.split_source_code
                       else self.source_code)

        res = subprocess.run(source_code,
                             **self.subprocess_run_kwargs)

        if res.returncode != 0:
            # log source code without expanded params
            self._logger.info(f'{self.source_code} returned stdout: '
                              f'{res.stdout} and stderr: {res.stderr} '
                              f'and exit status {res.returncode}')
            raise CalledProcessError(res.returncode, self.source_code)
        else:
            self._logger.info(f'Finished running {self}. stdout: {res.stdout},'
                              f' stderr: {res.stderr}')


class PythonCallable(Task):
    """A task that runs a Python callabel (i.e.  a function)
    """
    CODECLASS = PythonCodePlaceholder

    def __init__(self, code, product, dag, name=None, params=None):
        super().__init__(code, product, dag, name, params)

    def run(self):
        self._code._source(**self.params)


class ShellScript(Task):
    """A task to run a shell script
    """
    def __init__(self, code, product, dag, name=None, params=None,
                 client=None):
        super().__init__(code, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a client'
                             .format(type(self).__name__))

    def run(self):
        self.client.run(str(self._code))
