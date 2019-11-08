"""
Task implementations

A Task is a unit of work that produces a persistent change (Product)
such as a bash or a SQL script
"""
from multiprocessing import Pool
import shlex
import subprocess
from subprocess import CalledProcessError
import logging
from dstools.pipeline.tasks.Task import Task
from dstools.pipeline.placeholders import PythonCodePlaceholder


class BashCommand(Task):
    """A task that runs an inline bash command
    """

    def __init__(self, source, product, dag, name=None, params=None,
                 subprocess_run_kwargs={'stderr': subprocess.PIPE,
                                        'stdout': subprocess.PIPE,
                                        'shell': True},
                 split_source_code=False):
        super().__init__(source, product, dag, name, params)
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

    @property
    def language(self):
        return 'bash'


class PythonCallable(Task):
    """A task that runs a Python callable (i.e.  a function)
    """
    SOURCECLASS = PythonCodePlaceholder

    def __init__(self, source, product, dag, name=None, params=None):
        super().__init__(source, product, dag, name, params)

    def run(self):
        if self.dag._Executor.TASKS_CAN_CREATE_CHILD_PROCESSES:
            p = Pool()
            res = p.apply_async(func=self.source._source, kwds=self.params)

            # calling this make sure we catch the exception, from the docs:
            # Return the result when it arrives. If timeout is not None and
            # the result does not arrive within timeout seconds then
            # multiprocessing.TimeoutError is raised. If the remote call
            # raised an exception then that exception will be reraised by
            # get().
            # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.AsyncResult.get
            if self.dag._Executor.STOP_ON_EXCEPTION:
                res.get()

            p.close()
            p.join()
        else:
            self.source._source(**self.params)

    @property
    def language(self):
        return 'python'


class ShellScript(Task):
    """A task to run a shell script
    """
    def __init__(self, source, product, dag, name=None, params=None,
                 client=None):
        super().__init__(source, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a client'
                             .format(type(self).__name__))

    def run(self):
        self.client.execute(str(self.source))

    @property
    def language(self):
        return 'bash'
