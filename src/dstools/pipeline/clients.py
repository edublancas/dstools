"""
A client reflects a connection to a system that performs the actual
computations
"""
import logging
import atexit
import shlex
import subprocess
from subprocess import CalledProcessError

from dstools.templates.StrictTemplate import StrictTemplate

from sqlalchemy import create_engine

ENGINES = []

# TODO: make all clients expose the same API


class Client:
    """Abstract class
    """

    def __init__(self, uri):
        self._uri = uri
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))

    def connect(self):
        raise NotImplementedError("This method must be implemented in the "
                                  "subclasses")

    @property
    def uri(self):
        return self._uri

    def run(self, code):
        """Run code
        """
        raise NotImplementedError("This method must be implemented in the "
                                  "subclasses")


class SQLAlchemyClient(Client):

    def __init__(self, uri):

        super().__init__(uri)

        self._engine = None
        # self._conn = None

    @property
    def engine(self):
        """Returns a SQLAlchemy engine, creates one if one does not exist
        """
        if self._engine is None:
            self._engine = create_engine(self.uri)
            ENGINES.append(self._engine)

        return self._engine

    def connect(self):
        """Use the engine to return a connection object
        """
        # answer on engines, connections, etc:
        # https://stackoverflow.com/a/8705750/709975
        self._conn = self.engine.connect()

        return self._conn

    def raw_connection(self):
        """Uses engine to return a raw connection
        """
        self._conn = self.engine.raw_connection()

        return self._conn

    def __del__(self):
        """Same as client.close()
        """
        self.close()

    def close(self):
        """Disposes the engine if it exists
        """
        if self._engine is not None:
            print(f'Disposing engine {self._engine}')
            self._engine.dispose()


class ShellClient(Client):
    """Client to run command in the local shell
    """

    def __init__(self, run_template='bash {{path_to_code}}',
                 subprocess_run_kwargs={'stderr': subprocess.PIPE,
                                        'stdout': subprocess.PIPE,
                                        'shell': False}):
        """
        """
        self.run_template = StrictTemplate(run_template)
        self.subprocess_run_kwargs = subprocess_run_kwargs
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))

    def run(self, code):
        """Run code
        """
        path_to_code = code.save_to_tmp_file()
        source = self.run_template.render(dict(path_to_code=path_to_code))

        res = subprocess.run(shlex.split(source), **self.subprocess_run_kwargs)

        if res.returncode != 0:
            # log source code without expanded params
            self._logger.info(f'{self.source_code} returned stdout: '
                              f'{res.stdout} and stderr: {res.stderr} '
                              f'and exit status {res.returncode}')
            raise CalledProcessError(res.returncode, self.source_code)
        else:
            self._logger.info(f'Finished running {self}. stdout: {res.stdout},'
                              f' stderr: {res.stderr}')


class RemoteShellClient(Client):
    """Client to run commands in a remote shell
    """

    def run(self, code):
        """Run code
        """
        pass


@atexit.register
def close_conns():
    for engine in ENGINES:
        print(f'Disposing engine {engine}')
        engine.dispose()
