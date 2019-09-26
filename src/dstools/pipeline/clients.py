"""
A client reflects a connection to a system that performs the actual
computations
"""
import tempfile
from pathlib import Path
import random
import string
import logging
import atexit
import shlex
import subprocess
from subprocess import CalledProcessError

from dstools.templates.StrictTemplate import StrictTemplate

from sqlalchemy import create_engine
import paramiko

CLIENTS = []

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

        CLIENTS.append(self)

    @property
    def engine(self):
        """Returns a SQLAlchemy engine, creates one if one does not exist
        """
        if self._engine is None:
            self._engine = create_engine(self.uri)

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
            self._logger.info(f'Disposing engine {self._engine}')
            self._engine.dispose()
            self._engine = None


class ShellClient(Client):
    """Client to run command in the local shell
    """

    def __init__(self,
                 subprocess_run_kwargs={'stderr': subprocess.PIPE,
                                        'stdout': subprocess.PIPE,
                                        'shell': False}):
        """
        """
        self.subprocess_run_kwargs = subprocess_run_kwargs
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))

    def run(self, code, run_template='bash {{path_to_code}}'):
        """Run code
        """
        _, path_to_tmp = tempfile.mkstemp()
        Path(path_to_tmp).write_text(code)

        run_template = StrictTemplate(run_template)
        source = run_template.render(dict(path_to_code=path_to_tmp))

        res = subprocess.run(shlex.split(source), **self.subprocess_run_kwargs)

        if res.returncode != 0:
            # log source code without expanded params
            self._logger.info(f'{code} returned stdout: '
                              f'{res.stdout} and stderr: {res.stderr} '
                              f'and exit status {res.returncode}')
            raise CalledProcessError(res.returncode, code)
        else:
            self._logger.info(f'Finished running {self}. stdout: {res.stdout},'
                              f' stderr: {res.stderr}')


class RemoteShellClient(Client):
    """Client to run commands in a remote shell
    """

    def __init__(self, connect_kwargs, path_to_directory):
        """

        path_to_directory: str
            A path to save temporary files

        connect_kwargs: dict
            Parameters to send to the paramiko.SSHClient.connect constructor
        """
        self.path_to_directory = path_to_directory
        self.connect_kwargs = connect_kwargs
        self._raw_client = None
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))
        CLIENTS.append(self)

    @property
    def raw_client(self):
        if self._raw_client is None:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(**self.connect_kwargs)
            self._raw_client = client

        return self._raw_client

    def _random_name(self):
        filename = (''.join(random.choice(string.ascii_letters)
                            for i in range(16)))
        return filename

    def read_file(self, path):
        ftp = self.raw_client.open_sftp()

        _, path_to_tmp = tempfile.mkstemp()
        ftp.get(path, path_to_tmp)

        path_to_tmp = Path(path_to_tmp)

        content = path_to_tmp.read_text()
        path_to_tmp.unlink()

        ftp.close()

        return content

    def write_to_file(self, content, path):
        ftp = self.raw_client.open_sftp()

        _, path_to_tmp = tempfile.mkstemp()
        path_to_tmp = Path(path_to_tmp)
        path_to_tmp.write_text(content)
        ftp.put(path_to_tmp, path)

        ftp.close()
        path_to_tmp.unlink()

    def run(self, code, run_template='bash {{path_to_code}}'):
        """Run code
        """
        ftp = self.raw_client.open_sftp()
        path_remote = self.path_to_directory + self._random_name()

        _, path_to_tmp = tempfile.mkstemp()
        Path(path_to_tmp).write_text(code)

        ftp.put(path_to_tmp, path_remote)
        ftp.close()

        run_template = StrictTemplate(run_template)
        source = run_template.render(dict(path_to_code=path_remote))

        # stream stdout. related: https://stackoverflow.com/q/31834743
        # using pty is not ideal, fabric has a clean implementation for this
        # worth checking out
        stdin, stdout, stderr = self.raw_client.exec_command(source,
                                                             get_pty=True)

        for line in iter(stdout.readline, ""):
            self._logger.info('(STDOUT): {}'.format(line))

        returncode = stdout.channel.recv_exit_status()

        stdout = ''.join(stdout)
        stderr = ''.join(stderr)

        if returncode != 0:
            # log source code without expanded params
            self._logger.info(f'{code} returned stdout: '
                              f'{stdout} and stderr: {stderr} '
                              f'and exit status {returncode}')
            raise CalledProcessError(returncode, code)
        else:
            self._logger.info(f'Finished running {self}. stdout: {stdout},'
                              f' stderr: {stderr}')

        return {'returncode': returncode, 'stdout': stdout, 'stderr': stderr}

    def close(self):
        if self._raw_client is not None:
            self._logger.info(f'Closing client {self._raw_client}')
            self._raw_client.close()
            self._raw_client = None


@atexit.register
def close_all_clients():
    for client in CLIENTS:
        print(f'Closing client {client}')
        client.close()
