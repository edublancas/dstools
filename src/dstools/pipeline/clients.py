"""
A client reflects a connection to a system that performs the actual
computations
"""
import tempfile
from pathlib import Path
import random
import string
import logging
import shlex
import subprocess
from subprocess import CalledProcessError

from dstools.templates.StrictTemplate import StrictTemplate

from sqlalchemy import create_engine
import paramiko


# FIXME: make this an abstract classs (abc.ABC)
class Client:
    """
    Clients are classes that communicate with another system (usually a
    database), they provide a thin wrapper around libraries that implement
    clients to avoid managing connections directly. The most common use
    case by far is for a Task/Product to submit some code to a system,
    a client just provides a way of doing so without dealing with connection
    details.

    A Client is reponsible for making sure an open connection is available
    at any point (open a connection if none is available).

    However, clients are not strictly necessary, a Task/Product could manage
    their own client connections. For example the NotebookRunner task does have
    a Client since it only calls an external library to run.


    Notes
    -----
    Method's names were chosen to resemble the ones in the Python DB API Spec
    2.0 (PEP 249)

    """

    def __init__(self):
        self._set_logger()

    def execute(self, code):
        """Execute code
        """
        raise NotImplementedError("This method must be implemented in the "
                                  "subclasses")

    @property
    def connection(self):
        """Return a connection, open one if there isn't any
        """
        raise NotImplementedError("This method must be implemented in the "
                                  "subclasses")

    def close(self):
        """Close connection if there is one active
        """
        raise NotImplementedError("This method must be implemented in the "
                                  "subclasses")

    # __getstate__ and __setstate__ are needed to make this picklable

    def __getstate__(self):
        state = self.__dict__.copy()
        # _logger is not pickable, so we remove them and build it
        # again in __setstate__
        del state['_logger']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._set_logger()

    def _set_logger(self):
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))


class DBAPIClient(Client):
    """A client for a module following the PEP 214 DB API spec
    """

    def __init__(self, connect_fn, **connect_kwargs):
        super().__init__()
        self.connect_fn = connect_fn
        self.connect_kwargs = connect_kwargs

        # there is no open connection by default
        self._connection = None

    def execute(self, code):
        """Execute code with the existing connection
        """
        cur = self.connection.cursor()
        cur.execute(code)
        self.connection.commit()
        cur.close()

    @property
    def connection(self):
        """Return a connection, open one if there isn't any
        """
        # if there isn't an open connection, open one...
        if self._connection is None:
            self._connection = self.connect_fn(**self.connect_kwargs)

        return self._connection

    def close(self):
        """Close connection if there is one active
        """
        if self._connection is not None:
            self._connection.close()


class DrillClient(Client):
    def __init__(self, params=dict(host='localhost', port=8047)):
        self.params = params
        self._set_logger()
        self.drill = None

    def run(self, code):
        """Run code
        """
        if self.drill is None:
            from pydrill.client import PyDrill
            self.drill = PyDrill(**self.params)

        return self.drill.query(code)


class SQLAlchemyClient(Client):
    """Client for connecting with any SQLAlchemy supported database

    """

    def __init__(self, uri):
        super().__init__()
        self._uri = uri
        self._engine = None
        self._connection = None

    @property
    def engine(self):
        """Returns a SQLAlchemy engine
        """
        if self._engine is None:
            self._engine = create_engine(self._uri)

        return self._engine

    @property
    def connection(self):
        """Return a connection from the pool
        """
        # we have to keep this reference here,
        # if we just return self.engine.raw_connection(),
        # any cursor from that connection will fail
        # doing: engine.raw_connection().cursor().execute('') fails!
        if self._connection is None:
            self._connection = self.engine.raw_connection()

        # if a task or product calls client.connection.close(), we have to
        # re-open the connection
        if not self._connection.is_valid:
            self._connection = self.engine.raw_connection()

        return self._connection

    def close(self):
        """Closes all connections
        """
        self._logger.info(f'Disposing engine {self._engine}')
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def execute(self, code):
        cur = self.connection.cursor()
        cur.execute(code)
        self.connection.commit()
        cur.close()

    # __getstate__ and __setstate__ are needed to make this picklable

    def __getstate__(self):
        state = self.__dict__.copy()
        # _logger is not pickable, so we remove them and build it
        # again in __setstate__
        del state['_logger']
        del state['_engine']
        del state['_connection']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._set_logger()


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
        # CLIENTS.append(self)

    @property
    def raw_client(self):
        # client has not been created
        if self._raw_client is None:
            self._raw_client = paramiko.SSHClient()
            self._raw_client.set_missing_host_key_policy(
                paramiko.AutoAddPolicy())
            self._raw_client.connect(**self.connect_kwargs)

        # client has been created but still have to check if it's active:
        else:

            is_active = False

            # this might not always work: https://stackoverflow.com/a/28288598
            if self._raw_client.get_transport() is not None:
                is_active = self._raw_client.get_transport().is_active()

            if not is_active:
                self._raw_client.connect(**self.connect_kwargs)

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
