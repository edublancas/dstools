"""
Clients that communicate with databases
"""

from dstools.pipeline.clients.Client import Client

from sqlalchemy import create_engine


class DBAPIClient(Client):
    """A client for a module following the PEP 214 DB API spec
    """

    def __init__(self, connect_fn, **connect_kwargs):
        super().__init__()
        self.connect_fn = connect_fn
        self.connect_kwargs = connect_kwargs

        # there is no open connection by default
        self._connection = None

    @property
    def connection(self):
        """Return a connection, open one if there isn't any
        """
        # if there isn't an open connection, open one...
        if self._connection is None:
            self._connection = self.connect_fn(**self.connect_kwargs)

        return self._connection

    def execute(self, code):
        """Execute code with the existing connection
        """
        cur = self.connection.cursor()
        cur.execute(code)
        self.connection.commit()
        cur.close()

    def close(self):
        """Close connection if there is one active
        """
        if self._connection is not None:
            self._connection.close()


class SQLAlchemyClient(Client):
    """Client for connecting with any SQLAlchemy supported database

    """

    def __init__(self, uri):
        super().__init__()
        self._uri = uri
        self._engine = None
        self._connection = None

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

    def execute(self, code):
        cur = self.connection.cursor()
        cur.execute(code)
        self.connection.commit()
        cur.close()

    def close(self):
        """Closes all connections
        """
        self._logger.info(f'Disposing engine {self._engine}')
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    @property
    def engine(self):
        """Returns a SQLAlchemy engine
        """
        if self._engine is None:
            self._engine = create_engine(self._uri)

        return self._engine

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


class DrillClient(Client):
    def __init__(self, params=dict(host='localhost', port=8047)):
        self.params = params
        self._set_logger()
        self._connection = None

    @property
    def connection(self):
        from pydrill.client import PyDrill

        if self._connection is None:
            self._connection = PyDrill(**self.params)

        return self._connection

    def execute(self, code):
        """Run code
        """
        return self.connection.query(code)

    def close(self):
        pass
