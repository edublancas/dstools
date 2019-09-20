"""
A client reflects a connection to a system that performs the actual
computations
"""
import atexit
from sqlalchemy import create_engine

ENGINES = []


class Client:
    """Abstract class
    """

    def __init__(self, uri):
        self._uri = uri

    def connect(self):
        raise NotImplementedError("This method must be implemented in the "
                                  "subclasses")

    @property
    def uri(self):
        return self._uri


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


@atexit.register
def close_conns():
    for engine in ENGINES:
        print(f'Disposing engine {engine}')
        engine.dispose()
