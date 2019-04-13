
from pathlib import Path


class Product:
    """A product is a persistent triggered by a Task
    """

    def __init__(self, identifier):
        self._identifier = identifier

        self.timestamp, self.stored_source_code = self.fetch_metadata()

    @property
    def identifier(self):
        return self._identifier

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def stored_source_code(self):
        return self._stored_source_code

    @property
    def task(self):
        return self._task

    @task.setter
    def task(self, value):
        self._task = value

    @timestamp.setter
    def timestamp(self, value):
        self._timestamp = value

    @stored_source_code.setter
    def stored_source_code(self, value):
        self._stored_source_code = value

    def outdated_data_dependencies(self):
        outdated = any([up.product.timestamp > self.timestamp
                        for up in self.task.upstream])
        return outdated

    def outdated_code_dependency(self):
        return self.stored_source_code != self.task.source_code

    def fetch_metadata(self):
        raise NotImplementedError('You have to implement this method')

    def save_metadata(self):
        raise NotImplementedError('You have to implement this method')

    def exists(self):
        raise NotImplementedError('You have to implement this method')


class PostgresRelation(Product):
    def fetch_metadata(self):
        raise NotImplementedError('You have to implement this method')

    def save_metadata(self):
        raise NotImplementedError('You have to implement this method')

    def exists(self):
        raise NotImplementedError('You have to implement this method')


class File(Product):
    def __init__(self, identifier):
        self._identifier = identifier
        self._path_to_file = Path(self.identifier)
        self._path_to_stored_source_code = Path(str(self.path_to_file)
                                                + '.source')

        self.timestamp, self.stored_source_code = self.fetch_metadata()

    @property
    def path_to_file(self):
        return self._path_to_file

    @property
    def path_to_stored_source_code(self):
        return self._path_to_stored_source_code

    def fetch_metadata(self):
        if self.path_to_file.exists():
            timestamp = self.path_to_file.stat().st_mtime
        else:
            timestamp = None

        if self.path_to_stored_source_code.exists():
            stored_source_code = self.path_to_stored_source_code.read_text()
        else:
            stored_source_code = None

        return timestamp, stored_source_code

    def save_metadata(self):
        # timestamp automatically updates when the file is saved...

        self.path_to_stored_source_code.write_text(self.stored_source_code)

    def exists(self):
        return self.path_to_file.exists()
