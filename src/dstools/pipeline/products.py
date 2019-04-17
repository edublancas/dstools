from pathlib import Path


class Product:
    """A product is a persistent triggered by a Task
    """

    def __init__(self, identifier):
        self._identifier = identifier

        self.metadata = self._fetch_metadata()

    @property
    def identifier(self):
        return self._identifier

    @property
    def timestamp(self):
        return self.metadata.get('timestamp')

    @property
    def stored_source_code(self):
        return self.metadata.get('stored_source_code')

    @property
    def task(self):
        return self._task

    @property
    def metadata(self):
        return self._metadata

    @task.setter
    def task(self, value):
        self._task = value

    @timestamp.setter
    def timestamp(self, value):
        self.metadata['timestamp'] = value

    @stored_source_code.setter
    def stored_source_code(self, value):
        self.metadata['stored_source_code'] = value

    @metadata.setter
    def metadata(self, value):
        self._metadata = value

    def outdated_data_dependencies(self):
        def is_outdated(up_prod):
            if self.timestamp is None or up_prod.timestamp is None:
                return True
            else:
                return ((up_prod.timestamp > self.timestamp)
                        or up_prod.outdated())

        outdated = any([is_outdated(up.product) for up in self.task.upstream])

        return outdated

    def outdated_code_dependency(self):
        return self.stored_source_code != self.task.source_code

    def outdated(self):
        return (self.outdated_data_dependencies()
                or self.outdated_code_dependency())

    def _fetch_metadata(self):
        # if the product does not exist, return a metadata
        # with None in the values
        return (dict(timestamp=None, stored_source_code=None)
                if not self.exists() else self.fetch_metadata())

    def fetch_metadata(self):
        raise NotImplementedError('You have to implement this method')

    def save_metadata(self):
        raise NotImplementedError('You have to implement this method')

    def exists(self):
        raise NotImplementedError('You have to implement this method')


class File(Product):
    def __init__(self, identifier):
        # overridinfg super() since _path_to_file must be set before running
        # _fetch_metadata()

        self._identifier = identifier
        self._path_to_file = Path(self.identifier)
        self._path_to_stored_source_code = Path(str(self.path_to_file)
                                                + '.source')

        self.metadata = self._fetch_metadata()

    @property
    def path_to_file(self):
        return self._path_to_file

    @property
    def path_to_stored_source_code(self):
        return self._path_to_stored_source_code

    def fetch_metadata(self):
        # we can safely do this here since this is only run when the file
        # exists
        timestamp = self.path_to_file.stat().st_mtime

        # but we have no control over the stored code, it might be missing
        # so we check
        if self.path_to_stored_source_code.exists():
            stored_source_code = self.path_to_stored_source_code.read_text()
        else:
            stored_source_code = None

        return dict(timestamp=timestamp, stored_source_code=stored_source_code)

    def save_metadata(self):
        # timestamp automatically updates when the file is saved...

        self.path_to_stored_source_code.write_text(self.stored_source_code)

    def exists(self):
        return self.path_to_file.exists()
