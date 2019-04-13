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
        return self._metadata.get('timestamp')

    @property
    def stored_source_code(self):
        return self._metadata.get('stored_source_code')

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
        if self.metadata is None:
            self.metadata = {}

        self.metadata['timestamp'] = value

    @stored_source_code.setter
    def stored_source_code(self, value):
        if self.metadata is None:
            self.metadata = {}

        self.metadata['stored_source_code'] = value

    @metadata.setter
    def metadata(self, value):
        self._metadata = value

    def outdated_data_dependencies(self):
        # check timestamps only if we have a timestamp for this product
        if self.timestamp is not None:
            outdated = any([up.product.timestamp > self.timestamp
                            for up in self.task.upstream])
            return outdated

        # if not, then mark as outdated
        else:
            return True

    def outdated_code_dependency(self):
        return self.stored_source_code != self.task.source_code

    def _fetch_metadata(self):
        # only try to fetch metata if the product exists, if it doesn't
        # exist, metadata should be None
        return None if not self.exists() else self.fetch_metadata()

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
