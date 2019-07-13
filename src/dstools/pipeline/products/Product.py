"""
A Product specifies a persistent object in disk such as a file in the local
filesystem or an table in a database. Each Product is uniquely identified,
for example a file can be specified using a absolute path, a table can be
fully specified by specifying a database, a schema and a name. Names
are lazy evaluated, they can be built from templates
"""
import logging
import warnings
# from dstools.pipeline.identifiers import StringIdentifier


class Product:
    """
    A product is a persistent triggered by a Task, this is an abstract
    class for all products
    """
    IDENTIFIERCLASS = None

    def __init__(self, identifier):
        self._identifier = self.IDENTIFIERCLASS(identifier)
        self.tests, self.checks = [], []
        self.did_download_metadata = False
        self.task = None
        self.logger = logging.getLogger(__name__)

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
        if self._task is None:
            raise ValueError('This product has not been assigned to any Task')

        return self._task

    @property
    def metadata(self):
        if self.did_download_metadata:
            return self._metadata
        else:
            self.get_metadata()
            self.did_download_metadata = True
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
            """
            A task becomes data outdated if an upstream product has a higher
            timestamp or if an upstream product is outdated
            """
            if self.timestamp is None or up_prod.timestamp is None:
                return True
            else:
                return ((up_prod.timestamp > self.timestamp)
                        or up_prod.outdated())

        outdated = any([is_outdated(up.product) for up
                        in self.task.upstream.values()])

        return outdated

    def outdated_code_dependency(self):
        return self.stored_source_code != self.task.source_code

    def outdated(self):
        return (self.outdated_data_dependencies()
                or self.outdated_code_dependency())

    def get_metadata(self):
        """
        This method calls Product.fetch_metadata() (provided by subclasses),
        if some conditions are met, then it saves it in Product.metadata
        """
        metadata_empty = dict(timestamp=None, stored_source_code=None)
        # if the product does not exist, return a metadata
        # with None in the values
        if not self.exists():
            self.metadata = metadata_empty
        else:
            metadata = self.fetch_metadata()

            if metadata is None:
                self.metadata = metadata_empty
            else:
                # FIXME: we need to further validate this, need to check
                # that this is an instance of mapping, if yes, then
                # check keys [timestamp, stored_source_code], check
                # types and fill with None if any of the keys is missing
                self.metadata = metadata

    def test(self):
        """Run tests, raise exceptions if any of these are not true
        """
        for fn in self.tests:
            res = fn(self)
            if not res:
                raise AssertionError(f'{self} failed test: {fn}')

    def check(self):
        """
        Run checks, this are just for diagnostic purposes, if a any returns
        False, a warning is sent
        """
        for fn in self.checks:
            if not fn(self):
                warnings.warn(f'Check did not pass: {fn}')

    def pre_save_metadata_hook(self):
        pass

    def render(self, params, **kwargs):
        """
        Render Product - this will render contents of Templates used as
        identifier for this Product, if a regular string was passed, this
        method has no effect
        """
        self._identifier.render(params, **kwargs)

    def __str__(self):
        return str(self.identifier)

    def __repr__(self):
        return f'{type(self).__name__}({repr(self.identifier)})'

    def short_repr(self):
        return f'{self.identifier}'

    # Subclasses must implement the following methods

    def fetch_metadata(self):
        raise NotImplementedError('You have to implement this method')

    def save_metadata(self):
        raise NotImplementedError('You have to implement this method')

    def exists(self):
        """
        This method returns True if the product exists, it is not part
        of the metadata, so there is no cached status
        """
        raise NotImplementedError('You have to implement this method')

    def delete(self, force=False):
        """Deletes the product
        """
        raise NotImplementedError('You have to implement this method')

    @property
    def name(self):
        raise NotImplementedError('You have to implement this property')
