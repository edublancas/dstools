"""
A Product specifies a persistent object in disk such as a file in the local
filesystem or an table in a database. Each Product is uniquely identified,
for example a file can be specified using a absolute path, a table can be
fully specified by specifying a database, a schema and a name. Names
are lazy evaluated, they can be built from templates
"""
import abc
import logging
from math import ceil


class Product(abc.ABC):
    """
    A product is a persistent triggered by a Task, this is an abstract
    class for all products
    """
    def __init__(self, identifier):
        self._identifier = self._init_identifier(identifier)

        if self._identifier is None:
            raise TypeError('_init_identifier must return a value, returned '
                            'None')

        self.did_download_metadata = False
        self.task = None
        self.logger = logging.getLogger('{}.{}'.format(__name__,
                                                       type(self).__name__))

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
            self._get_metadata()
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

    def render(self, params, **kwargs):
        """
        Render Product - this will render contents of Templates used as
        identifier for this Product, if a regular string was passed, this
        method has no effect
        """
        self._identifier.render(params, **kwargs)

    def _outdated(self):
        return (self._outdated_data_dependencies()
                or self._outdated_code_dependency())

    def _outdated_data_dependencies(self):
        def is_outdated(up_prod):
            """
            A task becomes data outdated if an upstream product has a higher
            timestamp or if an upstream product is outdated
            """
            if self.timestamp is None or up_prod.timestamp is None:
                return True
            else:
                return ((up_prod.timestamp > self.timestamp)
                        or up_prod._outdated())

        outdated = any([is_outdated(up.product) for up
                        in self.task.upstream.values()])

        return outdated

    def _outdated_code_dependency(self):
        dag = self.task.dag
        return dag.differ.code_is_different(self.stored_source_code,
                                            self.task.source_code,
                                            language=self.task.source.language)

    def _get_metadata(self):
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

    def __str__(self):
        return str(self._identifier)

    def __repr__(self):
        return f'{type(self).__name__}({repr(self._identifier.safe)})'

    def _short_repr(self):
        s = str(self._identifier)

        if len(s) > 20:
            s_short = ''

            t = ceil(len(s) / 20)

            for i in range(t):
                s_short += s[(20 * i):(20 * (i + 1))] + '\n'
        else:
            s_short = s

        return s_short

    # __getstate__ and __setstate__ are needed to make this picklable

    def __getstate__(self):
        state = self.__dict__.copy()
        # logger is not pickable, so we remove them and build
        # them again in __setstate__
        del state['logger']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = logging.getLogger('{}.{}'.format(__name__,
                                                       type(self).__name__))

    def _to_json_serializable(self):
        """Returns a JSON serializable version of this product
        """
        # NOTE: this is used in tasks where only JSON serializable parameters
        # are supported such as NotebookRunner that depends on papermill
        return str(self)

    def __len__(self):
        # MetaProduct return the number of products, this is a single Product
        # hence the 1
        return 1

    # Subclasses must implement the following methods

    @abc.abstractmethod
    def _init_identifier(self, identifier):
        pass

    @abc.abstractmethod
    def fetch_metadata(self):
        pass

    # TODO: this should have metadata as parameter, it is confusing
    # when writing a new product to know that the metaada to save is
    # in self.metadata
    @abc.abstractmethod
    def save_metadata(self):
        pass

    @abc.abstractmethod
    def exists(self):
        """
        This method returns True if the product exists, it is not part
        of the metadata, so there is no cached status
        """
        pass

    @abc.abstractmethod
    def delete(self, force=False):
        """Deletes the product
        """
        pass

    @property
    @abc.abstractmethod
    def name(self):
        """
        Product name, this is used as Task.name default if no name
        is provided
        """
        pass
