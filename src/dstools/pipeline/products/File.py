"""
Products are persistent changes triggered by Tasks such as a new file
in the local filesystem or a table in a database
"""
import os
from pathlib import Path
from dstools.pipeline.products.Product import Product
from dstools.pipeline.placeholders import StringPlaceholder


class File(Product):
    """A product representing a file in the local filesystem
    """
    IDENTIFIERCLASS = StringPlaceholder

    @property
    def _path_to_file(self):
        return Path(str(self._identifier))

    @property
    def _path_to_stored_source_code(self):
        return Path(str(self._path_to_file) + '.source')

    def fetch_metadata(self):
        # we can safely do this here since this is only run when the file
        # exists
        timestamp = self._path_to_file.stat().st_mtime

        # but we have no control over the stored code, it might be missing
        # so we check
        if self._path_to_stored_source_code.exists():
            stored_source_code = self._path_to_stored_source_code.read_text()
        else:
            stored_source_code = None

        return dict(timestamp=timestamp, stored_source_code=stored_source_code)

    def save_metadata(self):
        # timestamp automatically updates when the file is saved...
        self._path_to_stored_source_code.write_text(self.stored_source_code)

    def exists(self):
        return self._path_to_file.exists()

    def delete(self, force=False):
        # force is not used for this product but it is left for API
        # compatibility
        if self.exists():
            self.logger.debug(f'Deleting {self._path_to_file}')
            os.remove(self._path_to_file)
        else:
            self.logger.debug(f'{self._path_to_file} does not exist '
                              'ignoring...')

    @property
    def name(self):
        return self._path_to_file.with_suffix('').name
