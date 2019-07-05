"""
Products are persistent changes triggered by Tasks such as a new file
in the local filesystem or a table in a database
"""
import os
from pathlib import Path
from dstools.pipeline.products.Product import Product


class File(Product):
    """A product representing a file in the local filesystem
    """
    @property
    def path_to_file(self):
        return Path(self.identifier)

    @property
    def path_to_stored_source_code(self):
        return Path(str(self.path_to_file) + '.source')

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

    def delete(self, force=False):
        # force is not used for this product but it is left for API
        # compatibility
        self.logger.debug(f'Deleting {self.path_to_file}')
        os.remove(self.path_to_file)

    def __repr__(self):
        return f'File({repr(self._identifier)})'
