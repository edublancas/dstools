from pathlib import Path

from dstools.pipeline.clients import ShellClient
from dstools.pipeline.placeholders import ClientCodePlaceholder


def test_shell_client(tmp_directory):
    path = Path(tmp_directory, 'a_file')

    client = ShellClient()
    code = """
    touch a_file
    """

    code_placeholder = ClientCodePlaceholder(code).render({})

    assert not path.exists()

    client.run(code_placeholder)

    assert path.exists()
