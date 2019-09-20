from dstools.pipeline.clients import ShellClient
from dstools.pipeline.placeholders import ClientCodePlaceholder

client = ShellClient()

code = """
touch a_file
"""

code_placeholder = ClientCodePlaceholder(code).render({})

# client.run(code_placeholder)

# str(code_placeholder)
