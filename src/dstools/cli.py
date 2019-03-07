from dstools import Env
import click


@click.group()
@click.version_option()
def cli():
    """dstools command line utilities
    """
    pass


@cli.command()
@click.argument('path', type=str)
def env(path):
    """Read env.yaml
    """
    elements = path.split('.')

    current = Env()

    for element in elements:
        current = getattr(current, element)

    click.echo(current)
