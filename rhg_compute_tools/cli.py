import click
from rhg_compute_tools.gcs import replicate_directory_structure_on_gcs


@click.group(
    context_settings={'help_option_names': ['-h', '--help']}
)
def rct_cli():
    """Rhodium Compute Tools"""
    pass


@rct_cli.group()
def gcs():
    """Tools for interacting with Google Cloud Storage
    """
    pass


@gcs.command()
@click.argument('src', type=click.Path(exists=True))
@click.argument('dst', type=click.Path())
@click.option('-c', '--credentials', type=click.Path(exists=True),
                envvar='GOOGLE_APPLICATION_CREDENTIALS',
                help='Optional path to GCS credentials file.')
def repdirstruc(src, dst, credentials):
    """Replicate a local directory structure onto GCS bucket.
    """
    replicate_directory_structure_on_gcs(src, dst, credentials)
