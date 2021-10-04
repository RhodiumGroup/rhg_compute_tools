import click

from rhg_compute_tools.gcs import (
    authenticated_client,
    replicate_directory_structure_on_gcs,
    create_directories_under_blob,
)


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def rctools_cli():
    """Rhodium Compute Tools"""
    pass


@rctools_cli.group()
def gcs():
    """Tools for interacting with Google Cloud Storage"""
    pass


@gcs.command()
@click.argument("src", type=click.Path(exists=True))
@click.argument("dst", type=click.Path())
@click.option(
    "-c",
    "--credentials",
    type=click.Path(exists=True),
    help="Optional path to GCS credentials file.",
)
def repdirstruc(src, dst, credentials):
    """Replicate directory structure onto GCS bucket.

    SRC is path to a local directory. Directories within will be replicated.
    DST is gs://[bucket] and optional path to replicate SRC into.

    If --credentials or -c is not explicitly given, checks the
    GOOGLE_APPLICATION_CREDENTIALS environment variable for path to a GCS
    credentials file, or default service accounts for authentication. See
    https://googleapis.dev/python/google-api-core/latest/auth.html for more
    details.
    """
    client = authenticated_client(credentials)
    replicate_directory_structure_on_gcs(src, dst, client)


@gcs.command()
@click.argument("blob")
@click.option("--project", default=None, required=False, help="Google Cloud project for bucket (default is inferred from your credentials)")
def mkdirs(blob, project=None):
    """
    Create directory markers for all directories under a certain path on gcs

    To set up authentication locally while developing, configure your google
    cloud sdk with `gcloud init` then run:
    
        gcloud auth application-default login
        
    See https://googleapis.dev/python/google-api-core/latest/auth.html for
    more information.

    """
    create_directories_under_blob(blob, project=None, client=None)
