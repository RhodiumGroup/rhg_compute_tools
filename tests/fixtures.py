import click
import pytest

import rhg_compute_tools.cli


@pytest.fixture
def replicate_directory_structure_on_gcs_stub(mocker):
    mocker.patch.object(
        rhg_compute_tools.cli,
        "replicate_directory_structure_on_gcs",
        new=lambda *args: click.echo(";".join(args)),
    )
    mocker.patch.object(
        rhg_compute_tools.cli, "authenticated_client", new=lambda x: str(x)
    )


@pytest.fixture
def tempfl_path(tmpdir):
    """Creates a temporary file, returning its path"""
    file = tmpdir.join("file.json")
    file.write("foobar")
    return str(file)
