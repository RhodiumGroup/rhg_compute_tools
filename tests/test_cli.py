"""
White-box testing to ensure that CLI passes variables correctly
"""


import click
import pytest
from click.testing import CliRunner

import rhg_compute_tools.cli
from tests.fixtures import replicate_directory_structure_on_gcs_stub, tempfl_path


@pytest.mark.parametrize("credflag", ["-c", "--credentials", None])
def test_repdirstruc(
    credflag,
    replicate_directory_structure_on_gcs_stub,
    tmpdir,
    tempfl_path,
    monkeypatch,
):
    """Test rctools gcs repdirstruc for main input options"""
    # Setup CLI args
    cred_path = str(tempfl_path)
    credargs = [credflag, cred_path]
    src_path = str(tmpdir)
    dst_path = "gcs://foo/bar"

    if credflag is None:
        credargs = []
        cred_path = str(None)

    # Run CLI
    runner = CliRunner()
    result = runner.invoke(
        rhg_compute_tools.cli.rctools_cli,
        ["gcs", "repdirstruc"] + credargs + [src_path, dst_path],
    )

    expected_output = ";".join([src_path, dst_path, cred_path]) + "\n"
    assert result.output == expected_output


@pytest.mark.parametrize("credflag", ["-c", "--credentials"])
def test_repdirstruc_nocredfile(
    credflag, replicate_directory_structure_on_gcs_stub, tmpdir, monkeypatch
):
    """Test rctools gcs repdirstruc for graceful fail when cred file missing"""
    # Setup CLI args
    cred_path = "_foobar.json"
    credargs = [credflag, cred_path]
    src_path = str(tmpdir)
    dst_path = "gcs://foo/bar"

    # Run CLI
    runner = CliRunner()
    result = runner.invoke(
        rhg_compute_tools.cli.rctools_cli,
        ["gcs", "repdirstruc"] + credargs + [src_path, dst_path],
    )

    expected = (
        "Usage: rctools-cli gcs repdirstruc [OPTIONS] SRC DST\nTry"
        " 'rctools-cli gcs repdirstruc -h' for help.\n\nError: "
        "Invalid value for '-c' / '--credentials': Path "
        "'_foobar.json' does not exist.\n"
    )
    assert result.output.replace('"', "'") == expected.replace('"', "'")
