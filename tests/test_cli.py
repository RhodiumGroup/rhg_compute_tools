"""
White-box testing to ensure that CLI passes variables correctly
"""


import pytest
import click
from click.testing import CliRunner
import rhg_compute_tools.cli


@pytest.fixture
def replicate_directory_structure_on_gcs_stub(mocker):
    mocker.patch.object(rhg_compute_tools.cli,
                        'replicate_directory_structure_on_gcs',
                        new=lambda *args: click.echo(';'.join(args)))


@pytest.fixture
def tempfl_path(tmpdir):
    """Creates a temporary file, returning its path"""
    file = tmpdir.join('file.json')
    file.write('foobar')
    return str(file)


@pytest.mark.parametrize('credflag', ['-c', '--credentials', None])
def test_repdirstruc(credflag, replicate_directory_structure_on_gcs_stub,
                     tmpdir, tempfl_path, monkeypatch):
    """Test rct gcs repdirstruc for main input options"""
    # Setup CLI args
    cred_path = str(tempfl_path)
    credargs = [credflag, cred_path]
    src_path = str(tmpdir)
    dst_path = 'gcs://foo/bar'

    if credflag is None:
        monkeypatch.setenv('GOOGLE_APPLICATION_CREDENTIALS', cred_path)
        credargs = []

    # Run CLI
    runner = CliRunner()
    result = runner.invoke(
        rhg_compute_tools.cli.rct_cli,
        ['gcs', 'repdirstruc'] + credargs + [src_path, dst_path],
    )

    expected_output = ';'.join([src_path, dst_path, cred_path]) + '\n'
    assert result.output == expected_output


@pytest.mark.parametrize('credflag', ['-c', '--credentials', None])
def test_repdirstruc_nocredfile(credflag, replicate_directory_structure_on_gcs_stub,
                                tmpdir, monkeypatch):
    """Test rct gcs repdirstruc for graceful fail when cred file missing"""
    # Setup CLI args
    cred_path = '_foobar.json'
    credargs = [credflag, cred_path]
    src_path = str(tmpdir)
    dst_path = 'gcs://foo/bar'

    if credflag is None:
        monkeypatch.setenv('GOOGLE_APPLICATION_CREDENTIALS', cred_path)
        credargs = []

    # Run CLI
    runner = CliRunner()
    result = runner.invoke(
        rhg_compute_tools.cli.rct_cli,
        ['gcs', 'repdirstruc'] + credargs + [src_path, dst_path],
    )

    expected_output = 'Usage: rct-cli gcs repdirstruc [OPTIONS] SRC DST\nTry' \
                      ' "rct-cli gcs repdirstruc -h" for help.\n\nError: ' \
                      'Invalid value for "-c" / "--credentials": Path ' \
                      '"_foobar.json" does not exist.\n'
    assert result.output == expected_output
