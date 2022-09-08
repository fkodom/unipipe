import os
from unittest import mock

from click.testing import CliRunner

from unipipe.cli import unipipe


def test_greet_cli():
    runner = CliRunner()
    local_dir = os.path.abspath(os.path.dirname(__file__))
    example_path = os.path.join(
        local_dir, os.pardir, "examples", "ex11_using_scripts.py"
    )

    # This will fail, because it doesn't have any PyPI credentials as ENV variables.
    # It should raise 'KeyError' as it tries to fetch those from 'os.environ'.
    result = runner.invoke(unipipe, ["run-script", example_path, "--hello=world"])
    assert result.exit_code == 1

    # Set mock values, so we can run the script without affecting other tests.
    mock_pypi_credentials = {"PYPI_USERNAME": "user", "PYPI_PASSWORD": "pass"}
    with mock.patch.dict(os.environ, mock_pypi_credentials):
        result = runner.invoke(unipipe, ["run-script", example_path, "--hello=world"])
        assert result.exit_code == 0
        assert "Hello, world!" in result.output

        result = runner.invoke(
            unipipe, ["run-script", example_path, "--hello", "Vertex"]
        )
        assert result.exit_code == 0
        assert "Hello, Vertex!" in result.output
