from pathlib import Path

from typer.testing import CliRunner

from runpandarun.cli import cli

runner = CliRunner()


def test_cli_base():
    result = runner.invoke(cli, "--help")
    assert result.exit_code == 0


def test_cli_run(fixtures_path: Path):
    config = str(fixtures_path / "spec.yml")
    result = runner.invoke(cli, [config])
    assert result.exit_code == 0

    # no arguments
    result = runner.invoke(cli, ["run"])
    assert result.exit_code == 1
