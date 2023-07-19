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


def test_cli_run_override(tmp_path: Path, fixtures_path: Path):
    args = [
        str(fixtures_path / "spec.yml"),
        "-i",
        fixtures_path / "testdata.csv",
        "-o",
        tmp_path / "out.xlsx",
        "-rh",
        "read_csv",
        "-wh",
        "to_excel",
    ]
    result = runner.invoke(cli, args)
    assert result.exit_code == 0


def test_cli_invalid_path(fixtures_path: Path):
    result = runner.invoke(cli, [str(fixtures_path / "spec.yml")])
    assert result.exit_code == 0

    result = runner.invoke(cli, ["/foo/bar"])
    assert result.exit_code == 1

    result = runner.invoke(cli, ["/foo/bar.yml"])
    assert result.exit_code == 1

    result = runner.invoke(cli, [str(fixtures_path)])
    assert result.exit_code == 1
