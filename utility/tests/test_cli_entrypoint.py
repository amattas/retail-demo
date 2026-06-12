"""Smoke test for the retail-setup CLI entry point."""

from typer.testing import CliRunner

from retail_setup.cli.main import app

runner = CliRunner()


def test_help_lists_three_commands():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for cmd in ["configure", "render", "deploy"]:
        assert cmd in result.output
