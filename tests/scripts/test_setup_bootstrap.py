from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path


def _load_setup_module():
    path = Path(__file__).resolve().parents[2] / "scripts" / "setup.py"
    spec = importlib.util.spec_from_file_location("retail_demo_setup", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


setup = _load_setup_module()


def test_detect_package_manager_prefers_winget_on_windows(monkeypatch):
    monkeypatch.setattr(setup, "_command_exists", lambda command: command == "winget")

    manager = setup.detect_package_manager("Windows")

    assert manager is not None
    assert manager.name == "winget"
    assert manager.install_commands["terraform"][0][:4] == [
        "winget",
        "install",
        "--id",
        "Hashicorp.Terraform",
    ]


def test_detect_package_manager_uses_brew_on_macos(monkeypatch):
    monkeypatch.setattr(setup, "_command_exists", lambda command: command == "brew")

    manager = setup.detect_package_manager("Darwin")

    assert manager is not None
    assert manager.name == "brew"
    assert ["brew", "install", "azure-cli"] in manager.install_commands["az"]


def test_install_prerequisites_dry_run_records_commands(monkeypatch):
    commands = []
    manager = setup.PackageManager(
        "testpm",
        {"terraform": [["testpm", "install", "terraform"]]},
    )
    monkeypatch.setattr(setup, "run_command", lambda command, **_: commands.append(command))

    setup.install_prerequisites(
        ["terraform"],
        package_manager=manager,
        dry_run=True,
        assume_yes=True,
    )

    assert commands == [["testpm", "install", "terraform"]]


def test_run_command_reports_nonzero_exit_without_traceback(monkeypatch, capsys):
    def fake_run(command, **kwargs):
        raise subprocess.CalledProcessError(7, command)

    monkeypatch.setattr(setup.subprocess, "run", fake_run)

    try:
        setup.run_command(["terraform", "init"])
    except SystemExit as exc:
        assert "Command failed with exit code 7: terraform init" in str(exc)
    else:
        raise AssertionError("run_command should raise SystemExit")

    assert "$ terraform init" in capsys.readouterr().out


def test_run_retail_setup_dry_run_without_deploy(monkeypatch):
    commands = []
    env = setup.PythonEnv(Path("python"), "test")
    monkeypatch.setattr(setup, "run_command", lambda command, **_: commands.append(command))
    monkeypatch.setattr(setup, "prompt_yes_no", lambda *_, **__: False)

    setup.run_retail_setup(
        env,
        deploy_env="qa",
        dry_run=True,
        assume_yes=False,
        deploy_requested=False,
    )

    assert commands == [
        ["python", "-m", "retail_setup.cli.main", "configure", "--env", "qa"],
        ["python", "-m", "retail_setup.cli.main", "render", "--env", "qa"],
    ]


def test_run_retail_setup_deploy_flag_runs_deploy(monkeypatch):
    commands = []
    env = setup.PythonEnv(Path("python"), "test")
    monkeypatch.setattr(setup, "run_command", lambda command, **_: commands.append(command))

    setup.run_retail_setup(
        env,
        deploy_env="qa",
        dry_run=True,
        assume_yes=True,
        deploy_requested=True,
    )

    assert commands[-1] == [
        "python",
        "-m",
        "retail_setup.cli.main",
        "deploy",
        "--env",
        "qa",
        "--yes",
    ]
