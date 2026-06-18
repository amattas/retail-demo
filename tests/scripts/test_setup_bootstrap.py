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
        assert "exit code 7" in str(exc)
        assert "terraform init" in str(exc)
    else:
        raise AssertionError("run_command should raise SystemExit")

    assert "$ terraform init" in capsys.readouterr().out


def test_pip_flags_quiet_by_default_and_loud_when_verbose(monkeypatch):
    monkeypatch.setattr(setup, "VERBOSE", False)
    assert setup._pip_flags() == ["-q", "--progress-bar", "off"]

    monkeypatch.setattr(setup, "VERBOSE", True)
    assert setup._pip_flags() == []


def test_run_command_label_hides_raw_command_on_success(monkeypatch, capsys):
    monkeypatch.setattr(setup, "VERBOSE", False)
    monkeypatch.setattr(setup.subprocess, "run", lambda command, **kwargs: None)

    setup.run_command(["pip", "install", "thing"], label="Installing thing")

    out = capsys.readouterr().out
    assert "Installing thing" in out
    assert "$ pip install thing" not in out


def test_run_command_label_reveals_command_on_failure(monkeypatch, capsys):
    monkeypatch.setattr(setup, "VERBOSE", False)

    def fake_run(command, **kwargs):
        raise subprocess.CalledProcessError(3, command)

    monkeypatch.setattr(setup.subprocess, "run", fake_run)

    try:
        setup.run_command(["pip", "install", "thing"], label="Installing thing")
    except SystemExit:
        pass
    else:
        raise AssertionError("run_command should raise SystemExit")

    # The raw command must still appear so failures are debuggable.
    assert "pip install thing" in capsys.readouterr().out


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
    monkeypatch.setattr(setup, "ensure_azure_login", lambda *_, **__: None)

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


def test_run_retail_setup_recreate_passes_recreate_flag(monkeypatch):
    commands = []
    env = setup.PythonEnv(Path("python"), "test")
    monkeypatch.setattr(setup, "run_command", lambda command, **_: commands.append(command))
    monkeypatch.setattr(setup, "ensure_azure_login", lambda *_, **__: None)

    setup.run_retail_setup(
        env,
        deploy_env="qa",
        dry_run=True,
        assume_yes=True,
        deploy_requested=True,
        recreate=True,
    )

    deploy_cmd = commands[-1]
    assert "--recreate" in deploy_cmd
    assert deploy_cmd.index("--recreate") < deploy_cmd.index("--yes")

    text = "tenant_id: 11111111-1111-1111-1111-111111111111\nauth:\n  mode: azure_cli\n"

    assert setup._extract_tenant_id(text) == "11111111-1111-1111-1111-111111111111"
    assert setup._extract_auth_mode(text) == "azure_cli"


def test_extract_tenant_id_treats_null_as_missing():
    assert setup._extract_tenant_id("tenant_id: null\n") is None
    assert setup._extract_tenant_id("subscription_id: x\n") is None


def test_ensure_azure_login_always_runs_az_login(monkeypatch):
    commands = []
    monkeypatch.setattr(setup, "read_deploy_auth", lambda _: ("azure_cli", "TENANT"))
    monkeypatch.setattr(setup, "_resolve_az", lambda: "C:/az.cmd")
    monkeypatch.setattr(setup, "run_command", lambda command, **_: commands.append(command))

    setup.ensure_azure_login("dev", dry_run=False)

    assert commands == [["C:/az.cmd", "login", "--tenant", "TENANT"]]


def test_ensure_azure_login_uses_powershell_for_az_powershell(monkeypatch):
    commands = []
    monkeypatch.setattr(setup, "read_deploy_auth", lambda _: ("azure_powershell", "TENANT"))
    monkeypatch.setattr(setup.shutil, "which", lambda command: "pwsh" if command == "pwsh" else None)
    monkeypatch.setattr(setup, "run_command", lambda command, **_: commands.append(command))

    setup.ensure_azure_login("dev", dry_run=False)

    assert commands == [["pwsh", "-NoProfile", "-Command", "Connect-AzAccount -Tenant TENANT"]]


def test_ensure_azure_login_noop_when_no_tenant(monkeypatch):
    commands = []
    monkeypatch.setattr(setup, "read_deploy_auth", lambda _: ("azure_cli", None))
    monkeypatch.setattr(setup, "run_command", lambda command, **_: commands.append(command))

    setup.ensure_azure_login("dev", dry_run=False)

    assert commands == []


# --- guided console (TTY) integration -------------------------------------- #
import contextlib
from types import SimpleNamespace


class _FakeUI:
    """Stand-in for retail_setup.cli.console.ConsoleUI."""

    def __init__(self, *, answer=True, cancelled=False):
        self.logs = []
        self.phases = []
        self.advances = []
        self.paused_count = 0
        self.entered = self.exited = False
        self._answer = answer
        self._cancelled = cancelled

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, *exc):
        self.exited = True
        return False

    def log(self, text=""):
        self.logs.append(text)

    def set_phase(self, text):
        self.phases.append(text)

    def advance(self, *, completed=None, fraction=None):
        self.advances.append(completed)

    @property
    def cancelled(self):
        return self._cancelled

    def prompt_yes_no(self, message, *, default=False):
        return self._answer

    @contextlib.contextmanager
    def paused(self):
        self.paused_count += 1
        yield


def test_emit_routes_to_active_ui(monkeypatch):
    ui = _FakeUI()
    monkeypatch.setattr(setup, "_UI", ui)
    setup._emit("hello")
    assert ui.logs == ["hello"]


def test_section_advances_bar_when_ui_active(monkeypatch):
    ui = _FakeUI()
    monkeypatch.setattr(setup, "_UI", ui)
    setup._section(3, 4, "Configuring the project")
    assert ui.phases == ["Step 3/4: Configuring the project"]
    assert ui.advances == [2]  # completed = step - 1


def test_prompt_yes_no_delegates_to_ui(monkeypatch):
    ui = _FakeUI(answer=False)
    monkeypatch.setattr(setup, "_UI", ui)
    assert setup.prompt_yes_no("ok?", default=True) is False


def test_run_command_streams_output_when_ui_active(monkeypatch):
    ui = _FakeUI()
    monkeypatch.setattr(setup, "_UI", ui)

    class _FakeProc:
        def __init__(self):
            self.stdout = iter(["line one\n", "line two\n"])
            self.returncode = 0

        def wait(self):
            return 0

    monkeypatch.setattr(setup.subprocess, "Popen", lambda *a, **k: _FakeProc())
    setup.run_command(["pip", "install", "thing"], label="Installing thing")
    assert "line one" in ui.logs and "line two" in ui.logs


def test_run_command_pauses_for_interactive_when_ui_active(monkeypatch):
    ui = _FakeUI()
    monkeypatch.setattr(setup, "_UI", ui)
    ran = []
    monkeypatch.setattr(setup.subprocess, "run", lambda command, **k: ran.append(command))

    setup.run_command(["az", "login"], interactive=True)

    assert ran == [["az", "login"]]
    assert ui.paused_count == 1  # bar paused so the child owns the terminal


def test_load_console_returns_none_without_tty(monkeypatch):
    # pytest's stdout is not a TTY, so the guided console must stay disabled.
    monkeypatch.delenv("RETAIL_SETUP_NO_UI", raising=False)
    assert setup._load_console() is None


def test_load_console_disabled_by_env(monkeypatch):
    monkeypatch.setenv("RETAIL_SETUP_NO_UI", "1")
    assert setup._load_console() is None


def test_main_drives_console_and_resets_ui(monkeypatch):
    ui = _FakeUI()
    monkeypatch.setattr(setup, "_load_console", lambda: (lambda *a, **k: ui))
    monkeypatch.setattr(
        setup, "parse_args",
        lambda: SimpleNamespace(env="dev", dry_run=False, yes=True, deploy=False,
                                recreate=False, skip_prereqs=True, verbose=False),
    )
    monkeypatch.setattr(setup, "current_python_env",
                        lambda: setup.PythonEnv(Path("python"), "test"))
    monkeypatch.setattr(setup, "install_python_dependencies", lambda *a, **k: None)
    monkeypatch.setattr(setup, "run_retail_setup", lambda *a, **k: None)

    rc = setup.main()

    assert rc == 0
    assert ui.entered and ui.exited
    assert setup._UI is None  # reset after the run
    assert any("Step 2/4" in p for p in ui.phases)
