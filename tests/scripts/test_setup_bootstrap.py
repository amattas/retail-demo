from __future__ import annotations

import importlib.util
import io
import os
import subprocess
import sys
from pathlib import Path

import pytest


def _load_setup_module():
    path = Path(__file__).resolve().parents[2] / "scripts" / "setup.py"
    spec = importlib.util.spec_from_file_location("retail_demo_setup", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


setup = _load_setup_module()


def test_setup_default_profile_comes_from_manifest(monkeypatch):
    declared_default = next(
        profile.deployment_name
        for profile in setup.BOOTSTRAP_MANIFEST.profiles
        if profile.default
    )
    monkeypatch.setattr(sys, "argv", ["setup.py"])

    assert setup.DEFAULT_PROFILE == declared_default
    assert setup.parse_args().profile == declared_default


def test_later_profile_argument_overrides_wrapper_default(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["setup.py", "--profile", "full-demo", "--profile", "core"],
    )

    assert setup.parse_args().profile == "core"


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


def test_setup_sh_mirrors_ps1_bootstrap_contract():
    script = Path(__file__).resolve().parents[2] / "scripts" / "setup.sh"
    content = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert 'CondaEnvName="retail-demo"' in content
    assert 'CondaPythonVersion="3.13"' in content
    assert 'MiniforgeVersion="26.3.2-3"' in content
    assert 'VenvPath="$RepoRoot/.venv"' in content
    assert '"$python_exe" "$SetupPy" --profile full-demo "$@"' in content
    assert "install_miniforge" in content
    assert "releases/latest" not in content
    assert "verify_sha256" in content
    assert content.index('verify_sha256 "$installer"') < content.index(
        'bash "$installer"'
    )


def test_setup_ps1_pins_miniforge_winget_install():
    script = Path(__file__).resolve().parents[2] / "scripts" / "setup.ps1"
    content = script.read_text(encoding="utf-8")

    assert "$MiniforgeVersion = '26.3.2-3'" in content
    assert "--version $MiniforgeVersion --source winget" in content
    assert "--disable-interactivity --silent" in content
    assert "& $python $SetupPy '--profile' 'full-demo' @ForwardArgs" in content


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
    monkeypatch.setattr(
        setup, "run_command", lambda command, **_: commands.append(command)
    )

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


def test_install_python_dependencies_uses_reviewed_lock(monkeypatch):
    commands = []
    env = setup.PythonEnv(Path("python"), "test")
    monkeypatch.setattr(
        setup, "run_command", lambda command, **_: commands.append(command)
    )

    setup.install_python_dependencies(env, dry_run=True)

    assert len(commands) == 2
    assert "--require-hashes" in commands[0]
    assert commands[0][-2:] == [
        "-r",
        str(setup.REPO_ROOT / "utility" / "requirements-deploy.txt"),
    ]
    assert "--no-deps" in commands[1]
    assert commands[1][-2:] == ["-e", str(setup.REPO_ROOT / "utility")]


def test_run_command_label_hides_raw_command_on_success(monkeypatch, capsys):
    monkeypatch.setattr(setup, "VERBOSE", False)
    monkeypatch.setattr(setup.subprocess, "run", lambda command, **kwargs: None)

    setup.run_command(["pip", "install", "thing"], label="Installing thing")

    out = capsys.readouterr().out
    assert "Installing thing" in out
    assert "$ pip install thing" in out
    assert "--" in out


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
    monkeypatch.setattr(
        setup, "run_command", lambda command, **_: commands.append(command)
    )
    monkeypatch.setattr(
        setup,
        "prompt_yes_no",
        lambda *_, **__: (_ for _ in ()).throw(
            AssertionError("dry run must not prompt")
        ),
    )

    setup.run_retail_setup(
        env,
        workspace_name="retail-demo-qa",
        profile="core",
        dry_run=True,
        assume_yes=False,
        deploy_requested=False,
    )

    assert commands == [
        [
            "python",
            "-m",
            "retail_setup.cli.main",
            "configure",
            "--workspace-name",
            "retail-demo-qa",
            "--profile",
            "core",
        ],
        ["python", "-m", "retail_setup.cli.main", "render", "--env", "qa"],
    ]


def test_run_retail_setup_deploy_flag_runs_deploy(monkeypatch):
    commands = []
    env = setup.PythonEnv(Path("python"), "test")
    monkeypatch.setattr(
        setup, "run_command", lambda command, **_: commands.append(command)
    )
    monkeypatch.setattr(setup, "ensure_azure_login", lambda *_, **__: None)

    setup.run_retail_setup(
        env,
        workspace_name="retail-demo-qa",
        profile="core",
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
        "--skip-login",
        "--yes",
    ]


def test_run_retail_setup_recreate_passes_recreate_flag(monkeypatch):
    commands = []
    env = setup.PythonEnv(Path("python"), "test")
    monkeypatch.setattr(
        setup, "run_command", lambda command, **_: commands.append(command)
    )
    monkeypatch.setattr(setup, "ensure_azure_login", lambda *_, **__: None)

    setup.run_retail_setup(
        env,
        workspace_name="retail-demo-qa",
        profile="core",
        dry_run=True,
        assume_yes=True,
        deploy_requested=True,
        recreate=True,
    )

    deploy_cmd = commands[-1]
    assert "--recreate" in deploy_cmd
    assert deploy_cmd.index("--recreate") < deploy_cmd.index("--yes")


def test_workspace_environment_name_uses_workspace_suffix():
    assert setup.workspace_environment_name("Retail Demo - Alice") == "alice"
    assert setup.workspace_environment_name("Customer Showcase") == "customer-showcase"

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
    monkeypatch.setattr(
        setup, "run_command", lambda command, **_: commands.append(command)
    )

    setup.ensure_azure_login("dev", dry_run=False)

    assert commands == [["C:/az.cmd", "login", "--tenant", "TENANT"]]


def test_ensure_azure_login_uses_powershell_for_az_powershell(monkeypatch):
    commands = []
    monkeypatch.setattr(
        setup, "read_deploy_auth", lambda _: ("azure_powershell", "TENANT")
    )
    monkeypatch.setattr(
        setup.shutil, "which", lambda command: "pwsh" if command == "pwsh" else None
    )
    monkeypatch.setattr(
        setup, "run_command", lambda command, **_: commands.append(command)
    )

    setup.ensure_azure_login("dev", dry_run=False)

    assert commands == [
        ["pwsh", "-NoProfile", "-Command", "Connect-AzAccount -Tenant TENANT"]
    ]


def test_ensure_azure_login_noop_when_no_tenant(monkeypatch):
    commands = []
    monkeypatch.setattr(setup, "read_deploy_auth", lambda _: ("azure_cli", None))
    monkeypatch.setattr(
        setup, "run_command", lambda command, **_: commands.append(command)
    )

    setup.ensure_azure_login("dev", dry_run=False)

    assert commands == []


def test_section_prints_linear_divider(capsys):
    setup._section(3, 4, "Configuring the project")
    out = capsys.readouterr().out
    assert "Step 3 of 4: Configuring the project" in out
    assert "==" in out


class _LoginProcess:
    def __init__(self, stderr, returncode):
        self.stderr = io.StringIO(stderr)
        self.returncode = returncode

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.stderr.close()

    def wait(self):
        return self.returncode


def test_windows_broker_failure_retries_browser_without_global_changes(monkeypatch, capsys):
    error = (
        "An attempt was made to import or load a key under an incorrect storage parent.. "
        "Status: Response_Status.Status_InteractionRequired, Error code: 2150171662\n"
    )
    calls = []
    original = dict(os.environ)
    monkeypatch.setattr(setup.platform, "system", lambda: "Windows")
    monkeypatch.setattr(setup, "read_deploy_auth", lambda _: ("azure_cli", "TENANT"))
    monkeypatch.setattr(setup, "_resolve_az", lambda: r"C:\Azure CLI\az.cmd")

    def popen(command, **kwargs):
        calls.append((command, kwargs))
        return _LoginProcess(error, 1)

    def run(command, **kwargs):
        calls.append((command, kwargs))
        if kwargs.get("env", {}).get("AZURE_CORE_ENABLE_BROKER_ON_WINDOWS") != "false":
            raise subprocess.CalledProcessError(1, command, stderr=error)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(setup.subprocess, "Popen", popen)
    monkeypatch.setattr(setup.subprocess, "run", run)

    overrides = setup.ensure_azure_login("dev", dry_run=False)

    assert overrides == {"AZURE_CORE_ENABLE_BROKER_ON_WINDOWS": "false"}
    assert len(calls) == 2
    assert all(command == [r"C:\Azure CLI\az.cmd", "login", "--tenant", "TENANT"]
               for command, _ in calls)
    fallback_env = calls[1][1]["env"]
    assert fallback_env["AZURE_CORE_ENABLE_BROKER_ON_WINDOWS"] == "false"
    assert dict(os.environ) == original
    output = capsys.readouterr()
    assert "incorrect storage parent" in output.err
    assert "browser" in output.out.lower()


@pytest.mark.parametrize(
    "error",
    ["AADSTS53003: Blocked by Conditional Access\n", "The user canceled authentication\n"],
)
def test_login_policy_errors_and_cancellations_are_not_retried(monkeypatch, error):
    calls = []
    monkeypatch.setattr(setup.platform, "system", lambda: "Windows")
    monkeypatch.setattr(setup, "read_deploy_auth", lambda _: ("azure_cli", "TENANT"))
    monkeypatch.setattr(setup, "_resolve_az", lambda: "az")

    def popen(command, **kwargs):
        calls.append(command)
        return _LoginProcess(error, 1)

    def run(command, **kwargs):
        calls.append(command)
        raise subprocess.CalledProcessError(1, command, stderr=error)

    monkeypatch.setattr(setup.subprocess, "Popen", popen)
    monkeypatch.setattr(setup.subprocess, "run", run)
    with pytest.raises(SystemExit):
        setup.ensure_azure_login("dev", dry_run=False)
    assert len(calls) == 1


def test_failed_browser_retry_propagates_without_a_third_login(monkeypatch):
    calls = []
    monkeypatch.setattr(setup.platform, "system", lambda: "Windows")
    monkeypatch.setattr(setup, "read_deploy_auth", lambda _: ("azure_cli", "TENANT"))
    monkeypatch.setattr(setup, "_resolve_az", lambda: "az")

    def popen(command, **kwargs):
        calls.append(command)
        return _LoginProcess("Error code: 2150171662\n", 1)

    def run(command, **kwargs):
        calls.append(command)
        raise subprocess.CalledProcessError(9, command)

    monkeypatch.setattr(setup.subprocess, "Popen", popen)
    monkeypatch.setattr(setup.subprocess, "run", run)
    with pytest.raises(SystemExit, match="exit code 9"):
        setup.ensure_azure_login("dev", dry_run=False)
    assert len(calls) == 2


def test_successful_windows_login_keeps_default_authentication(monkeypatch):
    calls = []
    monkeypatch.setattr(setup.platform, "system", lambda: "Windows")
    monkeypatch.setattr(setup, "read_deploy_auth", lambda _: ("azure_cli", "TENANT"))
    monkeypatch.setattr(setup, "_resolve_az", lambda: "az")

    def popen(command, **kwargs):
        calls.append((command, kwargs))
        return _LoginProcess("", 0)

    monkeypatch.setattr(setup.subprocess, "Popen", popen)
    monkeypatch.setattr(
        setup.subprocess, "run",
        lambda *_args, **_kwargs: pytest.fail("successful login must not retry"),
    )
    setup.ensure_azure_login("dev", dry_run=False)
    assert len(calls) == 1
    assert calls[0][1]["env"] is None
    assert "stdin" not in calls[0][1] and "stdout" not in calls[0][1]


def test_non_windows_login_does_not_change_broker_settings(monkeypatch):
    calls = []
    monkeypatch.setattr(setup.platform, "system", lambda: "Linux")
    monkeypatch.setattr(setup, "read_deploy_auth", lambda _: ("azure_cli", "TENANT"))
    monkeypatch.setattr(setup, "_resolve_az", lambda: "az")

    def run(command, **kwargs):
        calls.append((command, kwargs))
        raise subprocess.CalledProcessError(1, command, stderr="2150171662")

    monkeypatch.setattr(setup.subprocess, "run", run)
    with pytest.raises(SystemExit):
        setup.ensure_azure_login("dev", dry_run=False)
    assert len(calls) == 1
    assert "env" not in calls[0][1]


def test_login_dry_run_never_starts_authentication(monkeypatch):
    monkeypatch.setattr(setup.platform, "system", lambda: "Windows")
    monkeypatch.setattr(setup, "read_deploy_auth", lambda _: ("azure_cli", "TENANT"))
    monkeypatch.setattr(setup, "_resolve_az", lambda: "az")
    monkeypatch.setattr(
        setup.subprocess, "Popen",
        lambda *_args, **_kwargs: pytest.fail("dry run must not start a login"),
    )
    setup.ensure_azure_login("dev", dry_run=True)


def test_stderr_tee_preserves_visible_prompts_and_bounded_failure_diagnostics(capsys):
    message = "x" * 9000 + "prompt without newline"
    with pytest.raises(setup.CommandFailed) as failed:
        setup.run_command(
            [sys.executable, "-c",
             f"import sys; sys.stderr.write({message!r}); sys.exit(4)"],
            capture_stderr=True,
        )
    assert failed.value.stderr == message[-8192:]
    assert capsys.readouterr().err == message


def test_recovered_login_environment_follows_deployment_only(monkeypatch):
    calls = []
    overrides = {"AZURE_CORE_ENABLE_BROKER_ON_WINDOWS": "false"}
    monkeypatch.setattr(
        setup, "run_command",
        lambda command, **kwargs: calls.append((command, kwargs)),
    )
    monkeypatch.setattr(setup, "ensure_azure_login", lambda *_args, **_kwargs: overrides)
    environment = setup.PythonEnv(Path(sys.executable), "test")

    setup.run_retail_setup(
        environment, workspace_name="retail-demo", profile="core",
        dry_run=False, assume_yes=True, deploy_requested=True,
    )

    assert [call[1].get("environment_overrides") for call in calls] == [
        None, None, overrides
    ]
    assert "deploy" in calls[-1][0]
