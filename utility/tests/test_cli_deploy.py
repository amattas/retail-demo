import subprocess
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from retail_setup.cli.main import app, _deploy_plan, _validate_azure_cli_tenant

runner = CliRunner()


def test_dry_run_prints_full_plan_and_executes_nothing(monkeypatch):
    calls = []
    monkeypatch.setattr("subprocess.run", lambda *a, **k: calls.append(a))
    result = runner.invoke(app, ["deploy", "--env", "dev", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert calls == []
    out = result.output
    assert "generate_configs" in out and "terraform" in out
    assert "build_artifacts" in out and "core setup ml" in out.replace("'", "")
    assert "deploy_items" in out and "apply_kql" in out and "validate_deployment" in out


def test_skip_terraform_drops_terraform_steps():
    plan = _deploy_plan("dev", skip_terraform=True)
    flat = " ".join(" ".join(map(str, step.cmd)) for step in plan)
    assert "terraform" not in flat
    assert "generate_configs" in flat and "deploy_items" in flat


def test_plan_orders_steps_and_gates_apply():
    plan = _deploy_plan("dev", skip_terraform=False)
    cmds = [" ".join(map(str, s.cmd)) for s in plan]
    apply_idx = next(i for i, c in enumerate(cmds) if "apply" in c and "terraform" in c)
    assert plan[apply_idx].needs_confirmation
    assert any("plan" in c for c in cmds[:apply_idx])
    build_idx = next(i for i, c in enumerate(cmds) if "build_artifacts" in c)
    deploy_idx = next(i for i, c in enumerate(cmds) if "deploy_items" in c)
    assert apply_idx < build_idx < deploy_idx


def test_recreate_inserts_destroy_and_sleep_before_plan():
    plan = _deploy_plan("dev", skip_terraform=False, recreate=True)
    cmds = [" ".join(map(str, s.cmd)) for s in plan]
    init_idx = next(i for i, c in enumerate(cmds) if "terraform" in c and "init" in c)
    destroy_idx = next(i for i, c in enumerate(cmds) if "terraform" in c and "destroy" in c)
    sleep_idx = next(i for i, c in enumerate(cmds) if "time.sleep(30)" in c)
    plan_idx = next(i for i, c in enumerate(cmds) if "terraform" in c and " plan " in f" {c} ")
    assert init_idx < destroy_idx < sleep_idx < plan_idx
    assert plan[destroy_idx].needs_confirmation


def test_recreate_rejects_skip_terraform(monkeypatch):
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    result = runner.invoke(app, ["deploy", "--env", "dev", "--recreate", "--skip-terraform"])
    assert result.exit_code == 1, result.output
    assert "--recreate cannot be combined with --skip-terraform" in result.output


def test_recreate_dry_run_shows_destroy_step():
    result = runner.invoke(app, ["deploy", "--env", "dev", "--recreate", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "destroy" in result.output


def test_recreate_warns_and_aborts_on_decline(monkeypatch):
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    monkeypatch.setattr("subprocess.run", lambda *a, **k: SimpleNamespace(returncode=0))
    # Decline the first gated step (Terraform destroy).
    result = runner.invoke(app, ["deploy", "--env", "dev", "--recreate"], input="n\n")
    assert result.exit_code == 1, result.output
    assert "WARNING" in result.output
    assert "Aborted by user" in result.output


def test_deploy_reports_missing_terraform_without_traceback(monkeypatch):
    def fake_run(cmd, *args, **kwargs):
        if cmd and cmd[0] == "terraform":
            raise FileNotFoundError("terraform")
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr("subprocess.run", fake_run)
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    result = runner.invoke(app, ["deploy", "--env", "dev", "--yes"])
    assert result.exit_code == 127, result.output
    assert "Required executable not found: terraform" in result.output
    assert "--skip-terraform" in result.output
    assert "Traceback" not in result.output


def test_azure_cli_tenant_preflight_accepts_matching_tenant(monkeypatch):
    monkeypatch.setattr(
        "retail_setup.cli.main._load_deploy_environment",
        lambda *_: SimpleNamespace(auth_mode="azure_cli", tenant_id="TENANT"),
    )
    monkeypatch.setattr("retail_setup.cli.main._active_azure_cli_tenant", lambda: "tenant")

    _validate_azure_cli_tenant(Path("."), "dev")


def test_active_azure_cli_tenant_uses_resolved_az_path(monkeypatch):
    calls = []

    def fake_run(cmd, *args, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="tenant-id\n")

    monkeypatch.setattr("retail_setup.cli.main.shutil.which", lambda command: "C:/az.cmd")
    monkeypatch.setattr("subprocess.run", fake_run)

    from retail_setup.cli.main import _active_azure_cli_tenant

    assert _active_azure_cli_tenant() == "tenant-id"
    assert calls[0][0] == "C:/az.cmd"


def test_azure_cli_tenant_preflight_rejects_mismatched_tenant(monkeypatch):
    monkeypatch.setattr(
        "retail_setup.cli.main._load_deploy_environment",
        lambda *_: SimpleNamespace(auth_mode="azure_cli", tenant_id="expected-tenant"),
    )
    monkeypatch.setattr(
        "retail_setup.cli.main._active_azure_cli_tenant",
        lambda: "active-tenant",
    )

    result = runner.invoke(app, ["deploy", "--env", "dev", "--yes"])
    assert result.exit_code == 1, result.output
    assert "Azure CLI tenant does not match deploy config tenant_id" in result.output
    assert "az login --tenant expected-tenant" in result.output
    assert "terraform" not in result.output
