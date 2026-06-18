import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import retail_setup.cli.main as cli
from retail_setup.cli.main import (
    app,
    _deploy_plan,
    _validate_azure_cli_tenant,
    _RECREATE_WAIT_SECONDS,
    _PIPELINE_TRIGGER_ATTEMPTS,
    _run_setup_pipeline,
)

runner = CliRunner()


def test_dry_run_prints_full_plan_and_executes_nothing(monkeypatch):
    calls = []
    monkeypatch.setattr("subprocess.run", lambda *a, **k: calls.append(a))
    result = runner.invoke(app, ["deploy", "--env", "dev", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert calls == []
    out = result.output
    assert "generate_configs" in out and "terraform" in out
    assert "build_artifacts" in out and "core setup ml ontology reset stream" in out.replace("'", "")
    assert "deploy_items" in out and "apply_kql" in out and "validate_deployment" in out


def test_skip_terraform_drops_terraform_steps():
    plan = _deploy_plan("dev", skip_terraform=True)
    flat = " ".join(" ".join(map(str, step.cmd)) for step in plan)
    assert "terraform" not in flat
    assert "generate_configs" in flat and "deploy_items" in flat


def test_plan_builds_ontology_and_reset_notebook_groups():
    plan = _deploy_plan("dev", skip_terraform=False)
    build = next(s for s in plan if "build_artifacts" in " ".join(map(str, s.cmd)))
    cmd = build.cmd
    groups_idx = cmd.index("--notebook-groups")
    lakehouse_idx = cmd.index("--lakehouse-name")
    groups = cmd[groups_idx + 1 : lakehouse_idx]
    # The deploy stages every notebook group needed to link the full task flow:
    # ontology (30-create-ontology) and reset (99-reset-lakehouse) join the core
    # pipeline, setup notebooks, ML notebooks, and the streaming generator.
    assert set(groups) == {"core", "setup", "ml", "ontology", "reset", "stream"}


def test_plan_orders_steps_and_gates_apply():
    plan = _deploy_plan("dev", skip_terraform=False)
    cmds = [" ".join(map(str, s.cmd)) for s in plan]
    apply_idx = next(i for i, c in enumerate(cmds) if "apply" in c and "terraform" in c)
    assert plan[apply_idx].needs_confirmation
    # The redundant `terraform plan` step was removed; apply previews + confirms.
    assert not any(" plan " in f" {c} " for c in cmds)
    build_idx = next(i for i, c in enumerate(cmds) if "build_artifacts" in c)
    deploy_idx = next(i for i, c in enumerate(cmds) if "deploy_items" in c)
    assert apply_idx < build_idx < deploy_idx


def test_recreate_inserts_destroy_and_sleep_before_apply():
    plan = _deploy_plan("dev", skip_terraform=False, recreate=True)
    cmds = [" ".join(map(str, s.cmd)) for s in plan]
    init_idx = next(i for i, c in enumerate(cmds) if "terraform" in c and "init" in c)
    destroy_idx = next(i for i, c in enumerate(cmds) if "terraform" in c and "destroy" in c)
    sleep_idx = next(
        i for i, c in enumerate(cmds) if f"time.sleep({_RECREATE_WAIT_SECONDS})" in c
    )
    apply_idx = next(i for i, c in enumerate(cmds) if "terraform" in c and "apply" in c)
    assert init_idx < destroy_idx < sleep_idx < apply_idx
    assert plan[destroy_idx].needs_confirmation
    assert not any(" plan " in f" {c} " for c in cmds)


def test_terraform_steps_auto_approve_to_avoid_double_prompt():
    plan = _deploy_plan("dev", skip_terraform=False, recreate=True)
    tf = [s for s in plan if s.cmd and s.cmd[0] == "terraform"]
    apply = next(s for s in tf if "apply" in s.cmd)
    destroy = next(s for s in tf if "destroy" in s.cmd)
    # A single confirmation gate stays in retail-setup; terraform itself is
    # auto-approved so the user never has to say "yes" a second time.
    assert "-auto-approve" in apply.cmd and apply.needs_confirmation
    assert "-auto-approve" in destroy.cmd and destroy.needs_confirmation


def test_is_terraform_apply_detects_apply_steps():
    from retail_setup.cli.main import _is_terraform_apply, DeployStep

    apply = DeployStep(cmd=["terraform", "-chdir=deploy/terraform", "apply", "-auto-approve"])
    other = DeployStep(cmd=["terraform", "-chdir=deploy/terraform", "init"])
    assert _is_terraform_apply(apply) is True
    assert _is_terraform_apply(other) is False
    assert _is_terraform_apply(DeployStep(cmd=[])) is False


def test_cleanup_destroy_step_targets_environment():
    from retail_setup.cli.main import _cleanup_destroy_step

    step = _cleanup_destroy_step("prod")
    assert step.cmd[:3] == ["terraform", "-chdir=deploy/terraform", "destroy"]
    assert "-auto-approve" in step.cmd
    assert "-var-file=environments/prod.tfvars" in step.cmd


class _FakeUI:
    """Minimal ConsoleUI stand-in for exercising cancel handling."""

    def __init__(self, answer: bool) -> None:
        self._answer = answer
        self.logs: list[str] = []
        self.phases: list[str] = []

    def log(self, message: str = "") -> None:
        self.logs.append(message)

    def set_phase(self, text: str) -> None:
        self.phases.append(text)

    def status(self, text: str) -> None:
        pass

    def reset_cancel(self) -> None:
        pass

    def prompt_yes_no(self, question: str, *, default: bool = False) -> bool:
        return self._answer


def test_handle_cancel_runs_destroy_when_apply_started_and_confirmed(monkeypatch):
    import typer

    from retail_setup.cli.main import _handle_cancel

    ran: list[list[str]] = []
    monkeypatch.setattr(
        "retail_setup.cli.main._run_step_streamed",
        lambda step, root, ui: ran.append(step.cmd) or 0,
    )
    ui = _FakeUI(answer=True)
    with pytest.raises(typer.Exit) as exc:
        _handle_cancel(ui, Path("."), "dev", apply_started=True, skip_terraform=False)
    assert exc.value.exit_code == 130
    assert ran and "destroy" in ran[0]


def test_handle_cancel_skips_destroy_when_declined(monkeypatch):
    import typer

    from retail_setup.cli.main import _handle_cancel

    ran: list[list[str]] = []
    monkeypatch.setattr(
        "retail_setup.cli.main._run_step_streamed",
        lambda step, root, ui: ran.append(step.cmd) or 0,
    )
    ui = _FakeUI(answer=False)
    with pytest.raises(typer.Exit) as exc:
        _handle_cancel(ui, Path("."), "dev", apply_started=True, skip_terraform=False)
    assert exc.value.exit_code == 130
    assert ran == []


def test_handle_cancel_no_cleanup_prompt_before_apply(monkeypatch):
    import typer

    from retail_setup.cli.main import _handle_cancel

    ran: list[list[str]] = []
    monkeypatch.setattr(
        "retail_setup.cli.main._run_step_streamed",
        lambda step, root, ui: ran.append(step.cmd) or 0,
    )
    # answer=True would destroy if asked; apply_started=False must not even ask.
    ui = _FakeUI(answer=True)
    with pytest.raises(typer.Exit) as exc:
        _handle_cancel(ui, Path("."), "dev", apply_started=False, skip_terraform=False)
    assert exc.value.exit_code == 130
    assert ran == []


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


def test_deploy_offers_reset_when_workspace_exists(monkeypatch):
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    monkeypatch.setattr("retail_setup.cli.main._workspace_exists", lambda *_: True)
    monkeypatch.setattr("subprocess.run", lambda *a, **k: SimpleNamespace(returncode=0))
    # Accept the reset prompt ("y"), then decline the destroy gate ("n") to stop.
    result = runner.invoke(app, ["deploy", "--env", "dev"], input="y\nn\n")
    assert "already exists" in result.output
    assert "destroy" in result.output  # confirming reset took the recreate path
    assert "Aborted by user" in result.output


def test_deploy_skips_reset_prompt_when_workspace_absent(monkeypatch):
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    monkeypatch.setattr("retail_setup.cli.main._workspace_exists", lambda *_: False)
    monkeypatch.setattr("subprocess.run", lambda *a, **k: SimpleNamespace(returncode=0))
    result = runner.invoke(app, ["deploy", "--env", "dev"], input="n\n")
    assert "already exists" not in result.output


def test_deploy_yes_skips_reset_prompt(monkeypatch):
    seen = {"checked": False}

    def fake_exists(*_):
        seen["checked"] = True
        return True

    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    monkeypatch.setattr("retail_setup.cli.main._workspace_exists", fake_exists)
    monkeypatch.setattr("subprocess.run", lambda *a, **k: SimpleNamespace(returncode=0))
    runner.invoke(app, ["deploy", "--env", "dev", "--yes"])
    assert seen["checked"] is False  # --yes never prompts for reset


def test_workspace_exists_matches_display_name(monkeypatch):
    monkeypatch.setattr("retail_setup.cli.main.shutil.which", lambda _c: "C:/az.cmd")
    monkeypatch.setattr(
        "subprocess.run",
        lambda *a, **k: subprocess.CompletedProcess(
            a, 0, stdout='{"value": [{"displayName": "retail-demo-dev"}]}'
        ),
    )
    from retail_setup.cli.main import _workspace_exists

    assert _workspace_exists(Path("."), "retail-demo-dev") is True
    assert _workspace_exists(Path("."), "missing") is False


def test_workspace_exists_false_when_az_unavailable(monkeypatch):
    monkeypatch.setattr("retail_setup.cli.main.shutil.which", lambda _c: None)
    from retail_setup.cli.main import _workspace_exists

    assert _workspace_exists(Path("."), "retail-demo-dev") is False


def test_workspace_name_prefers_environment_overlay(tmp_path):
    cfg = tmp_path / "deploy" / "config"
    (cfg / "environments").mkdir(parents=True)
    (cfg / "deploy.yml").write_text("workspace:\n  name: retail-demo\n", encoding="utf-8")
    (cfg / "environments" / "dev.yml").write_text(
        "workspace:\n  name: retail-demo-dev\n", encoding="utf-8"
    )
    from retail_setup.cli.main import _workspace_name

    assert _workspace_name(tmp_path, "dev") == "retail-demo-dev"


def test_run_setup_pipeline_warns_takes_a_while_and_retries(monkeypatch, capsys):
    monkeypatch.setattr(cli.time, "sleep", lambda *_a: None)
    attempts = {"n": 0}

    def fake_run(_cmd, cwd=None):
        attempts["n"] += 1
        # Fail the first trigger, succeed on the retry.
        return SimpleNamespace(returncode=1 if attempts["n"] == 1 else 0)

    monkeypatch.setattr("subprocess.run", fake_run)
    _run_setup_pipeline(Path("."), "dev")

    out = capsys.readouterr().out
    assert "can take a while" in out
    assert attempts["n"] == 2  # retried once after the initial failure


def test_run_setup_pipeline_gives_up_after_max_attempts(monkeypatch, capsys):
    monkeypatch.setattr(cli.time, "sleep", lambda *_a: None)
    runs = {"n": 0}

    def fake_run(_cmd, cwd=None):
        runs["n"] += 1
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr("subprocess.run", fake_run)
    _run_setup_pipeline(Path("."), "dev")

    captured = capsys.readouterr()
    assert runs["n"] == _PIPELINE_TRIGGER_ATTEMPTS
    assert "run 'setup-pipeline' manually" in captured.err


def test_ontology_relink_hint_names_workspace_and_command(tmp_path, capsys):
    cfg = tmp_path / "deploy" / "config"
    (cfg / "environments").mkdir(parents=True)
    (cfg / "deploy.yml").write_text("workspace:\n  name: retail-demo\n", encoding="utf-8")
    (cfg / "environments" / "dev.yml").write_text(
        "workspace:\n  name: retail-demo-dev\n", encoding="utf-8"
    )
    from retail_setup.cli.main import _print_ontology_relink_hint

    _print_ontology_relink_hint(tmp_path, "dev")

    out = capsys.readouterr().out
    assert "RetailOntology_AutoGen" in out
    assert "deploy.scripts.taskflow deploy --workspace retail-demo-dev" in out


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
