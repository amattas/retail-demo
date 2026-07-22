import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import typer
import yaml
from typer.testing import CliRunner

import retail_setup.cli.main as cli
from retail_setup.cli import _deploy_journal
from retail_setup.cli.main import (
    app,
    _deploy_taskflow,
    _deploy_plan,
    _validate_azure_cli_tenant,
    _DELETION_WAIT_TIMEOUT_SECONDS,
    _PIPELINE_TRIGGER_ATTEMPTS,
    _run_setup_pipeline,
)

runner = CliRunner()

# The real deploy-dependency preflight, captured before the autouse stub below
# replaces it, so the dedicated tests can exercise the genuine logic.
_REAL_CHECK_DEPLOY_DEPENDENCIES = cli._check_deploy_dependencies


@pytest.fixture(autouse=True)
def _stub_deploy_dependency_check(monkeypatch):
    """Neutralize the deploy dependency preflight for command tests.

    These tests mock ``subprocess.run``, so the deploy-only Python packages
    (fabric-cicd, the Azure SDKs) need not be installed to exercise deploy
    behavior. Stub the in-process preflight to a no-op; the preflight's own
    logic is covered directly by the dedicated tests below.
    """
    monkeypatch.setattr(cli, "_check_deploy_dependencies", lambda: None)


def _seed_deploy_config(root: Path, env: str = "dev") -> None:
    """Write a minimal deploy config so CLI tests don't touch the real repo."""
    base = root / "deploy" / "config"
    (base / "environments").mkdir(parents=True)
    (base / "deploy.yml").write_text(
        yaml.safe_dump(
            {
                "auth": {"mode": "azure_cli"},
                "workspace": {"description": "Retail demo"},
                "lakehouse": {
                    "name": "retail_lakehouse",
                    "enable_schemas": True,
                },
                "eventhouse": {
                    "name": "retail_eventhouse",
                    "kql_database_name": "retail_eventhouse",
                    "kql_scripts": [],
                },
                "notebooks": {"include": ["core"]},
                "powerbi": {
                    "semantic_model_name": "retail_model",
                    "report_name": "retail_model",
                },
                "deployment": {
                    "item_types_in_scope": ["Lakehouse", "Notebook"],
                    "publish_skip": False,
                    "unpublish_skip": True,
                },
            },
            sort_keys=False,
        )
    )
    (base / "environments" / f"{env}.yml").write_text(
        yaml.safe_dump(
            {
                "tenant_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "workspace": {"name": f"retail-demo-{env}"},
            }
        )
    )
    output_root = root / "deploy" / ".generated" / env
    output_root.mkdir(parents=True)
    (output_root / "terraform-output.json").write_text(
        json.dumps(
            {
                "deployment_environment": env,
                "tenant_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "workspace_id": "11111111-1111-4111-8111-111111111111",
                "workspace_name": f"retail-demo-{env}",
                "lakehouse_id": "22222222-2222-4222-8222-222222222222",
                "lakehouse_name": "retail_lakehouse",
                "eventhouse_id": "33333333-3333-4333-8333-333333333333",
                "eventhouse_name": "retail_eventhouse",
                "kql_database_id": "44444444-4444-4444-8444-444444444444",
                "kql_database_name": "retail_eventhouse",
            }
        ),
        encoding="utf-8",
    )


def _always_ok(*_args, **_kwargs) -> SimpleNamespace:
    return SimpleNamespace(returncode=0)


def test_check_deploy_dependencies_passes_when_all_present(monkeypatch):
    monkeypatch.setattr("importlib.util.find_spec", lambda name, *a, **k: object())
    # Must not raise when every deploy package resolves.
    _REAL_CHECK_DEPLOY_DEPENDENCIES()


def test_check_deploy_dependencies_reports_missing(monkeypatch, capsys):
    def fake_find_spec(name, *_args, **_kwargs):
        if name == "fabric_cicd":
            return None
        # Simulate a completely absent `azure` namespace (find_spec raises when
        # a parent package is missing, not just returns None).
        raise ModuleNotFoundError(name)

    monkeypatch.setattr("importlib.util.find_spec", fake_find_spec)

    with pytest.raises(typer.Exit) as excinfo:
        _REAL_CHECK_DEPLOY_DEPENDENCIES()

    assert excinfo.value.exit_code == 1
    err = capsys.readouterr().err
    assert "fabric-cicd" in err
    assert "azure-identity" in err
    assert "azure-kusto-data" in err
    assert "requirements-deploy.txt" in err


def test_deploy_aborts_before_running_steps_when_dependencies_missing(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)
    ran = {"subprocess": False}

    def boom():
        typer.echo("Deploy needs Python packages that aren't installed", err=True)
        raise typer.Exit(code=1)

    def track_run(*_args, **_kwargs):
        ran["subprocess"] = True
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(cli, "_check_deploy_dependencies", boom)
    monkeypatch.setattr("subprocess.run", track_run)

    result = runner.invoke(
        app, ["deploy", "--repo-root", str(tmp_path), "--env", "dev", "--yes"]
    )

    assert result.exit_code == 1, result.output
    assert "aren't installed" in result.output
    # The preflight runs first, so no deploy step (and no subprocess) executes.
    assert ran["subprocess"] is False


def test_deploy_dry_run_skips_dependency_check(monkeypatch):
    checked = {"called": False}

    def track_check():
        checked["called"] = True

    monkeypatch.setattr(cli, "_check_deploy_dependencies", track_check)
    monkeypatch.setattr("subprocess.run", lambda *a, **k: None)

    result = runner.invoke(app, ["deploy", "--env", "dev", "--dry-run"])

    assert result.exit_code == 0, result.output
    # A dry run only prints the plan; it must not require deploy packages.
    assert checked["called"] is False


def test_dry_run_prints_full_plan_and_executes_nothing(monkeypatch):
    calls = []
    monkeypatch.setattr("subprocess.run", lambda *a, **k: calls.append(a))
    result = runner.invoke(app, ["deploy", "--env", "dev", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert calls == []
    out = result.output
    assert "generate_configs" in out and "terraform" in out
    assert "build_artifacts" in out and "core setup ml ontology reset stream" in out.replace(
        "'", ""
    )
    assert "deploy_items" in out and "apply_kql" in out and "validate_deployment" in out


def test_dry_run_uses_configured_auth_mode(monkeypatch):
    assert hasattr(cli, "_auth_mode")
    monkeypatch.setattr(
        cli,
        "_auth_mode",
        lambda *_args: "azure_powershell",
    )

    result = runner.invoke(app, ["deploy", "--env", "dev", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert "--auth-mode azure_powershell" in result.output


def test_dry_run_falls_back_when_auth_config_is_unavailable(monkeypatch):
    assert hasattr(cli, "_auth_mode")

    def unavailable(*_args):
        raise OSError("deploy config unavailable")

    monkeypatch.setattr(cli, "_auth_mode", unavailable)

    result = runner.invoke(app, ["deploy", "--env", "dev", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert "--auth-mode azure_cli" in result.output


def test_skip_terraform_drops_terraform_steps():
    plan = _deploy_plan("dev", skip_terraform=True)
    assert not any(step.cmd and step.cmd[0] == "terraform" for step in plan)
    generate = plan[0].cmd
    assert "--terraform-output" in generate
    assert "deploy/.generated/dev/terraform-output.json" in generate
    flat = " ".join(" ".join(map(str, step.cmd)) for step in plan)
    assert "generate_configs" in flat and "deploy_items" in flat


def test_skip_terraform_rejects_missing_outputs_before_commands(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)
    (tmp_path / "deploy" / ".generated" / "dev" / "terraform-output.json").unlink()
    calls = []
    monkeypatch.setattr("subprocess.run", lambda *args, **kwargs: calls.append(args))

    result = runner.invoke(
        app,
        [
            "deploy",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--skip-terraform",
            "--yes",
        ],
    )

    assert result.exit_code == 1, result.output
    assert "--skip-terraform requires complete Terraform outputs" in result.output
    assert calls == []


def test_skip_terraform_rejects_placeholder_outputs(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)
    output_path = tmp_path / "deploy" / ".generated" / "dev" / "terraform-output.json"
    outputs = json.loads(output_path.read_text(encoding="utf-8"))
    outputs["workspace_id"] = "00000000-0000-0000-0000-000000000001"
    output_path.write_text(json.dumps(outputs), encoding="utf-8")
    calls = []
    monkeypatch.setattr("subprocess.run", lambda *args, **kwargs: calls.append(args))

    result = runner.invoke(
        app,
        [
            "deploy",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--skip-terraform",
            "--yes",
        ],
    )

    assert result.exit_code == 1, result.output
    assert "placeholder" in result.output
    assert calls == []


def test_deploy_rejects_unmigrated_legacy_terraform_state(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)
    legacy_state = tmp_path / "deploy" / "terraform" / "terraform.tfstate"
    legacy_state.parent.mkdir(parents=True)
    legacy_state.write_text("{}\n", encoding="utf-8")
    calls = []
    monkeypatch.setattr("subprocess.run", lambda *args, **kwargs: calls.append(args))

    result = runner.invoke(
        app,
        [
            "deploy",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--yes",
        ],
    )

    assert result.exit_code == 1, result.output
    assert "Legacy Terraform state found" in result.output
    assert "deploy/.generated" in result.output.replace("\\", "/")
    assert calls == []


def test_plan_propagates_selected_auth_mode():
    import inspect

    assert "auth_mode" in inspect.signature(_deploy_plan).parameters
    plan = _deploy_plan(
        "dev",
        skip_terraform=True,
        auth_mode="azure_powershell",
    )
    authenticated = [
        step.cmd
        for step in plan
        if any(
            name in step.cmd for name in ("deploy.scripts.deploy_items", "deploy.scripts.apply_kql")
        )
    ]

    assert len(authenticated) == 2
    assert all(
        command[command.index("--auth-mode") + 1] == "azure_powershell" for command in authenticated
    )


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


def test_plan_renders_notebooks_before_building_artifacts():
    plan = _deploy_plan("dev", skip_terraform=False)
    cmds = [" ".join(map(str, s.cmd)) for s in plan]
    render_idx = next(i for i, c in enumerate(cmds) if "retail_setup.cli.main render" in c)
    build_idx = next(i for i, c in enumerate(cmds) if "deploy.scripts.build_artifacts" in c)
    # Render must refresh utility/out/ before build_artifacts stages it, so a
    # stale pre-rendered notebook (e.g. a GOLD_SOURCE_TABLES that drifted from
    # the engine) can never be deployed.
    assert render_idx < build_idx
    assert "render --env dev" in cmds[render_idx]


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


def test_plan_isolates_terraform_state_and_data_directory():
    dev_plan = _deploy_plan("demo-east", skip_terraform=False)
    west_plan = _deploy_plan("demo-west", skip_terraform=False)
    dev_terraform = [step for step in dev_plan if step.cmd and step.cmd[0] == "terraform"]
    west_terraform = [step for step in west_plan if step.cmd and step.cmd[0] == "terraform"]

    dev_init = next(step for step in dev_terraform if "init" in step.cmd)
    west_init = next(step for step in west_terraform if "init" in step.cmd)
    assert "-backend-config=path=../.generated/demo-east/terraform.tfstate" in dev_init.cmd
    assert "-backend-config=path=../.generated/demo-west/terraform.tfstate" in west_init.cmd
    assert all(
        step.process_environment["TF_DATA_DIR"] == "deploy/.generated/demo-east/.terraform"
        for step in dev_terraform
    )
    assert all(
        step.process_environment["TF_DATA_DIR"] == "deploy/.generated/demo-west/.terraform"
        for step in west_terraform
    )


def test_recreate_inserts_destroy_and_deletion_wait_before_apply():
    plan = _deploy_plan("dev", skip_terraform=False, recreate=True)
    step_ids = [s.step_id for s in plan]
    init_idx = step_ids.index("terraform-init")
    destroy_idx = step_ids.index("terraform-destroy-recreate")
    wait_idx = step_ids.index("deletion-wait")
    apply_idx = step_ids.index("terraform-apply")
    assert init_idx < destroy_idx < wait_idx < apply_idx
    assert plan[destroy_idx].needs_confirmation
    wait_step = plan[wait_idx]
    assert wait_step.action is not None
    assert wait_step.required is True
    assert str(_DELETION_WAIT_TIMEOUT_SECONDS) in wait_step.description
    cmds = [" ".join(map(str, s.cmd)) for s in plan]
    assert not any(" plan " in f" {c} " for c in cmds)


def test_deletion_wait_action_calls_helper_with_workspace_and_auth_mode(monkeypatch):
    calls = []
    monkeypatch.setattr(
        cli,
        "_wait_for_workspace_deletion",
        lambda repo_root, ws, auth: calls.append((repo_root, ws, auth)),
    )
    plan = _deploy_plan(
        "dev",
        skip_terraform=False,
        recreate=True,
        auth_mode="azure_powershell",
        workspace_name="retail-demo-dev",
    )
    wait_step = next(s for s in plan if s.step_id == "deletion-wait")
    wait_step.action(Path("/repo"))
    assert calls == [(Path("/repo"), "retail-demo-dev", "azure_powershell")]


def test_deletion_wait_dry_run_shows_step_but_makes_no_request(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)
    called = []
    monkeypatch.setattr(cli, "_wait_for_workspace_deletion", lambda *a: called.append(a) or None)
    result = runner.invoke(
        app,
        ["deploy", "--repo-root", str(tmp_path), "--env", "dev", "--recreate", "--dry-run"],
    )
    assert result.exit_code == 0, result.output
    assert "Wait for Fabric to release workspace" in result.output
    assert called == []


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
    assert "-var-file=../.generated/prod/terraform.tfvars" in step.cmd
    assert step.process_environment["TF_DATA_DIR"] == "deploy/.generated/prod/.terraform"


def test_recreate_rejects_skip_terraform(monkeypatch):
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    result = runner.invoke(app, ["deploy", "--env", "dev", "--recreate", "--skip-terraform"])
    assert result.exit_code == 1, result.output
    assert "--recreate cannot be combined with --skip-terraform" in result.output


def test_recreate_dry_run_shows_destroy_step():
    result = runner.invoke(app, ["deploy", "--env", "dev", "--recreate", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "destroy" in result.output


def test_recreate_warns_and_aborts_on_decline(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    monkeypatch.setattr("subprocess.run", _always_ok)
    # Decline the first gated step (Terraform destroy).
    result = runner.invoke(
        app,
        ["deploy", "--repo-root", str(tmp_path), "--env", "dev", "--recreate"],
        input="n\n",
    )
    assert result.exit_code == 1, result.output
    assert "WARNING" in result.output
    assert "Aborted by user" in result.output
    journal = json.loads(_deploy_journal.journal_path(tmp_path, "dev").read_text())
    destroy_step = next(s for s in journal["steps"] if s["step_id"] == "terraform-destroy-recreate")
    assert destroy_step["status"] == "FAILED"
    assert journal["status"] == "FAILED"


def test_deploy_prints_linear_command_dividers(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    monkeypatch.setattr("subprocess.run", _always_ok)

    result = runner.invoke(
        app,
        ["deploy", "--repo-root", str(tmp_path), "--env", "dev", "--yes", "--skip-terraform"],
    )

    assert result.exit_code == 0, result.output
    assert "Running step" in result.output
    assert "====" in result.output
    assert "$items" not in result.output


def test_deploy_offers_reset_when_workspace_exists(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    monkeypatch.setattr("retail_setup.cli.main._workspace_exists", lambda *_: True)
    monkeypatch.setattr("subprocess.run", _always_ok)
    # Accept the reset prompt ("y"), then decline the destroy gate ("n") to stop.
    result = runner.invoke(
        app, ["deploy", "--repo-root", str(tmp_path), "--env", "dev"], input="y\nn\n"
    )
    assert "already exists" in result.output
    assert "destroy" in result.output  # confirming reset took the recreate path
    assert "Aborted by user" in result.output


def test_deploy_skips_reset_prompt_when_workspace_absent(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    monkeypatch.setattr("retail_setup.cli.main._workspace_exists", lambda *_: False)
    monkeypatch.setattr("subprocess.run", _always_ok)
    result = runner.invoke(
        app, ["deploy", "--repo-root", str(tmp_path), "--env", "dev"], input="n\n"
    )
    assert "already exists" not in result.output


def test_deploy_yes_skips_reset_prompt(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)
    seen = {"checked": False}

    def fake_exists(*_):
        seen["checked"] = True
        return True

    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    monkeypatch.setattr("retail_setup.cli.main._workspace_exists", fake_exists)
    monkeypatch.setattr("subprocess.run", _always_ok)
    runner.invoke(app, ["deploy", "--repo-root", str(tmp_path), "--env", "dev", "--yes"])
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
    commands = []

    def fake_run(cmd, cwd=None):
        commands.append(cmd)
        attempts["n"] += 1
        # Fail the first trigger, succeed on the retry.
        return SimpleNamespace(returncode=1 if attempts["n"] == 1 else 0)

    monkeypatch.setattr("subprocess.run", fake_run)
    _run_setup_pipeline(Path("."), "dev", auth_mode="azure_powershell")

    out = capsys.readouterr().out
    assert "can take a while" in out
    assert attempts["n"] == 2  # retried once after the initial failure
    assert all(
        command[command.index("--auth-mode") + 1] == "azure_powershell" for command in commands
    )


def test_deploy_taskflow_passes_selected_auth_mode(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(cli, "_workspace_name", lambda *_args: "retail-demo-dev")
    monkeypatch.setattr(
        "subprocess.run",
        lambda cmd, cwd=None: calls.append(cmd) or SimpleNamespace(returncode=0),
    )

    _deploy_taskflow(tmp_path, "dev", auth_mode="azure_powershell")

    assert calls[0][calls[0].index("--auth-mode") + 1] == "azure_powershell"


def test_run_setup_pipeline_gives_up_after_max_attempts(monkeypatch, capsys):
    monkeypatch.setattr(cli.time, "sleep", lambda *_a: None)
    runs = {"n": 0}

    def fake_run(_cmd, cwd=None):
        runs["n"] += 1
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr("subprocess.run", fake_run)
    # The pipeline trigger is only ever called once the operator requested it,
    # so exhausting all retries now fails the deploy instead of just warning.
    with pytest.raises(typer.Exit) as exc_info:
        _run_setup_pipeline(Path("."), "dev")

    assert exc_info.value.exit_code == 1
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


def test_deploy_reports_missing_terraform_without_traceback(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)

    def fake_run(cmd, *args, **kwargs):
        if cmd and cmd[0] == "terraform":
            raise FileNotFoundError("terraform")
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr("subprocess.run", fake_run)
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    result = runner.invoke(app, ["deploy", "--repo-root", str(tmp_path), "--env", "dev", "--yes"])
    assert result.exit_code == 127, result.output
    assert "Required executable not found: terraform" in result.output
    assert "--skip-terraform" in result.output
    assert "Traceback" not in result.output
    journal = json.loads(_deploy_journal.journal_path(tmp_path, "dev").read_text())
    assert journal["status"] == "FAILED"
    tf_init = next(s for s in journal["steps"] if s["step_id"] == "terraform-init")
    assert tf_init["status"] == "FAILED"
    assert tf_init["error"] and "terraform" in tf_init["error"]
    # No live secrets, tokens, or raw subprocess output ever land in the journal.
    dumped = json.dumps(journal)
    assert "stdout" not in dumped and "stderr" not in dumped and "token" not in dumped.lower()


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


# --- Durable atomic journal: shape, redaction, and atomic writes ---------------


def test_journal_write_is_atomic_via_temp_file_and_replace(tmp_path, monkeypatch):
    journal = _deploy_journal.start_run("dev", targets={"workspace_name": "retail-demo-dev"})
    _deploy_journal.add_step(journal, "step-a", "Do a thing", required=True)

    seen_tmp_names = []
    real_replace = _deploy_journal.os.replace

    def spy_replace(src, dst):
        seen_tmp_names.append(Path(src).name)
        assert not Path(dst).exists()  # first write: nothing to replace yet
        return real_replace(src, dst)

    monkeypatch.setattr(_deploy_journal.os, "replace", spy_replace)
    _deploy_journal.write(tmp_path, journal)

    path = _deploy_journal.journal_path(tmp_path, "dev")
    assert path.is_file()
    assert seen_tmp_names and seen_tmp_names[0] != path.name  # wrote to a temp name first
    # No leftover temp files after a successful write.
    assert list(path.parent.glob("*.tmp")) == []


def test_journal_shape_has_required_fields_and_no_secrets(tmp_path):
    journal = _deploy_journal.start_run(
        "dev",
        targets={"workspace_name": "retail-demo-dev", "lakehouse_name": "retail_lakehouse"},
    )
    _deploy_journal.add_step(
        journal, "generate-configs", "Generate deployment configs", required=True
    )
    _deploy_journal.mark_running(journal, "generate-configs")
    _deploy_journal.mark_succeeded(journal, "generate-configs", exit_code=0)
    _deploy_journal.write(tmp_path, journal)

    data = json.loads(_deploy_journal.journal_path(tmp_path, "dev").read_text())
    assert set(data.keys()) >= {
        "run_id",
        "environment",
        "status",
        "started_at",
        "updated_at",
        "targets",
        "steps",
    }
    assert data["run_id"]  # unique run id present
    assert data["environment"] == "dev"
    assert data["status"] in ("RUNNING", "SUCCEEDED", "DEGRADED", "FAILED")
    step = data["steps"][0]
    assert set(step.keys()) >= {
        "step_id",
        "description",
        "classification",
        "status",
        "started_at",
        "ended_at",
        "exit_code",
        "error",
    }
    dumped = json.dumps(data).lower()
    for forbidden in ("token", "authorization", "bearer", "secret", "password"):
        assert forbidden not in dumped


def test_journal_error_is_redacted_and_truncated(tmp_path):
    journal = _deploy_journal.start_run("dev", targets={})
    _deploy_journal.add_step(journal, "step-a", "Do a thing", required=True)
    long_message = "boom " + ("x" * 1000)
    _deploy_journal.mark_failed(journal, "step-a", exit_code=1, error=long_message)
    _deploy_journal.write(tmp_path, journal)

    data = json.loads(_deploy_journal.journal_path(tmp_path, "dev").read_text())
    error = data["steps"][0]["error"]
    assert len(error) <= 300
    assert error.startswith("boom")


# --- Secret redaction: bearer tokens, JWT-like values, named assignments ------

_SENTINEL_SECRET = "sUp3rS3cr3tSentinelValue_DoNotLeak987"  # noqa: S105 - test fixture, not real


@pytest.mark.parametrize(
    "message",
    [
        f"401 Unauthorized: Bearer {_SENTINEL_SECRET}",
        f"auth header rejected -- Authorization: Bearer {_SENTINEL_SECRET}.extra.stuff",
        f"token={_SENTINEL_SECRET}",
        f"Token: {_SENTINEL_SECRET}",
        f"access_token={_SENTINEL_SECRET}",
        f"access-token={_SENTINEL_SECRET}",
        f"accessToken={_SENTINEL_SECRET}",
        f"ACCESS_TOKEN={_SENTINEL_SECRET}",
        f'client_secret: "{_SENTINEL_SECRET}"',
        f"client-secret={_SENTINEL_SECRET}",
        f"clientSecret={_SENTINEL_SECRET}",
        f"api_key={_SENTINEL_SECRET}",
        f"api-key={_SENTINEL_SECRET}",
        f"apiKey={_SENTINEL_SECRET}",
        f"API_KEY={_SENTINEL_SECRET}",
        f"password={_SENTINEL_SECRET}",
        f'password: "{_SENTINEL_SECRET}"',
        f"?access_token={_SENTINEL_SECRET}&foo=bar",
        (
            "standalone jwt eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0."
            "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c rejected"
        ),
    ],
)
def test_redact_strips_secret_value_in_all_named_and_bearer_forms(message):
    redacted = _deploy_journal._redact(message)

    assert _SENTINEL_SECRET not in redacted
    assert "eyJhbGciOiJIUzI1NiJ9" not in redacted  # JWT header segment also gone
    assert redacted  # a short, useful message is still preserved
    assert "[REDACTED]" in redacted or "Bearer [REDACTED]" in redacted


def test_redact_preserves_plain_error_text_without_secrets():
    message = "terraform apply failed: resource already exists"
    assert _deploy_journal._redact(message) == message


def test_journal_never_serializes_sentinel_secret_from_step_error(tmp_path):
    """End-to-end: an exception message containing secrets in several forms
    must never reach the serialized journal file, even though callers only
    ever pass short exception messages (never raw stdout/stderr/env)."""
    journal = _deploy_journal.start_run("dev", targets={})
    _deploy_journal.add_step(journal, "step-a", "Do a thing", required=True)
    leaky_message = (
        f"request failed: Authorization: Bearer {_SENTINEL_SECRET} "
        f"and client_secret={_SENTINEL_SECRET} rejected"
    )
    _deploy_journal.mark_failed(journal, "step-a", exit_code=1, error=leaky_message)
    _deploy_journal.write(tmp_path, journal)

    raw = _deploy_journal.journal_path(tmp_path, "dev").read_text()
    assert _SENTINEL_SECRET not in raw
    data = json.loads(raw)
    assert "rejected" in data["steps"][0]["error"]  # still a useful summary


def test_journal_never_serializes_sentinel_secret_from_skip_reason(tmp_path):
    journal = _deploy_journal.start_run("dev", targets={})
    _deploy_journal.add_step(journal, "step-a", "Do a thing", required=False)
    _deploy_journal.mark_skipped(
        journal, "step-a", reason=f"declined; last token=access_token={_SENTINEL_SECRET}"
    )
    _deploy_journal.write(tmp_path, journal)

    raw = _deploy_journal.journal_path(tmp_path, "dev").read_text()
    assert _SENTINEL_SECRET not in raw


# --- Required/optional classification -> overall run status transitions -------


def test_compute_status_required_failure_is_failed():
    journal = _deploy_journal.start_run("dev", targets={})
    _deploy_journal.add_step(journal, "required-step", "Required thing", required=True)
    _deploy_journal.add_step(journal, "optional-step", "Optional thing", required=False)
    _deploy_journal.mark_succeeded(journal, "optional-step")
    _deploy_journal.mark_failed(journal, "required-step", exit_code=1)

    assert _deploy_journal.compute_status(journal) == "FAILED"


def test_compute_status_optional_failure_is_degraded():
    journal = _deploy_journal.start_run("dev", targets={})
    _deploy_journal.add_step(journal, "required-step", "Required thing", required=True)
    _deploy_journal.add_step(journal, "optional-step", "Optional thing", required=False)
    _deploy_journal.mark_succeeded(journal, "required-step")
    _deploy_journal.mark_failed(journal, "optional-step", exit_code=1)

    assert _deploy_journal.compute_status(journal) == "DEGRADED"


def test_compute_status_all_succeeded_is_succeeded():
    journal = _deploy_journal.start_run("dev", targets={})
    _deploy_journal.add_step(journal, "a", "A", required=True)
    _deploy_journal.add_step(journal, "b", "B", required=False)
    _deploy_journal.mark_succeeded(journal, "a")
    _deploy_journal.mark_skipped(journal, "b", reason="declined by operator")

    assert _deploy_journal.compute_status(journal) == "SUCCEEDED"


def test_compute_status_pending_step_stays_running():
    journal = _deploy_journal.start_run("dev", targets={})
    _deploy_journal.add_step(journal, "a", "A", required=True)
    _deploy_journal.add_step(journal, "b", "B", required=True)
    _deploy_journal.mark_succeeded(journal, "a")
    # "b" is still PENDING.

    assert _deploy_journal.compute_status(journal) == "RUNNING"


def test_mark_required_promotes_optional_step_to_required():
    journal = _deploy_journal.start_run("dev", targets={})
    _deploy_journal.add_step(journal, "pipeline", "Trigger the setup pipeline", required=False)
    _deploy_journal.mark_required(journal, "pipeline")
    _deploy_journal.mark_failed(journal, "pipeline", exit_code=1)

    # Promoted to required, so a failure now fails the whole run.
    assert _deploy_journal.compute_status(journal) == "FAILED"


# --- Task-flow and setup-pipeline: required-once-requested, skip behavior -----


def test_taskflow_failure_is_required_and_raises_nonzero(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)
    (tmp_path / "fabric" / "taskflow").mkdir(parents=True)
    (tmp_path / "fabric" / "taskflow" / "taskflow.json").write_text("{}")
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)

    def fake_run(cmd, cwd=None):
        if "deploy.scripts.taskflow" in cmd:
            return SimpleNamespace(returncode=3)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    result = runner.invoke(
        app, ["deploy", "--repo-root", str(tmp_path), "--env", "dev", "--yes", "--skip-terraform"]
    )

    assert result.exit_code == 3, result.output
    assert "Deploy complete" not in result.output  # banner must not appear on failure
    journal = json.loads(_deploy_journal.journal_path(tmp_path, "dev").read_text())
    assert journal["status"] == "FAILED"
    taskflow_step = next(s for s in journal["steps"] if s["step_id"] == "task-flow-deploy")
    assert taskflow_step["classification"] == "required"
    assert taskflow_step["status"] == "FAILED"
    assert taskflow_step["exit_code"] == 3


def test_requested_setup_pipeline_failure_is_required_and_raises_nonzero(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    monkeypatch.setattr(cli.time, "sleep", lambda *_a: None)

    def fake_run(cmd, cwd=None):
        if "deploy.scripts.run_pipeline" in cmd:
            return SimpleNamespace(returncode=5)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    result = runner.invoke(
        app,
        ["deploy", "--repo-root", str(tmp_path), "--env", "dev", "--skip-terraform"],
        input="y\n",  # accept the setup-pipeline prompt
    )

    assert result.exit_code == 5, result.output
    assert "Deploy complete" not in result.output
    journal = json.loads(_deploy_journal.journal_path(tmp_path, "dev").read_text())
    assert journal["status"] == "FAILED"
    pipeline_step = next(s for s in journal["steps"] if s["step_id"] == "setup-pipeline-trigger")
    assert pipeline_step["classification"] == "required"  # promoted once requested
    assert pipeline_step["status"] == "FAILED"


def test_declined_setup_pipeline_is_skipped_and_deploy_still_succeeds(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    monkeypatch.setattr("subprocess.run", _always_ok)

    result = runner.invoke(
        app,
        ["deploy", "--repo-root", str(tmp_path), "--env", "dev", "--skip-terraform"],
        input="n\n",  # decline the setup-pipeline prompt
    )

    assert result.exit_code == 0, result.output
    assert "Deploy complete" in result.output
    journal = json.loads(_deploy_journal.journal_path(tmp_path, "dev").read_text())
    assert journal["status"] == "SUCCEEDED"
    pipeline_step = next(s for s in journal["steps"] if s["step_id"] == "setup-pipeline-trigger")
    assert pipeline_step["classification"] == "optional"
    assert pipeline_step["status"] == "SKIPPED"


def test_yes_flag_skips_setup_pipeline_without_failing(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    monkeypatch.setattr("subprocess.run", _always_ok)

    result = runner.invoke(
        app, ["deploy", "--repo-root", str(tmp_path), "--env", "dev", "--yes", "--skip-terraform"]
    )

    assert result.exit_code == 0, result.output
    assert "Deploy complete" in result.output
    journal = json.loads(_deploy_journal.journal_path(tmp_path, "dev").read_text())
    pipeline_step = next(s for s in journal["steps"] if s["step_id"] == "setup-pipeline-trigger")
    assert pipeline_step["status"] == "SKIPPED"  # --yes never fails the run
    assert journal["status"] == "SUCCEEDED"


def test_no_premature_complete_banner_on_required_step_failure(monkeypatch, tmp_path):
    """The 'Deploy complete' banner never prints before required work finishes."""
    _seed_deploy_config(tmp_path)
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)

    def fake_run(cmd, cwd=None, **kwargs):
        if "deploy.scripts.build_artifacts" in cmd:
            return SimpleNamespace(returncode=2)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    result = runner.invoke(
        app, ["deploy", "--repo-root", str(tmp_path), "--env", "dev", "--yes", "--skip-terraform"]
    )

    assert result.exit_code == 2, result.output
    assert "Deploy complete" not in result.output
    journal = json.loads(_deploy_journal.journal_path(tmp_path, "dev").read_text())
    assert journal["status"] == "FAILED"
    build_step = next(s for s in journal["steps"] if s["step_id"] == "build-artifacts")
    assert build_step["status"] == "FAILED"
    # Steps after the failed one were never reached (still PENDING).
    kql_step = next(s for s in journal["steps"] if s["step_id"] == "apply-kql")
    assert kql_step["status"] == "PENDING"
