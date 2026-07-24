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
    _validate_terraform_auth_boundary,
    _DELETION_WAIT_TIMEOUT_SECONDS,
    _PIPELINE_TRIGGER_ATTEMPTS,
    _run_setup_pipeline,
)
from retail_setup.contracts import load_repository_manifest, resolve_profile

runner = CliRunner()
TENANT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
REPO_ROOT = Path(__file__).resolve().parents[2]
_MANIFEST, _VALIDATION = load_repository_manifest(REPO_ROOT)


def _profile(name: str):
    return resolve_profile(_MANIFEST, _VALIDATION, name)


def _seed_deploy_config(
    root: Path,
    env: str = "dev",
    profile: str = "core",
) -> None:
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
                    "kql_scripts": [
                        "01-create-tables.kql",
                        "02-create-ingestion-mappings.kql",
                        "03-create-functions.kql",
                        "04-create-materialized-views.kql",
                        "06-ml-anomaly-detection.kql",
                        "07-pricing-approval-tables.kql",
                    ],
                },
                "notebooks": {"default_lakehouse_name": "retail_lakehouse"},
                "powerbi": {
                    "semantic_model_name": "retail_model",
                    "report_name": "retail_model",
                },
                "deployment": {
                    "profile": "core",
                    "item_types_in_scope": [
                        "Lakehouse",
                        "Notebook",
                        "SemanticModel",
                        "Report",
                        "KQLQueryset",
                        "DataPipeline",
                        "MLExperiment",
                        "DataAgent",
                    ],
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
                "deployment": {"profile": profile},
            }
        )
    )
    output_root = root / "deploy" / ".generated" / env
    output_root.mkdir(parents=True)
    (output_root / "terraform-output.json").write_text(
        json.dumps(
            {
                "deployment_environment": env,
                "deployment_profile": profile,
                "tenant_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "workspace_id": "11111111-1111-4111-8111-111111111111",
                "workspace_name": f"retail-demo-{env}",
                "lakehouse_id": "22222222-2222-4222-8222-222222222222",
                "lakehouse_name": "retail_lakehouse",
                "eventhouse_id": "33333333-3333-4333-8333-333333333333",
                "eventhouse_name": "retail_eventhouse",
                "kql_database_id": "44444444-4444-4444-8444-444444444444",
                "kql_database_name": "retail_eventhouse",
                **(
                    {
                        "spark_custom_pool_id": (
                            "55555555-5555-4555-8555-555555555555"
                        )
                    }
                    if profile == "full-demo"
                    else {}
                ),
            }
        ),
        encoding="utf-8",
    )


def _always_ok(*_args, **_kwargs) -> SimpleNamespace:
    return SimpleNamespace(returncode=0)


def test_dry_run_prints_full_plan_and_executes_nothing(monkeypatch):
    calls = []
    monkeypatch.setattr("subprocess.run", lambda *a, **k: calls.append(a))
    result = runner.invoke(app, ["deploy", "--env", "dev", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert calls == []
    out = result.output
    assert "generate_configs" in out and "terraform" in out
    assert "build_artifacts" in out and "--profile core" in out
    assert "asset.lakehouse" in out
    assert f"Manifest: {_MANIFEST.version}" in out
    assert "Expected staged items: 5 infrastructure + 0 Reporting = 5 total" in out
    assert "Profile support: core" in out
    assert "reset" not in out
    assert "deploy_items" in out and "apply_kql" not in out
    assert "validate_deployment" in out


def test_dry_run_uses_azure_powershell_for_python_clients_when_terraform_skips(
    tmp_path,
):
    _seed_deploy_config(tmp_path)
    base_path = tmp_path / "deploy" / "config" / "deploy.yml"
    base = yaml.safe_load(base_path.read_text(encoding="utf-8"))
    base["auth"]["mode"] = "azure_powershell"
    base_path.write_text(yaml.safe_dump(base), encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "deploy",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--skip-terraform",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "--auth-mode azure_powershell" in result.output
    assert "terraform apply" not in result.output


@pytest.mark.parametrize(
    "terraform_args",
    [[], ["--recreate"]],
    ids=["apply", "destroy-before-recreate"],
)
def test_azure_powershell_rejects_terraform_before_any_command(
    monkeypatch,
    tmp_path,
    terraform_args,
):
    _seed_deploy_config(tmp_path)
    base_path = tmp_path / "deploy" / "config" / "deploy.yml"
    base = yaml.safe_load(base_path.read_text(encoding="utf-8"))
    base["auth"]["mode"] = "azure_powershell"
    base_path.write_text(yaml.safe_dump(base), encoding="utf-8")
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
            "--dry-run",
            *terraform_args,
        ],
    )

    assert result.exit_code == 1
    assert "cannot authorize the Fabric Terraform provider" in result.output
    assert "Python Fabric clients" in result.output
    assert calls == []


def test_terraform_auth_accepts_one_provider_credential_and_binds_tenant():
    config = SimpleNamespace(
        auth_mode="azure_powershell",
        tenant_id=TENANT_ID,
    )
    provider_environment = {
        "FABRIC_TENANT_ID": TENANT_ID,
        "FABRIC_CLIENT_ID": "provider-client",
        "FABRIC_CLIENT_SECRET": "provider-secret",
    }

    _validate_terraform_auth_boundary(
        config,
        skip_terraform=False,
        environment=provider_environment,
    )

    provider_environment["FABRIC_TENANT_ID"] = (
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    )
    with pytest.raises(ValueError, match="configured deployment tenant"):
        _validate_terraform_auth_boundary(
            config,
            skip_terraform=False,
            environment=provider_environment,
        )


def test_dry_run_threads_configured_targets_without_printing_tenant(tmp_path):
    _seed_deploy_config(tmp_path)
    env_path = tmp_path / "deploy" / "config" / "environments" / "dev.yml"
    overlay = yaml.safe_load(env_path.read_text(encoding="utf-8"))
    overlay["eventhouse"] = {
        "name": "renamed_eventhouse",
        "kql_database_name": "renamed_eventhouse",
    }
    env_path.write_text(yaml.safe_dump(overlay), encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "deploy",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "--kql-database-name renamed_eventhouse" in result.output
    assert "--tenant-id [REDACTED]" in result.output
    assert TENANT_ID not in result.output


def test_dry_run_rejects_config_load_errors_instead_of_falling_back(monkeypatch):
    def unavailable(*_args):
        raise OSError("deploy config unavailable")

    monkeypatch.setattr(cli, "_load_deploy_environment", unavailable)

    result = runner.invoke(app, ["deploy", "--env", "dev", "--dry-run"])

    assert result.exit_code == 1
    assert "Invalid deployment config for dry-run" in result.output
    assert "default core profile" not in result.output


def test_dry_run_rejects_malformed_existing_environment_config(tmp_path):
    _seed_deploy_config(tmp_path)
    environment_path = (
        tmp_path / "deploy" / "config" / "environments" / "dev.yml"
    )
    environment_path.write_text("deployment: [\n", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "deploy",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--dry-run",
        ],
    )

    assert result.exit_code == 1
    assert "Invalid deployment config for dry-run" in result.output
    assert "default core profile" not in result.output


def test_skip_terraform_drops_terraform_steps():
    plan = _deploy_plan("dev", skip_terraform=True)
    assert not any(step.cmd and step.cmd[0] == "terraform" for step in plan)
    generate = next(
        step.cmd
        for step in plan
        if "deploy.scripts.generate_configs" in step.cmd
    )
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
        tenant_id=TENANT_ID,
        kql_database_name="renamed_eventhouse",
        profile=_profile("standard"),
        repo_root=REPO_ROOT,
    )
    authenticated = [
        step.cmd
        for step in plan
        if any(
            name in step.cmd for name in ("deploy.scripts.deploy_items", "deploy.scripts.apply_kql")
        )
    ]

    assert len(authenticated) == 3
    assert all(
        command[command.index("--auth-mode") + 1] == "azure_powershell" for command in authenticated
    )
    assert all(
        command[command.index("--tenant-id") + 1] == TENANT_ID
        for command in authenticated
    )
    build = next(
        step.cmd
        for step in plan
        if "deploy.scripts.build_artifacts" in step.cmd
    )
    assert build[build.index("--kql-database-name") + 1] == "renamed_eventhouse"


def test_plan_uses_exact_profile_and_never_stages_reset_group():
    plan = _deploy_plan(
        "dev",
        skip_terraform=False,
        profile=_profile("full-demo"),
        repo_root=REPO_ROOT,
    )
    build = next(s for s in plan if "build_artifacts" in " ".join(map(str, s.cmd)))
    cmd = build.cmd
    assert cmd[cmd.index("--profile") + 1] == "full-demo"
    assert "--notebook-groups" not in cmd
    assert "reset" not in cmd
    manifest_idx = cmd.index("--render-manifest")
    assert cmd[manifest_idx + 1] == "utility/out/render-manifest.json"


def test_plan_renders_before_artifact_builds_and_passes_manifest():
    plan = _deploy_plan(
        "dev",
        skip_terraform=False,
        profile=_profile("full-demo"),
        repo_root=REPO_ROOT,
    )
    commands = [" ".join(map(str, step.cmd)) for step in plan]
    render_idx = next(
        index
        for index, command in enumerate(commands)
        if "retail_setup.cli.main render" in command
    )
    build_indexes = [
        index
        for index, command in enumerate(commands)
        if "deploy.scripts.build_artifacts" in command
    ]

    assert build_indexes
    assert render_idx < min(build_indexes)
    for index in build_indexes:
        command = plan[index].cmd
        manifest_idx = command.index("--render-manifest")
        assert command[manifest_idx + 1] == "utility/out/render-manifest.json"


def test_plan_orders_steps_and_gates_apply():
    plan = _deploy_plan("dev", skip_terraform=False)
    cmds = [" ".join(map(str, s.cmd)) for s in plan]
    apply_idx = next(i for i, c in enumerate(cmds) if "apply" in c and "terraform" in c)
    preflight_idx = next(i for i, c in enumerate(cmds) if "profile_preflight" in c)
    assert plan[apply_idx].needs_confirmation
    # The redundant `terraform plan` step was removed; apply previews + confirms.
    assert not any(" plan " in f" {c} " for c in cmds)
    build_idx = next(i for i, c in enumerate(cmds) if "build_artifacts" in c)
    deploy_idx = next(i for i, c in enumerate(cmds) if "deploy_items" in c)
    assert preflight_idx < apply_idx < build_idx < deploy_idx


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
    preflight_idx = step_ids.index("profile-preflight")
    init_idx = step_ids.index("terraform-init")
    destroy_idx = step_ids.index("terraform-destroy-recreate")
    wait_idx = step_ids.index("deletion-wait")
    apply_idx = step_ids.index("terraform-apply")
    assert preflight_idx < init_idx < destroy_idx < wait_idx < apply_idx
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
        lambda repo_root, ws, auth, tenant: calls.append(
            (repo_root, ws, auth, tenant)
        ),
    )
    plan = _deploy_plan(
        "dev",
        skip_terraform=False,
        recreate=True,
        auth_mode="azure_powershell",
        workspace_name="retail-demo-dev",
        tenant_id=TENANT_ID,
    )
    wait_step = next(s for s in plan if s.step_id == "deletion-wait")
    wait_step.action(Path("/repo"))
    assert calls == [
        (Path("/repo"), "retail-demo-dev", "azure_powershell", TENANT_ID)
    ]


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
    _run_setup_pipeline(
        Path("."),
        "dev",
        auth_mode="azure_powershell",
        tenant_id=TENANT_ID,
    )

    out = capsys.readouterr().out
    assert "can take a while" in out
    assert attempts["n"] == 2  # retried once after the initial failure
    assert all(
        command[command.index("--auth-mode") + 1] == "azure_powershell" for command in commands
    )
    assert all(
        command[command.index("--tenant-id") + 1] == TENANT_ID
        for command in commands
    )


def test_verify_cli_is_read_only_unless_pipeline_flag_is_explicit(
    monkeypatch,
    tmp_path,
):
    commands = []
    monkeypatch.setattr(
        "subprocess.run",
        lambda cmd, cwd=None: commands.append(cmd)
        or SimpleNamespace(returncode=0),
    )

    result = runner.invoke(
        app,
        [
            "verify",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
        ],
    )
    explicit = runner.invoke(
        app,
        [
            "verify",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--run-pipeline",
        ],
    )

    assert result.exit_code == explicit.exit_code == 0
    assert "--run-pipeline" not in commands[0]
    assert "--run-pipeline" in commands[1]
    assert "deploy.scripts.verify_readiness" in commands[0]


def test_verify_cli_propagates_degraded_exit(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "subprocess.run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=3),
    )

    result = runner.invoke(
        app,
        [
            "verify",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
        ],
    )

    assert result.exit_code == 3


def test_deploy_records_degraded_readiness_without_claiming_success(
    monkeypatch,
    tmp_path,
):
    _seed_deploy_config(tmp_path, profile="standard")
    commands = []
    monkeypatch.setattr(
        "retail_setup.cli.main._validate_azure_cli_tenant",
        lambda *_: None,
    )

    def fake_run(cmd, cwd=None, **_kwargs):
        commands.append(cmd)
        if "deploy.scripts.verify_readiness" in cmd:
            return SimpleNamespace(returncode=3)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)

    result = runner.invoke(
        app,
        [
            "deploy",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--yes",
            "--skip-terraform",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "readiness is degraded" in result.output
    journal = json.loads(
        _deploy_journal.journal_path(tmp_path, "dev").read_text()
    )
    readiness = next(
        step for step in journal["steps"] if step["step_id"] == "verify-readiness"
    )
    assert readiness["status"] == "DEGRADED"
    assert journal["status"] == "DEGRADED"
    automatic_verify = next(
        command
        for command in commands
        if "deploy.scripts.verify_readiness" in command
    )
    assert "--run-pipeline" not in automatic_verify


def test_deploy_fails_when_required_readiness_evidence_fails(
    monkeypatch,
    tmp_path,
):
    _seed_deploy_config(tmp_path, profile="standard")
    monkeypatch.setattr(
        "retail_setup.cli.main._validate_azure_cli_tenant",
        lambda *_: None,
    )

    def fake_run(cmd, cwd=None, **_kwargs):
        return SimpleNamespace(
            returncode=1 if "deploy.scripts.verify_readiness" in cmd else 0
        )

    monkeypatch.setattr("subprocess.run", fake_run)

    result = runner.invoke(
        app,
        [
            "deploy",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--yes",
            "--skip-terraform",
        ],
    )

    assert result.exit_code == 1
    journal = json.loads(
        _deploy_journal.journal_path(tmp_path, "dev").read_text()
    )
    readiness = next(
        step for step in journal["steps"] if step["step_id"] == "verify-readiness"
    )
    assert readiness["status"] == "FAILED"
    assert journal["status"] == "FAILED"


def test_deploy_taskflow_passes_selected_auth_mode(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        "subprocess.run",
        lambda cmd, cwd=None: calls.append(cmd) or SimpleNamespace(returncode=0),
    )

    _deploy_taskflow(
        tmp_path,
        "dev",
        auth_mode="azure_powershell",
        tenant_id=TENANT_ID,
    )

    assert calls[0][calls[0].index("--auth-mode") + 1] == "azure_powershell"
    assert calls[0][calls[0].index("--tenant-id") + 1] == TENANT_ID
    assert calls[0][calls[0].index("--terraform-output") + 1] == (
        "deploy/.generated/dev/terraform-output.json"
    )
    assert "--workspace" not in calls[0]


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
    assert "retail-setup post-ontology --env dev" in out
    assert "--acknowledge ack.full-demo.ontology-created" in out
    assert "intentionally not published yet" in out


def test_post_ontology_plan_requires_acknowledgement_and_orders_publication(
    tmp_path,
):
    _seed_deploy_config(tmp_path, profile="full-demo")

    missing = runner.invoke(
        app,
        [
            "post-ontology",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--dry-run",
        ],
    )
    assert missing.exit_code == 1
    assert "ack.full-demo.ontology-created" in missing.output

    result = runner.invoke(
        app,
        [
            "post-ontology",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--acknowledge",
            "ack.full-demo.ontology-created",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output.index("post-ontology Data Agents") < result.output.index(
        "fully resolved workspace task flow"
    )
    assert "--environment dev --profile full-demo" in result.output
    assert "--defer-post-ontology" not in result.output


def test_post_ontology_validates_live_ontology_before_mutation(
    monkeypatch,
    tmp_path,
):
    _seed_deploy_config(tmp_path, profile="full-demo")
    calls = []
    monkeypatch.setattr(cli, "_validate_azure_cli_tenant", lambda *_args: None)
    monkeypatch.setattr(
        cli,
        "_validate_live_ontology",
        lambda *_args: (_ for _ in ()).throw(
            ValueError("ontology is not available")
        ),
    )
    monkeypatch.setattr(
        "subprocess.run",
        lambda *args, **kwargs: calls.append(args),
    )

    result = runner.invoke(
        app,
        [
            "post-ontology",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--acknowledge",
            "ack.full-demo.ontology-created",
        ],
    )

    assert result.exit_code == 1
    assert "live preflight failed" in result.output
    assert "ontology is not available" in result.output
    assert calls == []


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


def test_azure_cli_tenant_preflight_rejects_mismatched_tenant(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path)
    monkeypatch.setattr(
        "retail_setup.cli.main._active_azure_cli_tenant",
        lambda: "active-tenant",
    )

    result = runner.invoke(
        app,
        [
            "deploy",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--yes",
            "--skip-terraform",
        ],
    )
    assert result.exit_code == 1, result.output
    assert "Azure CLI tenant does not match deploy config tenant_id" in result.output
    assert "az login --tenant <configured-tenant>" in result.output
    assert TENANT_ID not in result.output
    assert "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb" not in result.output
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
    for forbidden in (
        "token",
        "authorization",
        "bearer",
        "secret",
        "password",
        TENANT_ID,
    ):
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


def test_journal_error_redacts_tenant_identifier(tmp_path):
    journal = _deploy_journal.start_run("dev", targets={})
    _deploy_journal.add_step(journal, "step-a", "Do a thing", required=True)
    _deploy_journal.mark_failed(
        journal,
        "step-a",
        exit_code=1,
        error=f"credential failed for tenant_id={TENANT_ID}",
    )
    _deploy_journal.write(tmp_path, journal)

    raw = _deploy_journal.journal_path(tmp_path, "dev").read_text()
    assert TENANT_ID not in raw
    assert "tenant_id=[REDACTED]" in raw


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


def test_compute_status_degraded_verification_is_degraded():
    journal = _deploy_journal.start_run("dev", targets={})
    _deploy_journal.add_step(
        journal,
        "verify-readiness",
        "Verify live readiness",
        required=True,
        evidence_path="deploy/.generated/dev/readiness-report.json",
    )
    _deploy_journal.mark_degraded(
        journal,
        "verify-readiness",
        reason="optional evidence stale",
    )

    assert _deploy_journal.compute_status(journal) == "DEGRADED"
    assert journal.steps[0].evidence_path.endswith("readiness-report.json")


def test_mark_required_promotes_optional_step_to_required():
    journal = _deploy_journal.start_run("dev", targets={})
    _deploy_journal.add_step(journal, "pipeline", "Trigger the setup pipeline", required=False)
    _deploy_journal.mark_required(journal, "pipeline")
    _deploy_journal.mark_failed(journal, "pipeline", exit_code=1)

    # Promoted to required, so a failure now fails the whole run.
    assert _deploy_journal.compute_status(journal) == "FAILED"


# --- Task-flow and setup-pipeline: required-once-requested, skip behavior -----


def test_initial_full_deploy_defers_taskflow_until_post_ontology(
    monkeypatch,
    tmp_path,
):
    _seed_deploy_config(tmp_path, profile="full-demo")
    (tmp_path / "fabric" / "taskflow").mkdir(parents=True)
    (tmp_path / "fabric" / "taskflow" / "taskflow.json").write_text("{}")
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)

    commands = []

    def fake_run(cmd, cwd=None):
        commands.append(cmd)
        if "deploy.scripts.taskflow" in cmd:
            return SimpleNamespace(returncode=3)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    result = runner.invoke(
        app, ["deploy", "--repo-root", str(tmp_path), "--env", "dev", "--yes", "--skip-terraform"]
    )

    assert result.exit_code == 0, result.output
    assert "Deploy complete" in result.output
    assert not any("deploy.scripts.taskflow" in command for command in commands)
    readiness = next(
        command
        for command in commands
        if "deploy.scripts.verify_readiness" in command
    )
    assert "--defer-post-ontology" in readiness
    journal = json.loads(_deploy_journal.journal_path(tmp_path, "dev").read_text())
    assert journal["status"] == "SUCCEEDED"
    assert not any(
        step["step_id"] == "task-flow-deploy" for step in journal["steps"]
    )


def test_setup_pipeline_failure_is_required_and_raises_nonzero(
    monkeypatch, tmp_path
):
    _seed_deploy_config(tmp_path, profile="full-demo")
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    monkeypatch.setattr(cli.time, "sleep", lambda *_a: None)

    def fake_run(cmd, cwd=None):
        if "deploy.scripts.run_pipeline" in cmd:
            return SimpleNamespace(returncode=5)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    result = runner.invoke(
        app,
        [
            "deploy",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--skip-terraform",
        ],
    )

    assert result.exit_code == 5, result.output
    assert "Deploy complete" not in result.output
    journal = json.loads(_deploy_journal.journal_path(tmp_path, "dev").read_text())
    assert journal["status"] == "FAILED"
    pipeline_step = next(
        s for s in journal["steps"] if s["step_id"] == "setup-pipeline-gate"
    )
    assert pipeline_step["classification"] == "required"
    assert pipeline_step["status"] == "FAILED"
    assert "Reporting was not published" in pipeline_step["error"]


def test_required_ml_failure_leaves_reporting_unpublished(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path, profile="full-demo")
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    commands = []

    def fake_run(cmd, cwd=None):
        commands.append(cmd)
        if (
            "deploy.scripts.run_pipeline" in cmd
            and "--pipeline" in cmd
            and cmd[cmd.index("--pipeline") + 1] == "ml-required"
        ):
            return SimpleNamespace(returncode=5)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)

    result = runner.invoke(
        app,
        ["deploy", "--repo-root", str(tmp_path), "--env", "dev", "--skip-terraform"],
    )

    assert result.exit_code == 5, result.output
    assert "Deploy complete" not in result.output
    journal = json.loads(_deploy_journal.journal_path(tmp_path, "dev").read_text())
    assert journal["status"] == "FAILED"
    gate = next(
        s
        for s in journal["steps"]
        if s["step_id"] == "required-ml-reporting-gate"
    )
    assert gate["status"] == "FAILED"
    assert "Reporting was not published" in gate["error"]
    reporting = next(
        s for s in journal["steps"] if s["step_id"] == "build-reporting"
    )
    assert reporting["status"] == "PENDING"
    publish_reporting = next(
        s for s in journal["steps"] if s["step_id"] == "deploy-reporting"
    )
    assert publish_reporting["status"] == "PENDING"
    assert not any(
        "deploy.scripts.build_artifacts" in command
        and "--publication-phase" in command
        and command[command.index("--publication-phase") + 1] == "reporting"
        for command in commands
    )
    assert sum(
        "deploy.scripts.deploy_items" in command for command in commands
    ) == 1


def test_yes_flag_runs_required_gates_without_prompt(monkeypatch, tmp_path):
    _seed_deploy_config(tmp_path, profile="full-demo")
    monkeypatch.setattr("retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None)
    monkeypatch.setattr("subprocess.run", _always_ok)

    result = runner.invoke(
        app, ["deploy", "--repo-root", str(tmp_path), "--env", "dev", "--yes", "--skip-terraform"]
    )

    assert result.exit_code == 0, result.output
    assert "Deploy complete" in result.output
    journal = json.loads(_deploy_journal.journal_path(tmp_path, "dev").read_text())
    setup_gate = next(
        s for s in journal["steps"] if s["step_id"] == "setup-pipeline-gate"
    )
    required_gate = next(
        s
        for s in journal["steps"]
        if s["step_id"] == "required-ml-reporting-gate"
    )
    assert setup_gate["status"] == "SUCCEEDED"
    assert required_gate["status"] == "SUCCEEDED"
    assert journal["status"] == "SUCCEEDED"
    profile = _profile("full-demo")
    assert journal["targets"]["profile"] == "full-demo"
    assert journal["targets"]["asset_ids"] == ",".join(profile.asset_ids)
    assert journal["targets"]["pipeline_refs"] == ",".join(profile.pipeline_refs)
    assert journal["targets"]["kql_scripts"] == ",".join(profile.kql_scripts)
    assert journal["targets"]["acknowledgements"] == ""
    assert journal["manifest"]["version"] == profile.manifest_version
    assert journal["manifest"]["hash"] == profile.manifest_hash
    assert journal["manifest"]["profile_support_status"] == "preview"
    assert journal["manifest"]["expected_item_counts"] == {
        "infrastructure": 40,
        "reporting": 2,
        "all": 42,
    }
    assert journal["manifest"]["asset_boundaries"]["preview"] == list(
        profile.preview_asset_ids
    )
    assert journal["manifest"]["asset_boundaries"]["manual"] == list(
        profile.manual_asset_ids
    )
    build = next(
        step for step in journal["steps"] if step["step_id"] == "build-infrastructure"
    )
    assert build["evidence_path"].endswith("artifact-inventory-infrastructure.json")
    readiness = next(
        step for step in journal["steps"] if step["step_id"] == "verify-readiness"
    )
    assert readiness["classification"] == "required"
    assert readiness["status"] == "SUCCEEDED"
    assert readiness["evidence_path"] == (
        "deploy/.generated/dev/readiness-report.json"
    )
    assert TENANT_ID not in json.dumps(journal)


def test_optional_ml_failure_does_not_block_published_reporting(
    monkeypatch, tmp_path
):
    _seed_deploy_config(tmp_path, profile="full-demo")
    monkeypatch.setattr(
        "retail_setup.cli.main._validate_azure_cli_tenant", lambda *_: None
    )

    def fake_run(cmd, cwd=None):
        if (
            "deploy.scripts.run_pipeline" in cmd
            and "--pipeline" in cmd
            and cmd[cmd.index("--pipeline") + 1] == "ml-optional"
        ):
            return SimpleNamespace(returncode=7)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)

    result = runner.invoke(
        app,
        [
            "deploy",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--yes",
            "--skip-terraform",
        ],
    )

    assert result.exit_code == 0, result.output
    journal = json.loads(_deploy_journal.journal_path(tmp_path, "dev").read_text())
    reporting = next(
        s for s in journal["steps"] if s["step_id"] == "deploy-reporting"
    )
    optional = next(
        s
        for s in journal["steps"]
        if s["step_id"] == "post-reporting-ml-optional"
    )
    experimental = next(
        s
        for s in journal["steps"]
        if s["step_id"] == "post-reporting-ml-experimental"
    )
    assert reporting["status"] == "SUCCEEDED"
    assert optional["status"] == "FAILED"
    assert optional["classification"] == "optional"
    assert experimental["status"] == "SUCCEEDED"
    assert journal["status"] == "DEGRADED"


def test_no_premature_complete_banner_on_required_step_failure(monkeypatch, tmp_path):
    """The 'Deploy complete' banner never prints before required work finishes."""
    _seed_deploy_config(tmp_path, profile="full-demo")
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
    build_step = next(
        s for s in journal["steps"] if s["step_id"] == "build-infrastructure"
    )
    assert build_step["status"] == "FAILED"
    # Steps after the failed one were never reached (still PENDING).
    kql_step = next(s for s in journal["steps"] if s["step_id"] == "apply-kql")
    assert kql_step["status"] == "PENDING"
