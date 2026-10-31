"""Resume identity and destructive Terraform plan guards."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from retail_setup.cli import _deploy_journal as journal
from retail_setup.cli import _deploy_resume
from retail_setup.cli import main as cli


def _run(statuses):
    run = journal.start_run(
        "dev",
        targets={"workspace_name": "dev"},
        manifest={
            "hash": "manifest",
            "profile_id": "core",
            "profile_name": "core",
            "plan_step_ids": list(statuses),
            "plan_graph": [
                {
                    "step_id": step_id,
                    "depends_on": [],
                    "resource": "non-spark",
                    "required": True,
                }
                for step_id in statuses
            ],
            "input_fingerprint": "inputs",
            "input_fingerprints": {
                "commands_sha256": "commands",
                "files": {"deploy/scripts/run_pipeline.py": "source"},
            },
        },
    )
    for name, status in statuses.items():
        journal.add_step(run, name, name, required=True)
        journal.get_step(run, name).status = status
    return run


@pytest.mark.parametrize("field", ["hash", "profile_id", "plan_step_ids", "input_fingerprint"])
def test_resume_rejects_changed_or_legacy_identity(field):
    previous = _run({"a": "SUCCEEDED", "b": "FAILED"})
    current = _run({"a": "PENDING", "b": "PENDING"})
    previous.manifest.pop(field)
    with pytest.raises(ValueError, match="plan changed"):
        cli._resume_succeeded_prefix(previous, current)


@pytest.mark.parametrize(
    ("producer", "consumer"),
    [
        ("terraform-plan-guard", "terraform-apply"),
        ("build-reporting", "deploy-reporting"),
        ("build-post-ontology", "deploy-post-ontology"),
    ],
)
def test_resume_regenerates_ephemeral_inputs_without_repeating_setup(producer, consumer):
    previous = _run({"setup": "SUCCEEDED", producer: "SUCCEEDED", consumer: "FAILED"})
    current = _run({"setup": "PENDING", producer: "PENDING", consumer: "PENDING"})

    assert cli._resume_succeeded_prefix(previous, current) == 1
    assert [step.status for step in current.steps] == ["SUCCEEDED", "PENDING", "PENDING"]


def test_resume_preserves_completed_recreate_without_repeating_destruction():
    previous = _run(
        {
            "terraform-init": "SUCCEEDED",
            "terraform-destroy-recreate": "SUCCEEDED",
            "deletion-wait": "SUCCEEDED",
            "setup": "SUCCEEDED",
            "reporting": "FAILED",
        }
    )
    previous.targets["recreate"] = "True"
    assert _deploy_resume.recreate_mode(previous) is True
    journal.get_step(previous, "terraform-destroy-recreate").status = "RUNNING"
    with pytest.raises(ValueError, match="unfinished destructive recreate"):
        _deploy_resume.recreate_mode(previous)


def test_resume_recreate_allows_failure_after_destroy_and_deletion_wait():
    previous = _run(
        {
            "terraform-destroy-recreate": "SUCCEEDED",
            "deletion-wait": "SUCCEEDED",
            "setup": "SUCCEEDED",
            "required-ml-reporting-gate": "FAILED",
        }
    )
    previous.targets["recreate"] = "True"

    assert _deploy_resume.recreate_mode(previous) is True


def test_resume_recreate_rejects_unfinished_destroy_or_deletion_wait():
    previous = _run(
        {
            "terraform-destroy-recreate": "SUCCEEDED",
            "deletion-wait": "FAILED",
            "setup": "PENDING",
        }
    )
    previous.targets["recreate"] = "True"

    with pytest.raises(ValueError, match="unfinished destructive recreate"):
        _deploy_resume.recreate_mode(previous)


def test_resume_requires_explicit_adoption_for_fingerprint_change():
    previous = _run({"setup": "SUCCEEDED", "required-ml-reporting-gate": "FAILED"})
    current = _run({"setup": "PENDING", "required-ml-reporting-gate": "PENDING"})
    previous.manifest["input_fingerprint"] = "old-inputs"
    current.manifest["input_fingerprint"] = "new-inputs"
    previous.manifest["input_fingerprints"]["files"]["deploy/scripts/run_pipeline.py"] = "old"
    current.manifest["input_fingerprints"]["files"]["deploy/scripts/run_pipeline.py"] = "new"

    with pytest.raises(ValueError, match="input_fingerprint"):
        cli._resume_succeeded_prefix(previous, current)

    assert cli._resume_succeeded_prefix(previous, current, allow_runner_file_change=True) == 1


def test_adopted_run_rejects_drift_in_any_other_source_file():
    previous = _run({"setup": "SUCCEEDED", "required-ml-reporting-gate": "FAILED"})
    current = _run({"setup": "PENDING", "required-ml-reporting-gate": "PENDING"})
    previous.manifest["input_fingerprint"] = "old-inputs"
    current.manifest["input_fingerprint"] = "new-inputs"
    previous.manifest["input_fingerprints"]["files"].update(
        {
            "deploy/scripts/run_pipeline.py": "old-runner",
            "fabric/lakehouse/06-ml-demand-forecast.ipynb": "same-notebook",
        }
    )
    current.manifest["input_fingerprints"]["files"].update(
        {
            "deploy/scripts/run_pipeline.py": "new-runner",
            "fabric/lakehouse/06-ml-demand-forecast.ipynb": "changed-notebook",
        }
    )

    with pytest.raises(ValueError, match="fabric/lakehouse/06-ml-demand-forecast.ipynb"):
        cli._resume_succeeded_prefix(previous, current, allow_runner_file_change=True)


def test_reporting_packaging_repair_allows_only_build_artifacts_change():
    previous = _run(
        {
            "required-ml-reporting-gate": "SUCCEEDED",
            "build-reporting": "SUCCEEDED",
            "deploy-reporting": "FAILED",
        }
    )
    current = _run(
        {
            "required-ml-reporting-gate": "PENDING",
            "build-reporting": "PENDING",
            "deploy-reporting": "PENDING",
        }
    )
    previous.manifest["input_fingerprint"] = "old-inputs"
    current.manifest["input_fingerprint"] = "new-inputs"
    previous.manifest["input_fingerprints"]["files"]["deploy/scripts/build_artifacts.py"] = "old"
    current.manifest["input_fingerprints"]["files"]["deploy/scripts/build_artifacts.py"] = "new"

    assert (
        cli._resume_succeeded_prefix(
            previous,
            current,
            allowed_source_changes={"deploy/scripts/build_artifacts.py"},
        )
        == 1
    )
    assert [step.status for step in current.steps] == [
        "SUCCEEDED",
        "PENDING",
        "PENDING",
    ]


def test_reporting_packaging_repair_rejects_additional_source_drift():
    previous = _run({"build-reporting": "SUCCEEDED", "deploy-reporting": "FAILED"})
    current = _run({"build-reporting": "PENDING", "deploy-reporting": "PENDING"})
    previous.manifest["input_fingerprint"] = "old-inputs"
    current.manifest["input_fingerprint"] = "new-inputs"
    previous.manifest["input_fingerprints"]["files"].update(
        {
            "deploy/scripts/build_artifacts.py": "old-build",
            "fabric/powerbi/retail_model.Report/definition.pbir": "old-report",
        }
    )
    current.manifest["input_fingerprints"]["files"].update(
        {
            "deploy/scripts/build_artifacts.py": "new-build",
            "fabric/powerbi/retail_model.Report/definition.pbir": "new-report",
        }
    )

    with pytest.raises(ValueError, match="definition.pbir"):
        cli._resume_succeeded_prefix(
            previous,
            current,
            allowed_source_changes={"deploy/scripts/build_artifacts.py"},
        )


@pytest.mark.parametrize("already_marked_successful", [False, True])
def test_adopt_completed_required_pipeline_run_verifies_exact_run(
    monkeypatch, tmp_path, already_marked_successful
):
    previous = journal.start_run(
        "retail-demo",
        targets={"workspace_name": "retail-demo", "recreate": "True"},
        manifest={"input_fingerprint": "old-inputs"},
    )
    journal.add_step(previous, "setup-pipeline-gate", "setup", required=True)
    journal.mark_succeeded(
        previous,
        "setup-pipeline-gate",
        started_at="2026-10-12T18:36:53+00:00",
        ended_at="2026-10-12T20:38:07+00:00",
    )
    journal.add_step(
        previous,
        "required-ml-reporting-gate",
        "required ML",
        required=True,
    )
    journal.mark_failed(previous, "required-ml-reporting-gate", exit_code=1)
    if already_marked_successful:
        journal.mark_succeeded(
            previous,
            "required-ml-reporting-gate",
            started_at="2026-10-12T20:38:19+00:00",
            ended_at="2026-10-12T21:27:45+00:00",
            evidence_id="33333333-3333-4333-8333-333333333333",
        )
    journal.add_step(previous, "build-reporting", "reporting", required=True)

    adopted = SimpleNamespace(
        name="ml-required",
        run_id="33333333-3333-4333-8333-333333333333",
        started_at="2026-10-12T20:38:19+00:00",
        ended_at="2026-10-12T21:27:45+00:00",
    )
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.load_terraform_outputs",
        lambda _path: {"workspace_id": "workspace-id"},
    )
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.validate_terraform_outputs",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        "deploy.scripts.export_items.build_session",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        "deploy.scripts.validate_target_access.validate_target_access",
        lambda *_args: SimpleNamespace(workspace_id="workspace-id"),
    )
    monkeypatch.setattr(
        "deploy.scripts.adopt_pipeline_runs.adopt_pipeline_runs",
        lambda *_args: [adopted],
    )
    config = SimpleNamespace(
        profile=SimpleNamespace(reporting_gate_pipeline_ref="ml-required.DataPipeline"),
        auth_mode="azure_cli",
        tenant_id="tenant-id",
    )

    result = cli._adopt_completed_required_pipeline(
        previous,
        f"ml-required={adopted.run_id}",
        repo_root=tmp_path,
        env="retail-demo",
        deploy_config=config,
    )

    assert result is adopted
    gate = journal.get_step(previous, "required-ml-reporting-gate")
    assert gate.status == "SUCCEEDED"
    assert gate.evidence_id == adopted.run_id
    assert gate.started_at == adopted.started_at
    assert gate.ended_at == adopted.ended_at


def test_resume_executes_only_failed_suffix(tmp_path, monkeypatch):
    previous = _run({"setup": "SUCCEEDED", "ml": "SUCCEEDED", "reporting": "FAILED"})
    current = _run({"setup": "PENDING", "ml": "PENDING", "reporting": "PENDING"})
    cli._resume_succeeded_prefix(previous, current)
    execute = Mock(return_value=SimpleNamespace(returncode=0))
    monkeypatch.setattr(cli.subprocess, "run", execute)
    plan = [cli.DeployStep(cmd=[step.step_id], step_id=step.step_id) for step in current.steps]

    cli._run_plan_plain(tmp_path, "dev", plan, len(plan), yes=True, journal=current)

    execute.assert_called_once_with(["reporting"], cwd=tmp_path)
    assert journal.read(tmp_path, "dev").status == "SUCCEEDED"


def test_resume_reattaches_running_pipeline_to_exact_location(
    tmp_path,
) -> None:
    previous = _run({"setup-pipeline-gate": "RUNNING"})
    current = _run({"setup-pipeline-gate": "PENDING"})
    evidence_path = "deploy/.generated/dev/pipeline-evidence/setup.json"
    location = (
        "https://api.fabric.microsoft.com/v1/workspaces/ws/items/pipeline-id/"
        "jobs/instances/11111111-1111-4111-8111-111111111111"
    )
    journal.get_step(previous, "setup-pipeline-gate").evidence_path = evidence_path
    journal.get_step(current, "setup-pipeline-gate").evidence_path = evidence_path
    path = tmp_path / evidence_path
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "workspace_id": "ws",
                "pipeline_name": "setup-pipeline",
                "pipeline_id": "pipeline-id",
                "run_id": "11111111-1111-4111-8111-111111111111",
                "location": location,
                "submitted_at": "2026-10-15T12:00:00+00:00",
                "status": "InProgress",
            }
        ),
        encoding="utf-8",
    )
    plan = [
        cli.DeployStep(
            cmd=[
                "python",
                "-m",
                "deploy.scripts.run_pipeline",
                "--pipeline",
                "setup-pipeline",
            ],
            step_id="setup-pipeline-gate",
            evidence_path=evidence_path,
        )
    ]

    reattached = cli._reattach_running_pipeline_steps(
        previous,
        current,
        plan,
        repo_root=tmp_path,
    )

    assert reattached == ["setup-pipeline-gate"]
    assert plan[0].cmd[-2:] == ["--reattach-location", location]
    assert (
        journal.get_step(current, "setup-pipeline-gate").evidence_id
        == "11111111-1111-4111-8111-111111111111"
    )


def test_resume_rejects_running_pipeline_with_incompatible_exact_evidence(
    tmp_path,
) -> None:
    previous = _run({"setup-pipeline-gate": "RUNNING"})
    current = _run({"setup-pipeline-gate": "PENDING"})
    evidence_path = "deploy/.generated/dev/pipeline-evidence/setup.json"
    journal.get_step(previous, "setup-pipeline-gate").evidence_path = evidence_path
    journal.get_step(current, "setup-pipeline-gate").evidence_path = evidence_path
    path = tmp_path / evidence_path
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "workspace_id": "ws",
                "pipeline_name": "different-pipeline",
                "pipeline_id": "pipeline-id",
                "run_id": "11111111-1111-4111-8111-111111111111",
                "location": "https://example.invalid/run",
                "submitted_at": "2026-10-15T12:00:00+00:00",
                "status": "InProgress",
            }
        ),
        encoding="utf-8",
    )
    plan = [
        cli.DeployStep(
            cmd=[
                "python",
                "-m",
                "deploy.scripts.run_pipeline",
                "--pipeline",
                "setup-pipeline",
            ],
            step_id="setup-pipeline-gate",
            evidence_path=evidence_path,
        )
    ]

    with pytest.raises(ValueError, match="different pipeline"):
        cli._reattach_running_pipeline_steps(
            previous,
            current,
            plan,
            repo_root=tmp_path,
        )


def test_monitor_timeout_keeps_pipeline_step_running_and_resumable(
    tmp_path,
    monkeypatch,
) -> None:
    evidence_path = "deploy/.generated/dev/pipeline-evidence/setup.json"
    path = tmp_path / evidence_path
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "workspace_id": "ws",
                "pipeline_name": "setup-pipeline",
                "pipeline_id": "pipeline-id",
                "run_id": "11111111-1111-4111-8111-111111111111",
                "location": "https://api.fabric.microsoft.com/exact-run",
                "submitted_at": "2026-10-15T12:00:00+00:00",
                "status": "InProgress",
            }
        ),
        encoding="utf-8",
    )
    current = _run({"setup-pipeline-gate": "PENDING"})
    journal.get_step(current, "setup-pipeline-gate").evidence_path = evidence_path
    plan = [
        cli.DeployStep(
            cmd=[
                "python",
                "-m",
                "deploy.scripts.run_pipeline",
                "--pipeline",
                "setup-pipeline",
            ],
            step_id="setup-pipeline-gate",
            evidence_path=evidence_path,
        )
    ]
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=4),
    )

    with pytest.raises(Exception) as exc_info:
        cli._run_plan_plain(
            tmp_path,
            "dev",
            plan,
            len(plan),
            yes=True,
            journal=current,
        )

    assert getattr(exc_info.value, "exit_code", None) == 4
    persisted = journal.read(tmp_path, "dev")
    step = journal.get_step(persisted, "setup-pipeline-gate")
    assert step.status == "RUNNING"
    assert step.evidence_id == "11111111-1111-4111-8111-111111111111"


def test_resumed_deploy_rechecks_capacity_without_rerunning_completed_setup(tmp_path, monkeypatch):
    previous = _run(
        {
            "live-profile-preflight": "SUCCEEDED",
            "setup": "SUCCEEDED",
            "reporting": "FAILED",
        }
    )
    current = _run(
        {
            "live-profile-preflight": "PENDING",
            "setup": "PENDING",
            "reporting": "PENDING",
        }
    )
    cli._resume_succeeded_prefix(previous, current)
    execute = Mock(return_value=SimpleNamespace(returncode=0))
    monkeypatch.setattr(cli.subprocess, "run", execute)
    plan = [cli.DeployStep(cmd=[step.step_id], step_id=step.step_id) for step in current.steps]

    cli._run_plan_plain(tmp_path, "dev", plan, len(plan), yes=True, journal=current)

    assert [call.args[0] for call in execute.call_args_list] == [
        ["live-profile-preflight"],
        ["reporting"],
    ]


def test_custom_pool_resume_regenerates_and_replans_before_unapplied_terraform():
    statuses = {
        "profile-preflight": "SUCCEEDED",
        "live-profile-preflight": "SUCCEEDED",
        "generate-configs": "SUCCEEDED",
        "terraform-init": "SUCCEEDED",
        "terraform-plan-guard": "SUCCEEDED",
        "terraform-apply": "FAILED",
    }
    previous = _run(statuses)
    current = _run({step_id: "PENDING" for step_id in statuses})
    previous.targets["spark_pool"] = current.targets["spark_pool"] = "custom"
    sizing = {"sha256": "same-sizing"}
    previous.manifest["capacity_sizing"] = sizing
    current.manifest["capacity_sizing"] = sizing

    assert cli._resume_succeeded_prefix(previous, current) == 1
    assert [step.status for step in current.steps] == [
        "SUCCEEDED",
        "PENDING",
        "PENDING",
        "PENDING",
        "PENDING",
        "PENDING",
    ]


def test_custom_pool_resume_rejects_changed_sizing_after_apply(
    tmp_path,
    monkeypatch,
):
    current = _run({"live-profile-preflight": "PENDING"})
    current.targets["spark_pool"] = "custom"
    current.manifest["capacity_sizing"] = {"sha256": "applied-sizing"}
    current.manifest["capacity_sizing_match_required"] = True
    monkeypatch.setattr(cli, "_load_deploy_environment", lambda *_args: object())
    monkeypatch.setattr(
        "deploy.scripts.capacity_sizing.load_capacity_sizing_evidence",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        "deploy.scripts.capacity_sizing.sizing_identity",
        lambda _evidence: {"sha256": "new-sizing"},
    )

    with pytest.raises(ValueError, match="new deployment"):
        cli._record_capacity_sizing_after_preflight(tmp_path, "dev", current)


def test_new_run_preserves_previous_journal_evidence(tmp_path):
    previous = _run({"setup": "FAILED"})
    journal.write(tmp_path, previous)
    current = _run({"setup": "PENDING"})

    journal.write(tmp_path, current)

    archives = list((journal.journal_path(tmp_path, "dev").parent / "run-history").glob("*.json"))
    assert len(archives) == 1
    assert json.loads(archives[0].read_text(encoding="utf-8"))["run_id"] == previous.run_id
    assert journal.read(tmp_path, "dev").run_id == current.run_id


def test_input_fingerprint_changes_with_config_source_or_plan(tmp_path):
    generation = tmp_path / "generation.yaml"
    generation.write_text("seed: 1", encoding="utf-8")
    source = tmp_path / "fabric" / "asset.json"
    source.parent.mkdir()
    source.write_text("{}", encoding="utf-8")
    commands = [["render", "--env", "dev"]]
    first = _deploy_resume.fingerprint(tmp_path, "dev", generation, commands)
    assert first == _deploy_resume.fingerprint(tmp_path, "dev", generation, commands)
    generation.write_text("seed: 2", encoding="utf-8")
    second = _deploy_resume.fingerprint(tmp_path, "dev", generation, commands)
    assert first != second
    source.write_text('{"changed":true}', encoding="utf-8")
    third = _deploy_resume.fingerprint(tmp_path, "dev", generation, commands)
    assert third != second
    assert third != _deploy_resume.fingerprint(tmp_path, "dev", generation, [["new"]])


def test_fingerprint_details_identify_changed_source_files(tmp_path):
    generation = tmp_path / "generation.yaml"
    generation.write_text("seed: 1", encoding="utf-8")
    runner = tmp_path / "deploy" / "scripts" / "run_pipeline.py"
    runner.parent.mkdir(parents=True)
    runner.write_text("before", encoding="utf-8")
    commands = [["run"]]
    before = _deploy_resume.fingerprint_details(tmp_path, "dev", generation, commands)

    runner.write_text("after", encoding="utf-8")
    after = _deploy_resume.fingerprint_details(tmp_path, "dev", generation, commands)

    assert _deploy_resume.changed_fingerprint_inputs(before, after) == {
        "deploy/scripts/run_pipeline.py"
    }


@pytest.mark.parametrize(
    "resource_type", ["fabric_workspace", "fabric_lakehouse", "fabric_eventhouse"]
)
@pytest.mark.parametrize("actions", [["delete"], ["delete", "create"], ["create", "delete"]])
def test_plan_guard_detects_all_destructive_data_resource_actions(resource_type, actions):
    change = {
        "address": f"module.demo.{resource_type}.item[0]",
        "type": resource_type,
        "change": {"actions": actions},
    }
    assert _deploy_resume.validate_plan_changes(
        {"format_version": "1.2", "resource_changes": [change]}
    ) == [change["address"]]


@pytest.mark.parametrize("payload", [{}, [], {"format_version": "1.2", "resource_changes": [{}]}])
def test_plan_guard_rejects_invalid_plan_evidence(payload):
    with pytest.raises(ValueError):
        _deploy_resume.validate_plan_changes(payload)


def test_plan_guard_accepts_exit_two_and_checks_saved_plan(tmp_path, monkeypatch):
    execute = Mock(
        side_effect=[
            SimpleNamespace(returncode=2, stdout="", stderr=""),
            SimpleNamespace(returncode=0, stdout=json.dumps({"format_version": "1.2"}), stderr=""),
        ]
    )
    monkeypatch.setattr(cli.subprocess, "run", execute)

    cli._terraform_plan_guard(tmp_path, "dev")

    assert len(execute.call_args_list) == 2
    assert "-out=../.generated/dev/terraform.tfplan" in execute.call_args_list[0].args[0]
    assert execute.call_args_list[1].args[0][-1] == "../.generated/dev/terraform.tfplan"
    assert Path(execute.call_args.kwargs["env"]["TF_DATA_DIR"]).is_absolute()
