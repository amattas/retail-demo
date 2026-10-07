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
            "input_fingerprint": "inputs",
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
    previous = _run({
        "terraform-init": "SUCCEEDED", "terraform-destroy-recreate": "SUCCEEDED",
        "deletion-wait": "SUCCEEDED", "setup": "SUCCEEDED", "reporting": "FAILED",
    })
    previous.targets["recreate"] = "True"
    assert _deploy_resume.recreate_mode(previous) is True
    journal.get_step(previous, "terraform-destroy-recreate").status = "RUNNING"
    with pytest.raises(ValueError, match="unfinished destructive recreate"):
        _deploy_resume.recreate_mode(previous)


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


def test_resumed_deploy_rechecks_capacity_without_rerunning_completed_setup(tmp_path, monkeypatch):
    previous = _run({
        "live-profile-preflight": "SUCCEEDED",
        "setup": "SUCCEEDED",
        "reporting": "FAILED",
    })
    current = _run({
        "live-profile-preflight": "PENDING",
        "setup": "PENDING",
        "reporting": "PENDING",
    })
    cli._resume_succeeded_prefix(previous, current)
    execute = Mock(return_value=SimpleNamespace(returncode=0))
    monkeypatch.setattr(cli.subprocess, "run", execute)
    plan = [cli.DeployStep(cmd=[step.step_id], step_id=step.step_id) for step in current.steps]

    cli._run_plan_plain(tmp_path, "dev", plan, len(plan), yes=True, journal=current)

    assert [call.args[0] for call in execute.call_args_list] == [
        ["live-profile-preflight"], ["reporting"],
    ]


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
