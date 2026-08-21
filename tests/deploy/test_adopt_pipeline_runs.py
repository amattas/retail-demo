"""Tests for exact Fabric pipeline-run recovery adoption."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from deploy.scripts.adopt_pipeline_runs import (
    PipelineAdoption,
    PipelineAdoptionError,
    adopt_pipeline_runs,
    build_recovery_journal,
    expected_pipeline_steps,
    finalize_recovery_journal,
    parse_run_specs,
)

WORKSPACE_ID = "11111111-1111-4111-8111-111111111111"
PIPELINE_ID = "22222222-2222-4222-8222-222222222222"
RUN_ID = "33333333-3333-4333-8333-333333333333"


def _profile() -> SimpleNamespace:
    return SimpleNamespace(
        post_deploy_pipeline_ref="setup-pipeline.DataPipeline",
        reporting_gate_pipeline_ref="ml-required.DataPipeline",
        post_reporting_pipeline_refs=(
            "ml-optional.DataPipeline",
            "ml-experimental.DataPipeline",
        ),
        manifest_version="1.4.0",
        manifest_hash="manifest-hash",
        id="profile.full-demo",
        deployment_name="full-demo",
        support_status="preview",
    )


def test_expected_pipeline_steps_covers_every_deployment_gate() -> None:
    assert expected_pipeline_steps(_profile()) == {
        "setup-pipeline": ("setup-pipeline-gate", True),
        "ml-required": ("required-ml-reporting-gate", True),
        "ml-optional": ("post-reporting-ml-optional", False),
        "ml-experimental": ("post-reporting-ml-experimental", False),
    }


def test_parse_run_specs_requires_complete_exact_inventory() -> None:
    expected = {"setup-pipeline", "ml-required"}

    assert parse_run_specs(
        [
            f"setup-pipeline={RUN_ID}",
            "ml-required=44444444-4444-4444-8444-444444444444",
        ],
        expected,
    )["setup-pipeline"] == RUN_ID
    with pytest.raises(PipelineAdoptionError, match="missing"):
        parse_run_specs([f"setup-pipeline={RUN_ID}"], expected)
    with pytest.raises(PipelineAdoptionError, match="more than once"):
        parse_run_specs(
            [f"setup-pipeline={RUN_ID}", f"setup-pipeline={RUN_ID}"],
            {"setup-pipeline"},
        )


def test_adopt_pipeline_runs_requires_exact_recent_terminal_success(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "deploy.scripts.adopt_pipeline_runs.list_items",
        lambda *_args: [
            {
                "displayName": "setup-pipeline",
                "id": PIPELINE_ID,
            }
        ],
    )
    monkeypatch.setattr(
        "deploy.scripts.adopt_pipeline_runs.list_pipeline_runs",
        lambda *_args: [
            {
                "id": RUN_ID,
                "itemId": PIPELINE_ID,
                "jobType": "Pipeline",
                "status": "Completed",
                "startTimeUtc": "2026-07-28T01:00:00Z",
                "endTimeUtc": "2026-07-28T01:05:00Z",
            }
        ],
    )

    adopted = adopt_pipeline_runs(
        SimpleNamespace(),
        WORKSPACE_ID,
        {"setup-pipeline": ("setup-pipeline-gate", True)},
        {"setup-pipeline": RUN_ID},
        observed_at=datetime(2026, 7, 28, 2, tzinfo=UTC),
    )

    assert adopted == [
        PipelineAdoption(
            name="setup-pipeline",
            step_id="setup-pipeline-gate",
            required=True,
            pipeline_id=PIPELINE_ID,
            run_id=RUN_ID,
            started_at="2026-07-28T01:00:00+00:00",
            ended_at="2026-07-28T01:05:00+00:00",
        )
    ]


def test_adopt_pipeline_runs_rejects_failed_exact_run(monkeypatch) -> None:
    monkeypatch.setattr(
        "deploy.scripts.adopt_pipeline_runs.list_items",
        lambda *_args: [
            {"displayName": "setup-pipeline", "id": PIPELINE_ID}
        ],
    )
    monkeypatch.setattr(
        "deploy.scripts.adopt_pipeline_runs.list_pipeline_runs",
        lambda *_args: [
            {
                "id": RUN_ID,
                "itemId": PIPELINE_ID,
                "status": "Failed",
                "startTimeUtc": "2026-07-28T01:00:00Z",
                "endTimeUtc": "2026-07-28T01:05:00Z",
            }
        ],
    )

    with pytest.raises(PipelineAdoptionError, match="not terminal-successful"):
        adopt_pipeline_runs(
            SimpleNamespace(),
            WORKSPACE_ID,
            {"setup-pipeline": ("setup-pipeline-gate", True)},
            {"setup-pipeline": RUN_ID},
            observed_at=datetime(2026, 7, 28, 2, tzinfo=UTC),
        )


def test_adopt_pipeline_runs_rejects_dependency_inversion(monkeypatch) -> None:
    pipeline_ids = {
        "setup-pipeline": PIPELINE_ID,
        "ml-required": "44444444-4444-4444-8444-444444444444",
    }
    run_ids = {
        "setup-pipeline": RUN_ID,
        "ml-required": "55555555-5555-4555-8555-555555555555",
    }
    monkeypatch.setattr(
        "deploy.scripts.adopt_pipeline_runs.list_items",
        lambda *_args: [
            {"displayName": name, "id": pipeline_id}
            for name, pipeline_id in pipeline_ids.items()
        ],
    )

    def runs_for_pipeline(_session, _workspace_id, pipeline_id):
        name = next(
            name for name, item_id in pipeline_ids.items()
            if item_id == pipeline_id
        )
        is_setup = name == "setup-pipeline"
        return [
            {
                "id": run_ids[name],
                "itemId": pipeline_id,
                "status": "Completed",
                "startTimeUtc": (
                    "2026-07-28T01:00:00Z"
                    if is_setup
                    else "2026-07-28T01:05:00Z"
                ),
                "endTimeUtc": (
                    "2026-07-28T01:10:00Z"
                    if is_setup
                    else "2026-07-28T01:09:00Z"
                ),
            }
        ]

    monkeypatch.setattr(
        "deploy.scripts.adopt_pipeline_runs.list_pipeline_runs",
        runs_for_pipeline,
    )

    with pytest.raises(PipelineAdoptionError, match="before.*setup"):
        adopt_pipeline_runs(
            SimpleNamespace(),
            WORKSPACE_ID,
            {
                "setup-pipeline": ("setup-pipeline-gate", True),
                "ml-required": ("required-ml-reporting-gate", True),
            },
            run_ids,
            observed_at=datetime(2026, 7, 28, 2, tzinfo=UTC),
        )


def test_recovery_journal_persists_current_manifest_and_exact_run() -> None:
    config = SimpleNamespace(
        environment="retail-demo",
        auth_mode="azure_cli",
        workspace=SimpleNamespace(name="retail-demo"),
        lakehouse=SimpleNamespace(name="retail_lakehouse"),
        profile=_profile(),
    )
    adoption = PipelineAdoption(
        name="setup-pipeline",
        step_id="setup-pipeline-gate",
        required=True,
        pipeline_id=PIPELINE_ID,
        run_id=RUN_ID,
        started_at="2026-07-28T01:00:00+00:00",
        ended_at="2026-07-28T01:05:00+00:00",
    )

    journal = build_recovery_journal(
        config,
        [adoption],
        recovered_from_run_id="44444444-4444-4444-8444-444444444444",
    )

    assert journal.manifest["hash"] == "manifest-hash"
    assert journal.manifest["recovered_from_run_id"]
    assert journal.steps[0].evidence_id == RUN_ID
    assert journal.status == "RUNNING"
    assert journal.steps[1].step_id == "verify-readiness"
    assert journal.steps[1].status == "PENDING"

    finalize_recovery_journal(journal, "DEGRADED")
    assert journal.steps[1].status == "DEGRADED"
