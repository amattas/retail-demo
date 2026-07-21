"""Executable deployment-profile contract tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from retail_setup.contracts import (
    ProfileResolutionError,
    SolutionManifest,
    load_repository_manifest,
    resolve_profile,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = REPO_ROOT / "contracts" / "retail-demo.json"


def _profiles():
    manifest, validation = load_repository_manifest(REPO_ROOT)
    return {
        name: resolve_profile(manifest, validation, name)
        for name in ("core", "standard", "full-demo")
    }


def test_profiles_resolve_to_exact_dependency_closed_inventories() -> None:
    profiles = _profiles()

    assert profiles["core"].asset_ids == ("asset.lakehouse",)
    assert profiles["core"].notebook_groups == ("setup",)
    assert profiles["core"].pipeline_refs == ()
    assert profiles["core"].kql_scripts == ()
    assert profiles["core"].item_types_in_scope == ("Lakehouse", "Notebook")

    assert profiles["standard"].asset_ids == (
        "asset.lakehouse",
        "asset.eventhouse",
        "asset.stream-events",
        "asset.semantic-model",
        "asset.report",
        "asset.data-pipelines",
        "asset.kql-queryset",
        "asset.ml-notebooks",
    )
    assert profiles["standard"].notebook_groups == (
        "setup",
        "core",
        "stream",
        "ml-required",
    )
    assert profiles["standard"].pipeline_refs == (
        "daily-maintenance.DataPipeline",
        "historical-data-load.DataPipeline",
        "ml-required.DataPipeline",
        "setup-pipeline.DataPipeline",
        "streaming-data-load.DataPipeline",
    )
    assert (
        profiles["standard"].reporting_gate_pipeline_ref
        == "ml-required.DataPipeline"
    )
    assert profiles["standard"].post_reporting_pipeline_refs == ()
    assert profiles["standard"].kql_scripts == (
        "01-create-tables.kql",
        "02-create-ingestion-mappings.kql",
        "03-create-functions.kql",
        "04-create-materialized-views.kql",
        "06-ml-anomaly-detection.kql",
        "07-pricing-approval-tables.kql",
    )
    assert "DataAgent" not in profiles["standard"].item_types_in_scope

    assert profiles["full-demo"].asset_ids == (
        "asset.lakehouse",
        "asset.eventhouse",
        "asset.stream-events",
        "asset.semantic-model",
        "asset.report",
        "asset.data-pipelines",
        "asset.kql-queryset",
        "asset.dashboard-templates",
        "asset.activator-rules",
        "asset.task-flow",
        "asset.ml-notebooks",
        "asset.ontology",
        "asset.data-agents",
        "asset.custom-spark-pool",
    )
    assert profiles["full-demo"].notebook_groups == (
        "setup",
        "core",
        "stream",
        "ml-required",
        "ml-optional",
        "ml-experimental",
        "ontology",
        "utility",
    )
    assert profiles["full-demo"].pipeline_refs == (
        "daily-maintenance.DataPipeline",
        "historical-data-load.DataPipeline",
        "ml-experimental.DataPipeline",
        "ml-optional.DataPipeline",
        "ml-required.DataPipeline",
        "setup-pipeline.DataPipeline",
        "streaming-data-load.DataPipeline",
    )
    assert profiles["full-demo"].kql_scripts == profiles["standard"].kql_scripts
    assert profiles["full-demo"].post_deploy_pipeline_ref == (
        "setup-pipeline.DataPipeline"
    )
    assert profiles["full-demo"].post_reporting_pipeline_refs == (
        "ml-optional.DataPipeline",
        "ml-experimental.DataPipeline",
    )
    assert profiles["full-demo"].uses_custom_pool
    assert profiles["full-demo"].deploys_task_flow


def test_resolver_rejects_unknown_profile() -> None:
    manifest, validation = load_repository_manifest(REPO_ROOT)

    with pytest.raises(ProfileResolutionError, match="expected one of"):
        resolve_profile(manifest, validation, "unknown")


def test_no_automatic_profile_selects_destructive_reset() -> None:
    for profile in _profiles().values():
        assert "reset" not in profile.notebook_groups
        assert all("reset" not in asset_id for asset_id in profile.asset_ids)


def test_standard_and_full_profiles_publish_no_enabled_automatic_schedule() -> None:
    for name in ("standard", "full-demo"):
        profile = _profiles()[name]
        for pipeline_ref in profile.pipeline_refs:
            schedule_path = (
                REPO_ROOT
                / "fabric"
                / "pipelines"
                / pipeline_ref
                / ".schedules"
            )
            if not schedule_path.is_file():
                continue
            schedules = json.loads(
                schedule_path.read_text(encoding="utf-8")
            )["schedules"]
            assert not any(
                schedule.get("enabled") is True for schedule in schedules
            ), pipeline_ref


def test_resolver_adds_transitive_asset_dependencies() -> None:
    document = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    core = document["profiles"][0]
    core["asset_refs"] = ["asset.report"]
    core["group_refs"] = ["setup", "ml-required"]
    manifest = SolutionManifest.model_validate(document)
    _, validation = load_repository_manifest(REPO_ROOT)

    resolved = resolve_profile(manifest, validation, "core")

    assert resolved.asset_ids == (
        "asset.lakehouse",
        "asset.semantic-model",
        "asset.report",
        "asset.ml-notebooks",
    )
    direct = {asset.id for asset in resolved.assets if asset.direct}
    assert direct == {"asset.report"}


def test_imp008_profile_blockers_are_replaced_by_executable_reporting_gate() -> None:
    profiles = _profiles()

    assert all(profile.blockers == () for profile in profiles.values())
    assert profiles["core"].reporting_gate_pipeline_ref is None
    for name in ("standard", "full-demo"):
        assert (
            profiles[name].reporting_gate_pipeline_ref
            == "ml-required.DataPipeline"
        )


def test_full_demo_declares_undetectable_operator_boundaries() -> None:
    full_demo = _profiles()["full-demo"]

    assert {
        acknowledgement.id
        for acknowledgement in full_demo.required_acknowledgements
    } == {
        "ack.full-demo.preview-surfaces",
        "ack.full-demo.custom-pool-capacity",
        "ack.full-demo.task-flow-api",
        "ack.full-demo.manual-assets",
    }
    assert {ack.kind for ack in full_demo.required_acknowledgements} == {
        "preview",
        "capacity",
        "manual",
    }
    assert set(full_demo.manual_asset_ids) == {
        "asset.dashboard-templates",
        "asset.activator-rules",
    }


def test_resolver_rejects_selected_asset_without_deployment_classification() -> None:
    document = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    document["profiles"][0]["asset_refs"].append("asset.bootstrap")
    manifest = SolutionManifest.model_validate(document)
    _, validation = load_repository_manifest(REPO_ROOT)

    with pytest.raises(ProfileResolutionError, match="unclassified asset"):
        resolve_profile(manifest, validation, "core")
