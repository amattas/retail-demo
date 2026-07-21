"""Unit and report-contract tests for live readiness verification."""

from __future__ import annotations

import base64
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from retail_setup.contracts import load_repository_manifest, resolve_profile

from deploy.scripts.verify_readiness import (
    CheckResult,
    EvidenceUnknown,
    ExpectedItem,
    FabricReadinessAdapter,
    ReadinessContext,
    ReadinessRunner,
    aggregate_status,
    build_report,
    checkpoint_signal_from_rows,
    compare_item_inventory,
    compare_sets,
    correlated_pipeline_run,
    data_agent_binding_errors,
    evaluate_freshness,
    exit_code_for_status,
    expected_live_items,
    load_readiness_context,
    notebook_binding_errors,
    parse_kql_inventory,
    pipeline_binding_errors,
    queryset_binding_errors,
    report_binding_errors,
    semantic_model_binding_errors,
    taskflow_binding_errors,
    validate_terminal_job_evidence,
    verify_environment,
    write_report_atomic,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TENANT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
WORKSPACE_ID = "11111111-1111-4111-8111-111111111111"
LAKEHOUSE_ID = "22222222-2222-4222-8222-222222222222"


def _part(path: str, value: object) -> dict[str, str]:
    payload = value if isinstance(value, str) else json.dumps(value)
    return {
        "path": path,
        "payload": base64.b64encode(payload.encode()).decode(),
    }


def _definition(path: str, value: object) -> dict:
    return {"parts": [_part(path, value)]}


def test_item_inventory_reports_missing_duplicates_and_id_mismatch() -> None:
    expected = [
        ExpectedItem("Notebook", "one"),
        ExpectedItem("Lakehouse", "retail_lakehouse", LAKEHOUSE_ID),
        ExpectedItem("SemanticModel", "empty-id"),
        ExpectedItem("Report", "missing"),
    ]
    observed = [
        {"type": "Notebook", "displayName": "one", "id": "one-a"},
        {"type": "Notebook", "displayName": "one", "id": "one-b"},
        {
            "type": "Lakehouse",
            "displayName": "retail_lakehouse",
            "id": "wrong",
        },
        {"type": "SemanticModel", "displayName": "empty-id", "id": ""},
    ]

    comparison = compare_item_inventory(expected, observed)

    assert comparison["duplicates"] == ["Notebook:one"]
    assert comparison["missing"] == ["Report:missing"]
    assert comparison["missing_ids"] == ["SemanticModel:empty-id"]
    assert comparison["mismatched_ids"][0]["expected_id"] == LAKEHOUSE_ID


def test_notebook_definition_requires_exact_lakehouse_binding() -> None:
    definition = _definition(
        "notebook-content.ipynb",
        {
            "metadata": {
                "dependencies": {
                    "lakehouse": {
                        "default_lakehouse": LAKEHOUSE_ID,
                        "default_lakehouse_name": "retail_lakehouse",
                        "default_lakehouse_workspace_id": WORKSPACE_ID,
                    }
                }
            }
        },
    )

    assert (
        notebook_binding_errors(
            definition,
            lakehouse_id=LAKEHOUSE_ID,
            lakehouse_name="retail_lakehouse",
            workspace_id=WORKSPACE_ID,
        )
        == []
    )
    assert "default_lakehouse ID mismatch" in notebook_binding_errors(
        definition,
        lakehouse_id="wrong",
        lakehouse_name="retail_lakehouse",
        workspace_id=WORKSPACE_ID,
    )


def test_pipeline_definition_detects_missing_and_mismatched_refs() -> None:
    definition = _definition(
        "pipeline-content.json",
        {
            "properties": {
                "activities": [
                    {
                        "name": "setup-one",
                        "type": "TridentNotebook",
                        "typeProperties": {
                            "notebookId": "notebook-id",
                            "workspaceId": WORKSPACE_ID,
                        },
                    }
                ]
            }
        },
    )

    assert (
        pipeline_binding_errors(
            definition,
            expected_notebook_ids={"setup-one": "notebook-id"},
            workspace_id=WORKSPACE_ID,
        )
        == []
    )
    errors = pipeline_binding_errors(
        definition,
        expected_notebook_ids={"setup-one": "wrong", "setup-two": "missing"},
        workspace_id=WORKSPACE_ID,
    )
    assert "pipeline notebook activity inventory mismatch" in errors
    assert "setup-one: notebook ID mismatch" in errors


def test_powerbi_and_agent_definition_bindings() -> None:
    semantic = _definition(
        "definition/expressions.tmdl",
        f'https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{LAKEHOUSE_ID}',
    )
    report = _definition(
        "definition.pbir",
        {"datasetReference": {"byPath": {"path": "../retail_model.SemanticModel"}}},
    )
    agent = {
        "parts": [
            _part(
                "Files/Config/draft/source/datasource.json",
                {"workspaceId": WORKSPACE_ID, "artifactId": "model-id"},
            )
        ]
    }

    assert not semantic_model_binding_errors(
        semantic,
        workspace_id=WORKSPACE_ID,
        lakehouse_id=LAKEHOUSE_ID,
    )
    assert not report_binding_errors(
        report,
        semantic_model_id="model-id",
        semantic_model_name="retail_model",
    )
    assert not data_agent_binding_errors(
        agent,
        workspace_id=WORKSPACE_ID,
        expected_artifact_ids={"model-id"},
    )
    assert data_agent_binding_errors(
        agent,
        workspace_id=WORKSPACE_ID,
        expected_artifact_ids={"other-id"},
    ) == ["data agent artifact binding mismatch"]


def test_queryset_definition_requires_exact_database_and_tabs() -> None:
    definition = _definition(
        "RealTimeQueryset.json",
        {
            "queryset": {
                "dataSources": [
                    {
                        "databaseItemId": "kql-id",
                        "databaseItemName": "retail_eventhouse",
                    }
                ],
                "tabs": [{"title": "sales"}, {"title": "inventory"}],
            }
        },
    )

    assert not queryset_binding_errors(
        definition,
        kql_database_id="kql-id",
        kql_database_name="retail_eventhouse",
        expected_tabs={"sales", "inventory"},
    )
    errors = queryset_binding_errors(
        definition,
        kql_database_id="wrong",
        kql_database_name="retail_eventhouse",
        expected_tabs={"sales"},
    )
    assert "queryset KQL database ID mismatch" in errors
    assert "queryset tab inventory mismatch" in errors


def test_taskflow_binding_detects_unresolved_references_and_duplicate_edges() -> None:
    expected = {
        "tasks": [
            {
                "items": [
                    {
                        "artifactType": "Pipeline",
                        "artifactName": "setup-pipeline",
                    }
                ]
            }
        ],
        "edges": [{"source": "setup", "target": "report"}],
    }
    raw_actual = {
        "tasks": [
            {
                "items": [
                    {"artifactObjectId": "resolved"},
                    {"artifactObjectId": "stale"},
                ]
            }
        ],
        "edges": [
            {"source": "setup", "target": "report"},
            {"source": "setup", "target": "report"},
        ],
    }
    portable_actual = {
        "tasks": expected["tasks"],
        "edges": raw_actual["edges"],
    }

    assert not taskflow_binding_errors(
        expected,
        {"tasks": [{"items": [{"artifactObjectId": "resolved"}]}]},
        expected,
    )
    errors = taskflow_binding_errors(expected, raw_actual, portable_actual)

    assert "live task flow contains an unresolved item reference" in errors
    assert "task-flow edges differ" in errors


def test_selected_kql_inventory_is_source_derived_and_differences_fail_closed() -> None:
    inventory = parse_kql_inventory(
        REPO_ROOT,
        [
            "01-create-tables.kql",
            "02-create-ingestion-mappings.kql",
            "03-create-functions.kql",
            "04-create-materialized-views.kql",
        ],
    )

    assert "receipt_created" in inventory.tables
    assert "fn_truck_sla" in inventory.functions
    assert "mv_store_sales_minute" in inventory.materialized_views
    assert "receipt_created/EventMapping" in inventory.mappings
    difference = compare_sets(
        frozenset({"one", "two"}),
        frozenset({"two", "three"}),
    )
    assert difference["missing"] == ["one"]
    assert difference["unexpected"] == ["three"]


def test_freshness_rejects_missing_stale_future_and_mismatched_evidence() -> None:
    now = datetime(2026, 7, 21, 10, 0, tzinfo=UTC)
    fresh = evaluate_freshness(
        (now - timedelta(minutes=5)).isoformat(),
        observed_at=now,
        max_age=timedelta(minutes=30),
        lineage="test",
    )
    assert fresh["age_seconds"] == 300
    with pytest.raises(Exception, match="stale"):
        evaluate_freshness(
            (now - timedelta(hours=1)).isoformat(),
            observed_at=now,
            max_age=timedelta(minutes=30),
            lineage="test",
        )
    with pytest.raises(Exception, match="future"):
        evaluate_freshness(
            (now + timedelta(hours=1)).isoformat(),
            observed_at=now,
            max_age=timedelta(minutes=30),
            lineage="test",
        )
    with pytest.raises(Exception, match="predates"):
        evaluate_freshness(
            (now - timedelta(minutes=20)).isoformat(),
            observed_at=now,
            max_age=timedelta(minutes=30),
            not_before=now,
            lineage="test",
        )


def test_checkpoint_signal_parses_kusto_wrapped_tags_and_rejects_mismatches() -> None:
    rows = [
        {
            "TableName": "receipt_created",
            "tag": "ingest-by:retail-demo:stream-a:receipt_created:17",
            "MaxCreatedOn": "2026-07-21T09:58:00Z",
        },
        {
            "TableName": "payment_processed",
            "tag": "retail-demo:stream-a:receipt_created:18",
            "MaxCreatedOn": "2026-07-21T09:59:00Z",
        },
    ]

    signal = checkpoint_signal_from_rows(
        rows,
        frozenset({"receipt_created", "payment_processed"}),
    )

    assert signal is not None
    assert signal["latest_batch_id"] == 17
    assert signal["table_count"] == 1
    assert signal["stream_id_hash"]


def test_sql_signals_normalize_driver_datetimes_without_credentials() -> None:
    timestamp = datetime(2026, 7, 21, 9, 58)
    adapter = object.__new__(FabricReadinessAdapter)
    rows = iter(
        [
            [
                {
                    "run_id": "run-a",
                    "status": "COMPLETED",
                    "generated_at": timestamp,
                }
            ],
            [{"source_count": 2, "updated_at": timestamp}],
        ]
    )
    adapter._execute_sql = lambda _query: next(rows)  # type: ignore[method-assign]

    setup = adapter.setup_signal()
    watermark = adapter.watermark_signal()

    assert setup is not None
    assert setup["generated_at"] == "2026-07-21T09:58:00+00:00"
    assert watermark is not None
    assert watermark["updated_at"] == "2026-07-21T09:58:00+00:00"


def test_terminal_pipeline_evidence_requires_complete_ordered_timestamps() -> None:
    valid = {
        "id": "job-a",
        "status": "Completed",
        "start_time": "2026-07-21T09:50:00Z",
        "end_time": "2026-07-21T09:55:00Z",
    }
    validate_terminal_job_evidence(valid)

    with pytest.raises(Exception, match="timestamp evidence is missing"):
        validate_terminal_job_evidence({**valid, "end_time": None})
    with pytest.raises(Exception, match="ended before"):
        validate_terminal_job_evidence(
            {
                **valid,
                "start_time": "2026-07-21T09:56:00Z",
            }
        )
    with pytest.raises(Exception, match="not terminal-successful"):
        validate_terminal_job_evidence({**valid, "status": "Mystery"})


def test_pipeline_history_correlation_rejects_stale_and_out_of_window_runs() -> None:
    step_started = datetime(2026, 7, 21, 9, 50, tzinfo=UTC)
    step_ended = datetime(2026, 7, 21, 10, 0, tzinfo=UTC)
    valid = {
        "id": "job-a",
        "itemId": "pipeline-a",
        "jobType": "Pipeline",
        "status": "Completed",
        "startTimeUtc": "2026-07-21T09:51:00Z",
        "endTimeUtc": "2026-07-21T09:59:00Z",
    }

    evidence = correlated_pipeline_run(
        [valid],
        pipeline_id="pipeline-a",
        step_started=step_started,
        step_ended=step_ended,
    )
    assert evidence["id"] == "job-a"

    with pytest.raises(Exception, match="no run within"):
        correlated_pipeline_run(
            [
                {
                    **valid,
                    "startTimeUtc": "2026-07-21T09:30:00Z",
                    "endTimeUtc": "2026-07-21T09:40:00Z",
                }
            ],
            pipeline_id="pipeline-a",
            step_started=step_started,
            step_ended=step_ended,
        )
    with pytest.raises(Exception, match="outside its journal step"):
        correlated_pipeline_run(
            [{**valid, "endTimeUtc": "2026-07-21T10:10:00Z"}],
            pipeline_id="pipeline-a",
            step_started=step_started,
            step_ended=step_ended,
        )


@pytest.mark.parametrize(
    ("required_status", "optional_status", "expected", "exit_code"),
    [
        ("PASS", "PASS", "SUCCEEDED", 0),
        ("FAIL", "PASS", "FAILED", 1),
        ("UNKNOWN", "PASS", "FAILED", 1),
        ("PASS", "FAIL", "DEGRADED", 3),
        ("PASS", "UNKNOWN", "DEGRADED", 3),
    ],
)
def test_status_aggregation_and_exit_codes(
    required_status: str,
    optional_status: str,
    expected: str,
    exit_code: int,
) -> None:
    checks = [
        CheckResult(
            "required",
            "test",
            True,
            True,
            required_status,
            "required",
        ),
        CheckResult(
            "optional",
            "test",
            True,
            False,
            optional_status,
            "optional",
        ),
        CheckResult(
            "unselected",
            "test",
            False,
            False,
            "SKIPPED",
            "not selected",
        ),
    ]

    status = aggregate_status(checks)

    assert status == expected
    assert exit_code_for_status(status) == exit_code


def test_selected_skipped_check_is_not_success() -> None:
    check = CheckResult(
        "selected",
        "test",
        True,
        True,
        "SKIPPED",
        "invalid skip",
    )
    assert aggregate_status([check]) == "FAILED"


def test_report_write_is_atomic_bounded_and_redacted(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "deploy" / ".generated" / "dev" / "readiness-report.json"
    replacements: list[tuple[Path, Path]] = []
    real_replace = os.replace

    def spy_replace(source: str, target: str) -> None:
        replacements.append((Path(source), Path(target)))
        real_replace(source, target)

    monkeypatch.setattr(
        "deploy.scripts.verify_readiness.os.replace",
        spy_replace,
    )
    write_report_atomic(
        path,
        {
            "schema_version": "1.0.0",
            "token": "do-not-write",
            "summary": "Bearer " + "synthetic-secret",
            "tenant_id": TENANT_ID,
            "tenant_summary": f"tenant_id={TENANT_ID}",
            "long": "x" * 1000,
        },
    )

    raw = path.read_text(encoding="utf-8")
    assert "do-not-write" not in raw
    assert "synthetic-secret" not in raw
    assert TENANT_ID not in raw
    assert len(json.loads(raw)["long"]) == 300
    assert replacements[0][0] != replacements[0][1]
    assert list(path.parent.glob("*.tmp")) == []


def test_context_validates_terraform_targets_before_live_use(
    monkeypatch,
    tmp_path: Path,
) -> None:
    manifest, validation = load_repository_manifest(REPO_ROOT)
    profile = resolve_profile(manifest, validation, "core")
    config = SimpleNamespace(profile=profile)
    outputs = {"workspace_id": WORKSPACE_ID}
    calls: list[tuple[object, object]] = []
    monkeypatch.setattr(
        "deploy.scripts.verify_readiness.load_environment",
        lambda *_args, **_kwargs: config,
    )
    monkeypatch.setattr(
        "deploy.scripts.verify_readiness.load_terraform_outputs",
        lambda _path: outputs,
    )
    monkeypatch.setattr(
        "deploy.scripts.verify_readiness.validate_terraform_outputs",
        lambda candidate, candidate_outputs: calls.append(
            (candidate, candidate_outputs)
        ),
    )
    monkeypatch.setattr(
        "retail_setup.contracts.load_repository_manifest",
        lambda _root: (manifest, validation),
    )

    context = load_readiness_context(tmp_path, "dev")

    assert context.outputs is outputs
    assert calls == [(config, outputs)]


class _CoreAdapter:
    def __init__(self, now: datetime) -> None:
        self.now = now
        self.setup_status = "COMPLETED"
        self.items = [
            {
                "type": "Lakehouse",
                "displayName": "retail_lakehouse",
                "id": LAKEHOUSE_ID,
            },
            {
                "type": "SQLEndpoint",
                "displayName": "retail_lakehouse",
                "id": "sql-id",
            },
        ]
        self.definitions: dict[str, dict] = {}
        for index, name in enumerate(
            (
                "setup-01-seed-dictionaries",
                "setup-02-generate-dimensions",
                "setup-03-generate-facts",
                "setup-04-build-gold",
            )
        ):
            item_id = f"notebook-{index}"
            self.items.append(
                {"type": "Notebook", "displayName": name, "id": item_id}
            )
            self.definitions[item_id] = _definition(
                "notebook-content.ipynb",
                {
                    "metadata": {
                        "dependencies": {
                            "lakehouse": {
                                "default_lakehouse": LAKEHOUSE_ID,
                                "default_lakehouse_name": "retail_lakehouse",
                                "default_lakehouse_workspace_id": WORKSPACE_ID,
                            }
                        }
                    }
                },
            )

    def list_items(self):
        return self.items

    def get_definition(self, item_id: str):
        return self.definitions[item_id]

    def setup_signal(self):
        return {
            "run_id": "setup-run",
            "status": self.setup_status,
            "generated_at": (self.now - timedelta(minutes=2)).isoformat(),
        }

    def __getattr__(self, name: str):
        raise AssertionError(f"unselected adapter method called: {name}")


def test_core_profile_runs_fixed_taxonomy_and_skips_unselected_capabilities(
    monkeypatch,
    tmp_path: Path,
) -> None:
    manifest, validation = load_repository_manifest(REPO_ROOT)
    profile = resolve_profile(manifest, validation, "core")
    now = datetime(2026, 7, 21, 10, 0, tzinfo=UTC)
    config = SimpleNamespace(
        environment="dev",
        tenant_id=TENANT_ID,
        deployment=SimpleNamespace(profile="core"),
        profile=profile,
        workspace=SimpleNamespace(
            name="retail-demo-dev",
            existing_id=None,
        ),
        lakehouse=SimpleNamespace(name="retail_lakehouse"),
        eventhouse=SimpleNamespace(
            enabled=False,
            name="retail_eventhouse",
            kql_database_name="retail_eventhouse",
        ),
        powerbi=SimpleNamespace(
            semantic_model_name="retail_model",
            report_name="retail_model",
        ),
    )
    outputs = {
        "deployment_environment": "dev",
        "deployment_profile": "core",
        "tenant_id": TENANT_ID,
        "workspace_id": WORKSPACE_ID,
        "workspace_name": "retail-demo-dev",
        "lakehouse_id": LAKEHOUSE_ID,
        "lakehouse_name": "retail_lakehouse",
    }
    context = ReadinessContext(
        repo_root=REPO_ROOT,
        environment="dev",
        config=config,
        manifest=manifest,
        outputs=outputs,
        manifest_hash="manifest-hash",
        profile_hash="profile-hash",
        deploy_journal=None,
        observed_at=now,
    )

    adapter = _CoreAdapter(now)
    checks = ReadinessRunner(context, adapter).run()

    assert len(checks) == 26
    assert aggregate_status(checks) == "SUCCEEDED"
    assert sum(check.status == "SKIPPED" for check in checks) == 22
    report = build_report(
        context,
        checks,
        run_pipeline_requested=False,
    )
    assert report["schema_version"] == "1.0.0"
    assert report["profile"]["hash"] == "profile-hash"
    assert report["profile"]["support_status"] == "core"
    assert report["profile"]["expected_item_counts"] == {
        "infrastructure": 5,
        "reporting": 0,
        "all": 5,
    }
    assert report["profile"]["asset_boundaries"]["preview"] == []
    assert report["profile"]["asset_boundaries"]["manual"] == []
    assert report["manifest"]["hash"] == "manifest-hash"
    assert report["targets"]["workspace"]["id"] == WORKSPACE_ID
    assert report["mode"] == {
        "read_only": True,
        "pipeline_trigger_requested": False,
        "post_ontology_deferred": False,
    }
    assert report["counts"]["total"] == 26
    assert report["freshness_lineage"][0]["check_id"] == (
        "freshness.setup_run_log"
    )
    monkeypatch.setattr(
        "deploy.scripts.verify_readiness.load_readiness_context",
        lambda *_args, **_kwargs: context,
    )
    persisted, path = verify_environment(
        tmp_path,
        "dev",
        adapter=adapter,
    )
    assert persisted["status"] == "SUCCEEDED"
    assert json.loads(path.read_text(encoding="utf-8"))["counts"]["total"] == 26

    context.deploy_journal = {
        "environment": "dev",
        "manifest": {
            "hash": "manifest-hash",
            "profile_id": profile.id,
            "profile_name": "core",
        },
        "targets": {
            "profile": "core",
            "workspace_name": "retail-demo-dev",
            "lakehouse_name": "retail_lakehouse",
        },
        "steps": [
            {
                "step_id": "pipeline-step",
                "status": "SUCCEEDED",
                "started_at": "2026-07-21T09:50:00Z",
                "ended_at": "2026-07-21T09:55:00Z",
            }
        ],
    }
    correlation_runner = ReadinessRunner(context, adapter)
    assert correlation_runner._journal_step_window("pipeline-step") is not None
    context.deploy_journal["manifest"]["hash"] = "stale-manifest"
    assert correlation_runner._journal_step_window("pipeline-step") is None

    adapter.setup_status = "COMPLETED_CLEANUP_FAILED"
    cleanup_failed = ReadinessRunner(context, adapter).run()
    setup = next(
        check
        for check in cleanup_failed
        if check.check_id == "freshness.setup_run_log"
    )
    assert setup.status == "FAIL"
    assert aggregate_status(cleanup_failed) == "FAILED"


def _profile_context(profile_name: str, now: datetime) -> ReadinessContext:
    manifest, validation = load_repository_manifest(REPO_ROOT)
    profile = resolve_profile(manifest, validation, profile_name)
    config = SimpleNamespace(
        environment="dev",
        tenant_id=TENANT_ID,
        deployment=SimpleNamespace(profile=profile_name),
        profile=profile,
        workspace=SimpleNamespace(
            name="retail-demo-dev",
            existing_id=None,
        ),
        lakehouse=SimpleNamespace(name="retail_lakehouse"),
        eventhouse=SimpleNamespace(
            enabled=profile.provisions_eventhouse,
            name="retail_eventhouse",
            kql_database_name="retail_eventhouse",
        ),
        powerbi=SimpleNamespace(
            semantic_model_name="retail_model",
            report_name="retail_model",
        ),
    )
    return ReadinessContext(
        repo_root=REPO_ROOT,
        environment="dev",
        config=config,
        manifest=manifest,
        outputs={
            "workspace_id": WORKSPACE_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "eventhouse_id": "eventhouse-id",
            "kql_database_id": "kql-database-id",
        },
        manifest_hash="manifest-hash",
        profile_hash="profile-hash",
        deploy_journal=None,
        observed_at=now,
    )


def test_required_ml_freshness_uses_generation_time_and_nonblank_run_id() -> None:
    now = datetime(2026, 7, 21, 10, 0, tzinfo=UTC)
    context = _profile_context("standard", now)

    class ModelAdapter:
        run_id_present = True

        def model_signals(self, contracts):
            assert {contract["as_of_column"] for contract in contracts} == {
                "generated_at"
            }
            assert all(
                "model_run_id" in contract["lineage_columns"]
                for contract in contracts
            )
            return [
                {
                    "contract_id": contract["id"],
                    "as_of": (now - timedelta(minutes=5)).isoformat(),
                    "run_id_present": self.run_id_present,
                    "lineage_hash": contract["id"],
                }
                for contract in contracts
            ]

    adapter = ModelAdapter()
    readiness = ReadinessRunner(context, adapter)
    readiness.pipeline_evidence["ml-required"] = {
        "start_time": (now - timedelta(minutes=10)).isoformat()
    }

    observation = readiness._model_freshness("required")
    assert observation.freshness is not None
    assert observation.freshness["age_seconds"] == 300
    assert "generation timestamp" in observation.freshness["lineage"]

    adapter.run_id_present = False
    with pytest.raises(EvidenceUnknown, match="blank model_run_id"):
        readiness._model_freshness("required")


def test_full_demo_initial_inventory_defers_ontology_dependent_items() -> None:
    now = datetime(2026, 7, 21, 10, 0, tzinfo=UTC)
    context = _profile_context("full-demo", now)

    complete = expected_live_items(context)
    initial = expected_live_items(context, include_post_ontology=False)
    complete_types = {item.item_type for item in complete}
    initial_types = {item.item_type for item in initial}

    assert {"Ontology", "DataAgent"} <= complete_types
    assert "Ontology" not in initial_types
    assert "DataAgent" not in initial_types
    assert len(complete) - len(initial) == 3
