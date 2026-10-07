"""Unit and report-contract tests for live readiness verification."""

from __future__ import annotations

import base64
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from retail_setup.contracts import load_repository_manifest, resolve_profile

from deploy.scripts.deploy_config import OperationsPlanningConfig, TwinsConfig
from deploy.scripts.verify_readiness import (
    CheckFailed,
    CheckResult,
    EvidenceUnknown,
    ExpectedItem,
    FabricReadinessAdapter,
    KqlInventory,
    ReadinessContext,
    ReadinessRunner,
    _connect_lakehouse_sql,
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


def test_notebook_source_definition_reads_fabric_metadata_binding() -> None:
    source = "\n".join(
        [
            "# Fabric notebook source",
            "# META {",
            '# META   "dependencies": {',
            '# META     "lakehouse": {',
            f'# META       "default_lakehouse": "{LAKEHOUSE_ID}",',
            '# META       "default_lakehouse_name": "retail_lakehouse",',
            (
                '# META       "default_lakehouse_workspace_id": '
                f'"{WORKSPACE_ID}"'
            ),
            "# META     }",
            "# META   }",
            "# META }",
            "",
            "print('ready')",
        ]
    )
    definition = _definition("notebook-content.py", source)

    assert notebook_binding_errors(
        definition,
        lakehouse_id=LAKEHOUSE_ID,
        lakehouse_name="retail_lakehouse",
        workspace_id=WORKSPACE_ID,
    ) == []


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
                    {
                        "artifactType": "Pipeline",
                        "artifactUniqueId": "Pipeline:resolved",
                        "artifactObjectId": "resolved",
                    },
                    {
                        "artifactType": "Pipeline",
                        "artifactUniqueId": "Pipeline:stale",
                        "artifactObjectId": "stale",
                    },
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
        {
            "tasks": [
                {
                    "items": [
                        {
                            "artifactType": "Pipeline",
                            "artifactUniqueId": "Pipeline:resolved",
                            "artifactObjectId": "resolved",
                        }
                    ]
                }
            ]
        },
        expected,
    )
    errors = taskflow_binding_errors(expected, raw_actual, portable_actual)

    assert "live task flow contains an unresolved item reference" in errors
    assert "task-flow edges differ" in errors


def test_taskflow_binding_rejects_conflicting_unique_and_object_ids() -> None:
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
        "edges": [],
    }
    raw_actual = {
        "tasks": [
            {
                "items": [
                    {
                        "artifactType": "Pipeline",
                        "artifactUniqueId": "Pipeline:wrong",
                        "artifactObjectId": "expected",
                    }
                ]
            }
        ],
        "edges": [],
    }

    errors = taskflow_binding_errors(expected, raw_actual, expected)

    assert any("object ID disagrees" in error for error in errors)


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


def test_kql_inventory_parses_additional_paths_with_the_same_parser(tmp_path):
    source = tmp_path / "native.kql"
    source.write_text(
        ".create-merge table custom_signal (value:long)\n"
        ".create-or-alter function custom_lookup() { custom_signal }\n"
        ".create-or-alter materialized-view custom_view on table custom_signal {}\n"
        ".create-or-alter table custom_signal ingestion json mapping 'CustomMap' '[]'\n",
        encoding="utf-8",
    )

    inventory = parse_kql_inventory(
        tmp_path, [], additional_paths=[Path("native.kql")]
    )

    assert inventory.tables == frozenset({"custom_signal"})
    assert inventory.functions == frozenset({"custom_lookup"})
    assert inventory.materialized_views == frozenset({"custom_view"})
    assert inventory.mappings == frozenset({"custom_signal/CustomMap"})


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
            [{"staging_table_count": 0}],
            [{"table_count": 1}],
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


def test_setup_signal_separates_distributed_data_from_catalog_metadata() -> None:
    adapter = object.__new__(FabricReadinessAdapter)
    queries = []
    timestamp = datetime(2026, 9, 6, 0, 20)

    def execute(query):
        queries.append(query)
        if "INFORMATION_SCHEMA" in query and "[ag].[setup_run_log]" in query:
            raise RuntimeError("object is not supported in distributed processing mode")
        if "INFORMATION_SCHEMA" in query:
            return [{"staging_table_count": 2}]
        return [{"run_id": "setup-run", "status": "COMPLETED", "generated_at": timestamp}]

    adapter._execute_sql = execute
    signal = adapter.setup_signal()

    assert len(queries) == 2
    assert signal["status"] == "COMPLETED"
    assert signal["staging_table_count"] == 2
    assert signal["generated_at"] == "2026-09-06T00:20:00+00:00"


@pytest.mark.parametrize(
    "metadata",
    [[], [{}], [{"staging_table_count": None}], [{"staging_table_count": -1}]],
)
def test_setup_signal_does_not_treat_missing_catalog_evidence_as_zero(metadata) -> None:
    adapter = object.__new__(FabricReadinessAdapter)
    results = iter([
        [{"run_id": "setup-run", "status": "COMPLETED", "generated_at": "2026-09-06T00:20:00Z"}],
        metadata,
    ])
    adapter._execute_sql = lambda _: next(results)

    with pytest.raises(EvidenceUnknown, match="staging"):
        adapter.setup_signal()


def test_checkpoint_query_coerces_extent_tags_to_dynamic() -> None:
    adapter = object.__new__(FabricReadinessAdapter)
    queries = []
    adapter._execute_kql = (  # type: ignore[method-assign]
        lambda query, **kwargs: queries.append((query, kwargs)) or []
    )

    signal = adapter.checkpoint_signal(frozenset({"receipt_created"}))

    assert signal is None
    assert "todynamic(Tags)" in queries[0][0]
    assert "mv-expand tag=tag_values" in queries[0][0]


def test_sql_connection_prefers_installed_system_odbc_driver(monkeypatch) -> None:
    calls = []
    connection = object()
    pyodbc = SimpleNamespace(
        drivers=lambda: ["ODBC Driver 18 for SQL Server"],
        connect=lambda *args, **kwargs: calls.append((args, kwargs)) or connection,
    )
    monkeypatch.setitem(sys.modules, "pyodbc", pyodbc)
    monkeypatch.setitem(
        sys.modules,
        "mssql_python",
        SimpleNamespace(
            connect=lambda *_args, **_kwargs: pytest.fail(
                "bundled driver should not be used"
            )
        ),
    )
    credential = SimpleNamespace(
        get_token=lambda scope: SimpleNamespace(token="sql-token")
    )

    result = _connect_lakehouse_sql(
        "server.fabric.microsoft.com",
        "retail_lakehouse",
        credential,
    )

    assert result is connection
    assert "ODBC Driver 18 for SQL Server" in calls[0][0][0]
    assert calls[0][1]["timeout"] == 60
    assert isinstance(calls[0][1]["attrs_before"][1256], bytes)


def test_sql_connection_falls_back_to_bundled_driver(monkeypatch) -> None:
    calls = []
    connection = object()
    monkeypatch.setitem(
        sys.modules,
        "pyodbc",
        SimpleNamespace(drivers=lambda: [], connect=lambda *_args, **_kwargs: None),
    )
    monkeypatch.setitem(
        sys.modules,
        "mssql_python",
        SimpleNamespace(
            connect=lambda *args, **kwargs: calls.append((args, kwargs))
            or connection
        ),
    )
    credential = SimpleNamespace(
        get_token=lambda scope: SimpleNamespace(token="sql-token")
    )

    result = _connect_lakehouse_sql(
        "server.fabric.microsoft.com",
        "retail_lakehouse",
        credential,
    )

    assert result is connection
    assert "Database=retail_lakehouse" in calls[0][0][0]
    assert "Authentication=" not in calls[0][0][0]
    assert calls[0][1]["timeout"] == 60
    assert isinstance(calls[0][1]["attrs_before"][1256], bytes)


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
        expected_run_id="job-a",
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
    with pytest.raises(Exception, match="exact adopted run"):
        correlated_pipeline_run(
            [valid],
            pipeline_id="pipeline-a",
            step_started=step_started,
            step_ended=step_ended,
            expected_run_id="job-b",
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
        twins=TwinsConfig(),
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
    assert setup.status == "PASS"
    assert setup.evidence["cleanup_required"] is True
    assert aggregate_status(cleanup_failed) == "DEGRADED"

    latest = adapter.setup_signal()
    latest.update(status="COMPLETED", staging_table_count=1)
    monkeypatch.setattr(adapter, "setup_signal", lambda: latest)
    later_gold_success = ReadinessRunner(context, adapter).run()
    assert aggregate_status(later_gold_success) == "DEGRADED"
    latest["staging_table_count"] = 0
    assert aggregate_status(ReadinessRunner(context, adapter).run()) == "SUCCEEDED"


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
        twins=TwinsConfig(),
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


def _native_context(enabled: bool = True, include_plan: bool = True) -> ReadinessContext:
    context = _profile_context("standard", datetime(2026, 9, 8, tzinfo=UTC))
    context.config.operations_planning = OperationsPlanningConfig(
        enabled=enabled, include_plan=include_plan,
    )
    return context


def _twins_context(enabled: bool = True) -> ReadinessContext:
    context = _profile_context("standard", datetime(2026, 9, 8, tzinfo=UTC))
    context.config.twins = TwinsConfig(enabled=enabled)
    return context


def test_native_items_are_expected_only_when_enabled():
    baseline = expected_live_items(_native_context(False))
    native = expected_live_items(_native_context())
    added = {item.key for item in native} - {item.key for item in baseline}

    assert added == {
        ("OperationsAgent", "retail-availability-operations"),
        ("Notebook", "prepare-replenishment-case"),
        ("Plan", "retail-replenishment"),
    }
    required = {item.key: item.required for item in native if item.key in added}
    assert required == {
        ("OperationsAgent", "retail-availability-operations"): False,
        ("Notebook", "prepare-replenishment-case"): True,
        ("Plan", "retail-replenishment"): True,
    }


def test_twins_report_is_required_only_when_explicitly_enabled():
    baseline = expected_live_items(_twins_context(False))
    twins = expected_live_items(_twins_context())
    added = {item.key for item in twins} - {item.key for item in baseline}

    assert added == {("Report", "retail-twins")}
    assert all(item.required for item in twins if item.key in added)


def test_twins_inventory_boundary_does_not_claim_visual_or_physical_runtime():
    context = _twins_context()
    expected = expected_live_items(context)
    verifier = ReadinessRunner(context, object())
    verifier._items_error = None
    verifier.items = [
        {
            "type": item.item_type,
            "displayName": item.display_name,
            "id": item.item_id or f"item-{index}",
        }
        for index, item in enumerate(expected)
    ]

    observation = verifier._item_inventory(expected)

    assert "visual runtime" in observation.summary
    boundary = observation.evidence["connected_twins"]
    assert boundary["visual_runtime"] == "NOT_VERIFIED"
    assert boundary["physical_realtime_twin"] == "NOT_CLAIMED"
    assert boundary["publication_report"].endswith("twins-report.json")


def test_twins_report_binding_requires_primary_and_twins_reports():
    context = _twins_context()
    items = [
        {"type": "SemanticModel", "displayName": "retail_model", "id": "model-id"},
        {"type": "Report", "displayName": "retail_model", "id": "primary-report"},
        {"type": "Report", "displayName": "retail-twins", "id": "twins-report"},
    ]
    definitions = {
        "primary-report": _definition(
            "definition.pbir",
            {"datasetReference": {"byPath": {"path": "../retail_model.SemanticModel"}}},
        ),
        "twins-report": _definition(
            "definition.pbir",
            {"datasetReference": {"byConnection": {"modelId": "model-id"}}},
        ),
    }
    verifier = ReadinessRunner(
        context,
        SimpleNamespace(get_definition=lambda item_id: definitions[item_id]),
    )
    verifier.items = items

    observation = verifier._report_binding()

    assert observation.evidence["report_count"] == 2
    assert observation.evidence["reports"] == ["retail_model", "retail-twins"]
    assert observation.evidence["connected_twins"]["semantic_model"] == "retail_model"

    verifier.items = items[:-1]
    with pytest.raises(CheckFailed, match="retail-twins"):
        verifier._report_binding()

    verifier.items = [*items[:2], {**items[2], "id": "primary-report"}]
    with pytest.raises(CheckFailed, match="unique"):
        verifier._report_binding()


def test_default_report_binding_behavior_checks_only_primary_report():
    context = _twins_context(False)
    items = [
        {"type": "SemanticModel", "displayName": "retail_model", "id": "model-id"},
        {"type": "Report", "displayName": "retail_model", "id": "primary-report"},
    ]
    definitions = {
        "primary-report": _definition(
            "definition.pbir",
            {"datasetReference": {"byPath": {"path": "../retail_model.SemanticModel"}}},
        ),
    }
    verifier = ReadinessRunner(
        context,
        SimpleNamespace(get_definition=lambda item_id: definitions[item_id]),
    )
    verifier.items = items

    observation = verifier._report_binding()

    assert observation.evidence == {}


def test_operations_only_inventory_does_not_require_native_plan():
    context = _native_context(include_plan=False)
    expected = expected_live_items(context)
    keys = {item.key for item in expected}
    assert ("OperationsAgent", "retail-availability-operations") in keys
    assert ("Notebook", "prepare-replenishment-case") in keys
    assert not any(item.item_type == "Plan" for item in expected)
    verifier = ReadinessRunner(context, SimpleNamespace())
    verifier._items_error = None
    verifier.items = [
        {"id": f"item-{index}", "type": item.item_type, "displayName": item.display_name}
        for index, item in enumerate(expected)
    ]
    # Terraform IDs are checked separately; exercise only the native subset here.
    selected = [item for item in expected if item.item_type in {"OperationsAgent", "Notebook"}
                and item.display_name in {"retail-availability-operations", "prepare-replenishment-case"}]
    observation = verifier._item_inventory(selected)
    assert observation.evidence["operations_planning"]["plan_interactive_acceptance"] == "NOT_SELECTED"


@pytest.mark.parametrize("enabled", [False, True])
def test_native_kql_inventory_is_included_only_for_enabled_pack(enabled):
    context = _native_context(enabled)
    observed = parse_kql_inventory(
        REPO_ROOT,
        context.config.profile.kql_scripts,
        additional_paths=[Path("fabric") / "operations" / "01-availability-tables.kql"],
    )
    captured = []

    def get_inventory(expected_tables):
        captured.append(expected_tables)
        return observed

    verifier = ReadinessRunner(
        context, SimpleNamespace(get_kql_inventory=get_inventory)
    )
    verifier._run_kql_checks()

    tables = next(check for check in verifier.checks if check.check_id == "kql.tables")
    assert tables.status == "PASS"
    if not enabled:
        assert tables.evidence["unselected_owned_objects"] == [
            "ops_availability_signals", "ops_case_actions",
        ]
    assert ("ops_availability_signals" in captured[0]) is enabled
    assert ("ops_case_actions" in captured[0]) is enabled
    assert len(captured) == 1


def test_disabling_native_pack_does_not_allow_arbitrary_extra_kql_tables():
    context = _native_context(False)
    source = parse_kql_inventory(REPO_ROOT, context.config.profile.kql_scripts)
    observed = KqlInventory(
        tables=source.tables | {"unexpected_table"},
        functions=source.functions,
        materialized_views=source.materialized_views,
        mappings=source.mappings,
    )
    verifier = ReadinessRunner(
        context, SimpleNamespace(get_kql_inventory=lambda _: observed),
    )
    verifier._run_kql_checks()
    assert next(check for check in verifier.checks if check.check_id == "kql.tables").status == "FAIL"


@pytest.mark.parametrize("live_stream_observed", [False, True])
def test_native_seed_inventory_does_not_supply_optional_stream_freshness(
    monkeypatch, live_stream_observed
):
    context = _native_context()
    stream_tables = frozenset(
        event.event_type
        for contract in context.manifest.data_contracts
        if contract.kind == "event"
        for event in contract.events
    )
    inventory = parse_kql_inventory(
        REPO_ROOT,
        context.config.profile.kql_scripts,
        additional_paths=[Path("fabric") / "operations" / "01-availability-tables.kql"],
    )
    adapter = object.__new__(FabricReadinessAdapter)
    monkeypatch.setattr(adapter, "get_kql_inventory", lambda _tables: inventory)
    queries = []

    def execute_kql(query, *, management=False):
        queries.append(query)
        if management:
            # Even a seed extent with a stream-like tag is not live event evidence.
            tables = ["ops_availability_signals"]
            if live_stream_observed:
                tables.append("receipt_created")
            return [
                {
                    "TableName": name,
                    "tag": f"retail-demo:stream-a:{name}:1",
                    "MaxCreatedOn": context.observed_at,
                }
                for name in tables
            ]
        union, _, _summary = query.partition(" | summarize")
        assert set(union.removeprefix("union withsource=table_name ").split(", ")) == (
            stream_tables
        )
        assert "ops_" not in query
        return (
            [
                {"table_name": name, "latest": context.observed_at, "row_count": 1}
                for name in stream_tables
            ]
            if live_stream_observed
            else []
        )

    monkeypatch.setattr(adapter, "_execute_kql", execute_kql)
    verifier = ReadinessRunner(context, adapter)
    verifier._run_kql_checks()
    assert all(check.status == "PASS" for check in verifier.checks)
    assert "ops_availability_signals" in verifier.kql_inventory.tables
    for check_id, function in (
        ("freshness.eventhouse_ingestion", verifier._eventhouse_freshness),
        ("freshness.checkpoint", verifier._checkpoint_freshness),
    ):
        verifier._check(
            check_id,
            "freshness",
            selected=True,
            required=False,
            function=function,
        )
    assert len(queries) == 2
    assert [check.status for check in verifier.checks[-2:]] == (
        ["PASS", "PASS"] if live_stream_observed else ["UNKNOWN", "UNKNOWN"]
    )


@pytest.mark.parametrize("problem", [None, "missing-plan", "duplicate-plan"])
def test_native_inventory_accepts_stopped_agent_without_claiming_activation(problem):
    context = _native_context()
    expected = expected_live_items(context)
    verifier = ReadinessRunner(context, object())
    verifier._items_error = None
    verifier.items = [
        {
            "type": item.item_type,
            "displayName": item.display_name,
            "id": item.item_id or f"item-{index}",
            "state": "STOPPED" if item.item_type == "OperationsAgent" else None,
        }
        for index, item in enumerate(expected)
    ]
    plan = next(item for item in verifier.items if item["type"] == "Plan")
    if problem == "missing-plan":
        verifier.items.remove(plan)
    elif problem == "duplicate-plan":
        verifier.items.append({**plan, "id": "duplicate-plan-id"})
    if problem:
        with pytest.raises(CheckFailed):
            verifier._item_inventory(expected)
        return

    observation = verifier._item_inventory(expected)

    assert "does not verify activated monitoring" in observation.summary
    boundary = observation.evidence["operations_planning"]
    assert boundary["monitoring_readiness"] == "NOT_VERIFIED"
    assert boundary["plan_interactive_acceptance"] == "NOT_VERIFIED"
    assert boundary["manual_activation_required"] is True
    assert boundary["publication_report"].endswith("operations-planning-report.json")


@pytest.mark.parametrize("invalid_action", [False, True])
def test_native_action_notebook_lakehouse_binding_is_verified(
    monkeypatch, invalid_action
):
    context = _native_context()
    verifier = ReadinessRunner(context, object())
    checked = []

    def definition(item_type, name):
        assert item_type == "Notebook"
        checked.append(name)
        return _definition(
            "notebook-content.ipynb",
            {
                "metadata": {
                    "dependencies": {
                        "lakehouse": {
                            "default_lakehouse": (
                                "wrong"
                                if invalid_action and name == "prepare-replenishment-case"
                                else LAKEHOUSE_ID
                            ),
                            "default_lakehouse_name": "retail_lakehouse",
                            "default_lakehouse_workspace_id": WORKSPACE_ID,
                        }
                    }
                }
            },
        )

    monkeypatch.setattr(verifier, "_definition", definition)
    if invalid_action:
        with pytest.raises(CheckFailed, match="Lakehouse bindings"):
            verifier._notebook_bindings()
    else:
        verifier._notebook_bindings()
    assert checked.count("prepare-replenishment-case") == 1


@pytest.mark.parametrize("enabled", [False, True])
def test_native_inventory_queries_typed_endpoints_only_when_enabled(
    monkeypatch, enabled
):
    import deploy.scripts.taskflow as taskflow_module
    import deploy.scripts.verify_readiness as readiness_module

    context = _native_context(enabled)
    adapter = object.__new__(FabricReadinessAdapter)
    adapter.context = context
    adapter.fabric = object()
    adapter.workspace_id = WORKSPACE_ID
    urls = []
    generic = [
        {"id": "existing-notebook", "type": "Notebook", "displayName": "other"},
        {
            "id": "agent-id",
            "type": "OperationsAgent",
            "displayName": "retail-availability-operations",
        },
    ]
    typed = {
        "operationsAgents": [
            {"id": "agent-id", "displayName": "retail-availability-operations"}
        ],
        "notebooks": [
            {"id": "action-id", "displayName": "prepare-replenishment-case"}
        ],
        "plans": [
            {"id": "plan-id", "displayName": "retail-replenishment"}
        ],
    }

    def paginated(_session, url):
        urls.append(url)
        return list(generic) if url.endswith("/items") else typed[url.rsplit("/", 1)[1]]

    def supplement(_session, _workspace, items, required_types):
        assert "OperationsAgent" not in required_types
        assert "Plan" not in required_types
        return items

    monkeypatch.setattr(readiness_module, "paginated_get", paginated)
    monkeypatch.setattr(taskflow_module, "supplement_workspace_items", supplement)

    items = adapter.list_items()

    assert len(urls) == (4 if enabled else 1)
    assert len([item for item in items if item["id"] == "agent-id"]) == 1
    if enabled:
        assert {item["type"] for item in items} == {"Notebook", "OperationsAgent", "Plan"}
        assert any(item["id"] == "action-id" for item in items)


def test_live_item_inventory_supplements_preview_types(monkeypatch) -> None:
    import deploy.scripts.taskflow as taskflow_module
    import deploy.scripts.verify_readiness as readiness_module

    context = _profile_context(
        "full-demo",
        datetime(2026, 7, 21, 10, 0, tzinfo=UTC),
    )
    adapter = object.__new__(FabricReadinessAdapter)
    adapter.context = context
    adapter.fabric = object()
    adapter.workspace_id = WORKSPACE_ID
    captured = {}
    monkeypatch.setattr(
        readiness_module,
        "paginated_get",
        lambda *_args, **_kwargs: [{"id": "lakehouse", "type": "Lakehouse"}],
    )

    def supplement(_session, _workspace, items, required_types):
        captured["required_types"] = required_types
        return [
            *items,
            {"id": "ontology", "type": "Ontology"},
            {"id": "agent", "type": "DataAgent"},
        ]

    monkeypatch.setattr(
        taskflow_module,
        "supplement_workspace_items",
        supplement,
    )

    items = adapter.list_items()

    assert {"Ontology", "DataAgent"} <= captured["required_types"]
    assert {item["type"] for item in items} >= {
        "Lakehouse",
        "Ontology",
        "DataAgent",
    }


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
        "start_time": (now - timedelta(minutes=10)).isoformat(),
        "end_time": (now - timedelta(minutes=4)).isoformat(),
    }

    observation = readiness._model_freshness("required")
    assert observation.freshness is not None
    assert observation.freshness["age_seconds"] == 300
    assert "generation timestamp" in observation.freshness["lineage"]

    adapter.run_id_present = False
    with pytest.raises(EvidenceUnknown, match="blank model_run_id"):
        readiness._model_freshness("required")


def test_optional_ml_freshness_accepts_pipeline_correlated_empty_snapshots() -> None:
    now = datetime(2026, 7, 21, 10, 0, tzinfo=UTC)
    context = _profile_context("full-demo", now)
    optional_contracts = [
        contract
        for contract in context.manifest.ml_contracts
        if contract.tier == "optional"
    ]

    class EmptyModelAdapter:
        def model_signals(self, contracts):
            return [
                {
                    "contract_id": contract["id"],
                    "as_of": None,
                    "run_id_present": False,
                    "lineage_hash": contract["id"],
                }
                for contract in contracts
            ]

    readiness = ReadinessRunner(context, EmptyModelAdapter())
    readiness.pipeline_evidence["ml-required"] = {
        "start_time": (now - timedelta(minutes=20)).isoformat(),
        "end_time": (now - timedelta(minutes=12)).isoformat(),
    }
    readiness.pipeline_evidence["ml-optional"] = {
        "start_time": (now - timedelta(minutes=10)).isoformat(),
        "end_time": (now - timedelta(minutes=5)).isoformat(),
    }

    observation = readiness._model_freshness("optional")

    assert observation.freshness is not None
    # Optional contracts produced by a notebook that runs inside ml-required
    # fall back to that pipeline's end time, which is the oldest evidence.
    assert observation.freshness["age_seconds"] == 720
    assert len(observation.evidence["empty_contracts"]) == len(optional_contracts)


def test_model_freshness_correlates_contracts_with_their_producer_pipeline() -> None:
    """A contract is judged against the pipeline that runs its producer notebook.

    07-ml-market-basket and 10-ml-promotion-effectiveness run inside ml-required
    because they emit required-tier outputs, so their non-required outputs must
    not be flagged stale for predating the ml-optional/ml-experimental windows.
    """

    now = datetime(2026, 7, 21, 10, 0, tzinfo=UTC)
    context = _profile_context("full-demo", now)

    class ModelAdapter:
        def model_signals(self, contracts):
            # Outputs of a producer that runs inside ml-required are generated
            # well before the ml-optional / ml-experimental pipelines start.
            return [
                {
                    "contract_id": contract["id"],
                    "as_of": (
                        now
                        - timedelta(
                            minutes=(
                                15
                                if contract["pipeline_ref"]
                                == "ml-required.DataPipeline"
                                else 8
                            )
                        )
                    ).isoformat(),
                    "run_id_present": True,
                    "lineage_hash": contract["id"],
                }
                for contract in contracts
            ]

    readiness = ReadinessRunner(context, ModelAdapter())
    readiness.pipeline_evidence["ml-required"] = {
        "start_time": (now - timedelta(minutes=20)).isoformat(),
        "end_time": (now - timedelta(minutes=12)).isoformat(),
    }
    for name in ("ml-optional", "ml-experimental"):
        readiness.pipeline_evidence[name] = {
            "start_time": (now - timedelta(minutes=10)).isoformat(),
            "end_time": (now - timedelta(minutes=5)).isoformat(),
        }

    producer_pipelines = readiness._producer_pipeline_refs()
    assert producer_pipelines["07-ml-market-basket"] == "ml-required.DataPipeline"
    assert (
        producer_pipelines["10-ml-promotion-effectiveness"]
        == "ml-required.DataPipeline"
    )

    for tier in ("optional", "experimental"):
        observation = readiness._model_freshness(tier)
        assert observation.freshness is not None
        assert observation.freshness["age_seconds"] == 900
        assert observation.evidence["empty_contracts"] == []
        assert observation.evidence["model_count"] == len(
            [
                contract
                for contract in context.manifest.ml_contracts
                if contract.tier == tier
            ]
        )


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
