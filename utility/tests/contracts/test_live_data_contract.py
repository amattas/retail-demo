"""IMP-005 source-derived event, route, fixture, and terminal contracts."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from pydantic import ValidationError

from retail_setup.contracts import (
    ManifestSourceError,
    SolutionManifest,
    derive_data_contract_snapshot,
    load_solution_manifest,
    validate_data_contracts,
    validate_manifest_repository,
)
from retail_setup.contracts import data_validation
from retail_setup.contracts.source_parsers import (
    notebook_python_source,
    python_symbol,
    tmdl_active_table_schemas,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = REPO_ROOT / "contracts" / "retail-demo.json"
MANIFEST = load_solution_manifest(MANIFEST_PATH)
SNAPSHOT = derive_data_contract_snapshot(MANIFEST, REPO_ROOT)
VALIDATION = validate_data_contracts(MANIFEST, REPO_ROOT)
EVENT_CONTRACT = next(
    contract for contract in MANIFEST.data_contracts if contract.kind == "event"
)
EVENTS = {event.event_type: event for event in EVENT_CONTRACT.events}
PATHS = {
    path.event_ids[0]: path
    for path in MANIFEST.event_paths
    if path.path_kind == "emitted"
}
EXCEPTIONS = {exception.id: exception for exception in MANIFEST.exceptions}
FIXTURES = {
    event.event_type: event
    for scenario in SNAPSHOT.fixture_scenarios
    for event in scenario.events
    if event.event_type in EVENTS
}


@pytest.mark.parametrize("event_type", sorted(EVENTS))
def test_all_event_wire_route_and_terminal_contracts(event_type: str) -> None:
    """Prove each event from wire schema through a semantic or named boundary."""

    declaration = EVENTS[event_type]
    payload = SNAPSHOT.driver.payloads[event_type]
    envelope = SNAPSHOT.driver.envelope
    ddl = SNAPSHOT.kql_tables[event_type]
    mapping = SNAPSHOT.kql_mappings[event_type]
    fixture = FIXTURES[event_type]
    path = PATHS[declaration.id]

    expected_types = {
        field.name: field.data_type for field in (*payload, *envelope)
    }
    assert ddl == expected_types
    assert {name: item[0] for name, item in mapping.items()} == expected_types
    assert {name: item[1] for name, item in mapping.items()} == {
        **{
            field.name: f"$.payload.{field.source_name}"
            for field in payload
        },
        **{field.name: f"$.{field.name}" for field in envelope},
    }
    assert all(field.nullable for field in (*payload, *envelope))

    wire_values = {
        **fixture.envelope,
        **{
            field.name: fixture.payload[field.source_name]
            for field in payload
            if field.source_name is not None
        },
    }
    assert all(wire_values[key] is not None for key in declaration.business_keys)
    assert declaration.dedupe_keys == SNAPSHOT.silver.dedupe_keys[event_type]
    event_time = wire_values[declaration.event_time.field]
    assert isinstance(event_time, str) and event_time.endswith("Z")
    assert fixture.envelope["ingest_timestamp"].endswith("Z")

    assert [
        target.name for target in path.targets if target.layer == "eventhouse"
    ] == [event_type]
    silver_target = SNAPSHOT.silver.routes[event_type]
    assert [
        target.name for target in path.targets if target.layer == "silver"
    ] == [silver_target]
    for gold_target in (
        target.name for target in path.targets if target.layer == "gold"
    ):
        assert silver_target in SNAPSHOT.gold[gold_target].source_tables

    semantic_targets = [
        target.name for target in path.targets if target.layer == "semantic"
    ]
    if semantic_targets:
        assert path.terminal_exception_id is None
        assert set(semantic_targets) <= set(SNAPSHOT.semantic_tables)
    else:
        assert path.terminal_exception_id in EXCEPTIONS
        exception = EXCEPTIONS[path.terminal_exception_id]
        assert declaration.id in exception.event_ids
        assert silver_target in exception.target_names


def test_envelope_kql_and_fixture_counts_are_exact() -> None:
    assert VALIDATION.event_count == 18
    assert VALIDATION.envelope_field_count == 9
    assert VALIDATION.typed_kql_table_count == 18
    assert VALIDATION.operational_kql_table_count == 1
    assert VALIDATION.mapping_count == 19
    assert VALIDATION.path_count == 19
    assert VALIDATION.exception_count == 4
    assert VALIDATION.fixture_scenario_count == 8
    assert VALIDATION.fixture_event_count == 23


def test_derived_attribution_route_uses_verified_inputs_and_terminals() -> None:
    path = next(
        path
        for path in MANIFEST.event_paths
        if path.id == "event-path.marketing-attribution"
    )
    route = SNAPSHOT.silver.derived_routes["marketing_attribution"]
    event_silver_targets = {
        next(
            target.name
            for target in PATHS[event_id].targets
            if target.layer == "silver"
        )
        for event_id in path.event_ids
    }

    assert event_silver_targets == set(route.source_tables)
    assert route.target_table == "fact_marketing_attribution"
    assert SNAPSHOT.gold["campaign_performance_daily"].source_tables == (
        "fact_marketing",
        "fact_marketing_attribution",
    )
    assert {
        target.name for target in path.targets if target.layer == "semantic"
    } == {"fact_marketing_attribution", "campaign_performance_daily"}


def test_named_boundaries_match_only_source_derived_differences() -> None:
    assert VALIDATION.operational_targets == {"unknown_event"}
    assert VALIDATION.streaming_only_targets == {"fact_online_order_status"}
    assert VALIDATION.historical_only_targets == {
        "dc_inventory_position_current",
        "dim_customers",
        "dim_date",
        "dim_distribution_centers",
        "dim_geographies",
        "dim_products",
        "dim_stores",
        "dim_trucks",
        "fact_dc_inventory_txn",
        "fact_online_order_lines",
        "fact_promo_lines",
        "fact_truck_inventory",
    }
    for kind in ("operational-catch-all", "streaming-only", "historical-only"):
        exception = next(
            exception for exception in MANIFEST.exceptions if exception.kind == kind
        )
        assert exception.rationale
        assert exception.verification_owner


def test_nullable_and_unknown_fixtures_cover_intentional_boundaries() -> None:
    scenarios = {scenario.id: scenario for scenario in SNAPSHOT.fixture_scenarios}
    nullable = scenarios["nullable-variants"].events
    receipt = next(event for event in nullable if event.event_type == "receipt_created")
    line = next(event for event in nullable if event.event_type == "receipt_line_added")
    declined = next(
        event for event in nullable if event.event_type == "payment_processed"
    )
    unknown = scenarios["unknown-event-dlq"].events

    assert receipt.payload["campaign_id"] is None
    assert receipt.payload["impression_id"] is None
    assert line.payload["promo_code"] is None
    assert declined.payload["order_id"] is None
    assert declined.payload["decline_reason"] == "INSUFFICIENT_FUNDS"
    assert len(unknown) == 1
    assert unknown[0].event_type not in EVENTS
    assert "unknown_event" in SNAPSHOT.kql_tables


def test_active_tmdl_types_and_legacy_names_are_source_derived() -> None:
    assert len(SNAPSHOT.semantic_tables) == 40
    assert (
        next(
            field
            for field in SNAPSHOT.semantic_tables["fact_stockouts"].fields
            if field.name == "StoreID"
        ).data_type
        == "double"
    )
    assert {
        field.name for field in SNAPSHOT.historical_tables["fact_dc_inventory_txn"]
    } >= {"Source", "__index_level_0__"}
    required_ml = {
        contract.output.table
        for contract in MANIFEST.ml_contracts
        if contract.tier == "required"
    }
    for table_name, table in SNAPSHOT.semantic_tables.items():
        expected_schema = (
            "au"
            if table_name in SNAPSHOT.gold or table_name in required_ml
            else "ag"
        )
        assert (table.source_schema, table.source_table) == (
            expected_schema,
            table_name,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        (
            "source_asset_id",
            "asset.eventhouse",
            "streaming-notebook",
        ),
        (
            "target_asset_id",
            "asset.lakehouse",
            "semantic-model",
        ),
        (
            "contract_ids",
            [
                "data-contract.historical",
                "data-contract.semantic-model",
            ],
            "owning contract",
        ),
    ),
)
def test_event_path_rejects_contradictory_existing_ids(
    field: str,
    value: object,
    message: str,
) -> None:
    document = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    document["event_paths"][0][field] = value

    with pytest.raises(ValidationError, match=message):
        SolutionManifest.model_validate(document)


def test_event_path_mode_rejects_unknown_and_contradictory_values() -> None:
    document = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    document["event_paths"][0]["mode"] = "not-a-route"
    with pytest.raises(ValidationError):
        SolutionManifest.model_validate(document)

    document = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    document["event_paths"][0]["mode"] = "derived-last-touch-7d"
    with pytest.raises(ValidationError, match="requires a derived semantic route"):
        SolutionManifest.model_validate(document)


def test_event_path_rejects_existing_but_wrong_terminal_exception() -> None:
    document = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    path = next(
        item
        for item in document["event_paths"]
        if item["id"] == "event-path.online-order-picked"
    )
    path["terminal_exception_id"] = "exception.historical.no-live-event-route"

    with pytest.raises(ValidationError, match="streaming-only exception"):
        SolutionManifest.model_validate(document)


def test_event_contract_rejects_wrong_envelope_symbol_pointer() -> None:
    document = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    contract = next(
        item for item in document["data_contracts"] if item["kind"] == "event"
    )
    contract["envelope"]["source"] = next(
        source
        for source in contract["sources"]
        if source.get("selector", {}).get("value") == "EVENT_PAYLOADS"
    )

    with pytest.raises(ValidationError, match="ENVELOPE symbol"):
        SolutionManifest.model_validate(document)


def test_event_path_sources_are_not_pooled_across_paths() -> None:
    document = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    path = document["event_paths"][0]
    path["sources"][-1] = {
        "path": "fabric/kql_database/01-create-tables.kql",
        "selector": {"kind": "kql_create_merge_tables"},
    }
    manifest = SolutionManifest.model_validate(document)

    with pytest.raises(
        ManifestSourceError,
        match="sources must exactly cover its physical route",
    ):
        validate_manifest_repository(manifest, REPO_ROOT)


@pytest.mark.parametrize(
    ("binding", "replacement"),
    (
        ("entityName: demand_forecast", "entityName: wrong_table"),
        ("schemaName: au", "schemaName: wrong_schema"),
    ),
)
def test_tmdl_parser_rejects_executable_binding_drift(
    tmp_path: Path,
    binding: str,
    replacement: str,
) -> None:
    source_path = (
        REPO_ROOT
        / "fabric"
        / "powerbi"
        / "retail_model.SemanticModel"
        / "definition"
        / "tables"
        / "demand_forecast.tmdl"
    )
    definition = tmp_path / "definition"
    table_dir = definition / "tables"
    table_dir.mkdir(parents=True)
    (definition / "model.tmdl").write_text(
        "model Model\n\nref table demand_forecast\n",
        encoding="utf-8",
    )
    source = source_path.read_text(encoding="utf-8")
    assert source.count(binding) == 1
    (table_dir / source_path.name).write_text(
        source.replace(binding, replacement),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Direct Lake binding disagrees with sourceLineageTag",
    ):
        tmdl_active_table_schemas(definition / "model.tmdl")


def test_semantic_validation_rejects_wrong_expected_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tables = dict(SNAPSHOT.semantic_tables)
    tables["demand_forecast"] = replace(
        tables["demand_forecast"],
        source_schema="ag",
    )
    drifted = replace(SNAPSHOT, semantic_tables=tables)
    monkeypatch.setattr(
        data_validation,
        "derive_data_contract_snapshot",
        lambda *_: drifted,
    )

    with pytest.raises(
        ManifestSourceError,
        match="TMDL binding differs for 'demand_forecast'",
    ):
        validate_data_contracts(MANIFEST, REPO_ROOT)


def test_python_and_notebook_adapters_reject_executable_or_invalid_input(
    tmp_path: Path,
) -> None:
    python_path = tmp_path / "unsafe.py"
    python_path.write_text("VALUE = __import__('os').getcwd()\n", encoding="utf-8")
    with pytest.raises(ValueError):
        python_symbol(python_path, "VALUE")

    notebook_path = tmp_path / "invalid.ipynb"
    notebook_path.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "cell_type": "code",
                        "source": [1],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(TypeError):
        notebook_python_source(notebook_path)


def test_contract_check_command_is_non_mutating() -> None:
    authoritative = [
        MANIFEST_PATH,
        REPO_ROOT / "contracts" / "fixtures" / "event-scenarios.json",
        REPO_ROOT / "utility" / "notebooks" / "templates" / "driver-05-stream.py",
        REPO_ROOT / "fabric" / "kql_database" / "01-create-tables.kql",
        REPO_ROOT / "fabric" / "kql_database" / "02-create-ingestion-mappings.kql",
        REPO_ROOT / "utility" / "src" / "retail_setup" / "generation" / "schemas.py",
        REPO_ROOT / "fabric" / "lakehouse" / "03-streaming-to-silver.ipynb",
        REPO_ROOT / "fabric" / "lakehouse" / "04-streaming-to-gold.ipynb",
        REPO_ROOT
        / "fabric"
        / "powerbi"
        / "retail_model.SemanticModel"
        / "definition"
        / "model.tmdl",
    ]
    before = {path: _digest(path) for path in authoritative}
    env = {
        **os.environ,
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": os.pathsep.join(
            [
                str(REPO_ROOT / "utility" / "src"),
                str(REPO_ROOT),
                os.environ.get("PYTHONPATH", ""),
            ]
        ),
    }

    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "check_data_contracts.py")],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["events"] == 18
    assert report["ml_required_outputs"] == [
        "churn_predictions",
        "customer_segments",
        "demand_forecast",
        "stockout_risk",
    ]
    assert {path: _digest(path) for path in authoritative} == before


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
