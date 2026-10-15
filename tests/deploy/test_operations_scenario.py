from datetime import UTC, datetime, timedelta

import pytest

from deploy.scripts._operations_definition import (
    render_case_notebook,
    render_operations_definition,
)
from deploy.scripts._operations_scenario import (
    scenario_signals,
    signal_ingestion_query,
    validate_signal_readback,
)
from deploy.scripts.fabric_runtime import decode_definition_parts

NOW = datetime(2026, 9, 7, 12, tzinfo=UTC)
ID = "11111111-1111-4111-8111-111111111111"


def source_row(**changes):
    return {
        "store_id": 1,
        "product_id": 2,
        "product_name": 'Milk "A"\n',
        "source_as_of": NOW - timedelta(days=1),
        "forecast_as_of": NOW - timedelta(days=1),
        "forecast_days": 7,
        "forecast_rows": 7,
        "forecast_versions": 1,
        "current_inventory": 4,
        "forecast_units_7d": 10.1,
        "stockout_probability": 0.8,
        "risk_horizon_days": 3,
        "days_of_inventory": 2.0,
        **changes,
    }


def test_real_evidence_has_deterministic_case_and_shortage():
    first = scenario_signals([source_row()], NOW)[0]
    second = scenario_signals([source_row()], NOW + timedelta(hours=1))[0]
    assert first["case_id"] == second["case_id"]
    assert first["shortage_units"] == 7
    assert first["source_as_of"] != first["event_ts"]
    assert "unverified" in first["provenance"]


def test_last_inventory_movement_can_precede_forecast_anchor_without_relabeling():
    source = source_row(source_as_of=NOW - timedelta(days=3))
    signal = scenario_signals([source], NOW)[0]
    assert signal["source_as_of"] == source["source_as_of"].isoformat()
    assert signal["forecast_as_of"] == source["forecast_as_of"].isoformat()
    refreshed = scenario_signals(
        [source_row(**{**source, "forecast_as_of": NOW})], NOW
    )[0]
    assert refreshed["case_id"] != signal["case_id"]


@pytest.mark.parametrize(
    "changes",
    [
        {"current_inventory": None},
        {"stockout_probability": float("nan")},
        {"stockout_probability": 2},
        {"forecast_units_7d": -1},
        {"source_as_of": NOW + timedelta(seconds=1)},
        {"source_as_of": NOW - timedelta(days=8)},
        {"forecast_days": 6},
        {"forecast_rows": 8},
        {"forecast_versions": 2},
        {"forecast_as_of": NOW - timedelta(days=8)},
    ],
)
def test_invalid_or_stale_data_is_not_silently_seeded(changes):
    with pytest.raises(ValueError):
        scenario_signals([source_row(**changes)], NOW)


def test_duplicate_source_and_out_of_scope_batches_are_rejected():
    with pytest.raises(ValueError, match="unique"):
        scenario_signals([source_row(), source_row()], NOW)
    with pytest.raises(ValueError, match="five stores"):
        scenario_signals([source_row(store_id=i) for i in range(6)], NOW)


def test_kql_quotes_strings_and_keeps_nonempty_batch():
    query = signal_ingestion_query(scenario_signals([source_row()], NOW))
    assert 'Milk \\"A\\"\\n' in query
    assert query.startswith(".set-or-append ops_availability_signals")
    with pytest.raises(ValueError):
        signal_ingestion_query([])


def test_signal_readback_checks_values_not_just_existing_case_ids():
    signals = scenario_signals([source_row()], NOW)
    validate_signal_readback(signals, signals)
    changed = [{**signals[0], "shortage_units": 999}]
    with pytest.raises(ValueError, match="shortage_units"):
        validate_signal_readback(signals, changed)
    with pytest.raises(ValueError, match="identity"):
        validate_signal_readback(signals, [])


def test_native_definitions_are_bound_stopped_and_compile():
    import json

    definition = render_operations_definition(
        workspace_id=ID,
        kql_database_id=ID,
    )
    parts = decode_definition_parts(definition)
    agent = json.loads(parts["Configurations.json"])
    assert definition["format"] == "OperationsAgentV1"
    assert set(parts) == {"Configurations.json"}
    assert set(agent) == {"configuration", "shouldRun"}
    assert agent["shouldRun"] is False
    assert agent["configuration"]["actions"] == {}
    notebook = json.loads(
        decode_definition_parts(
            render_case_notebook(
                workspace_id=ID,
                lakehouse_id=ID,
                lakehouse_name="retail_lakehouse",
                kusto_uri="https://example.kusto.fabric.microsoft.com",
                database_name="retail",
            )
        )["notebook-content.ipynb"]
    )
    assert notebook["metadata"]["dependencies"]["lakehouse"]["default_lakehouse"] == ID
    for cell in notebook["cells"]:
        if cell["cell_type"] == "code":
            compile("".join(cell["source"]), "<case notebook>", "exec")


def test_operations_only_handoff_never_requires_native_planning():
    import json

    definition = render_operations_definition(
        workspace_id=ID, kql_database_id=ID, include_plan=False,
    )
    agent = json.loads(decode_definition_parts(definition)["Configurations.json"])
    instructions = agent["configuration"]["instructions"]
    assert "through SSO" in instructions
    assert "retail-replenishment" not in instructions
    assert "{{PLANNING_HANDOFF}}" not in instructions
    notebook = render_case_notebook(
        workspace_id=ID, lakehouse_id=ID, lakehouse_name="retail_lakehouse",
        kusto_uri="https://example.kusto.fabric.microsoft.com", database_name="retail",
        include_plan=False,
    )
    assert "retail-replenishment" not in decode_definition_parts(notebook)["notebook-content.ipynb"].decode()
