from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from deploy.scripts._operations_definition import (
    render_case_notebook,
    render_operations_definition,
)
from deploy.scripts._operations_scenario import (
    HIGH_STOCKOUT_PROBABILITY,
    LOW_DAYS_OF_INVENTORY,
    MAX_SIGNALS,
    PLAN_PRODUCT_IDS,
    current_alert_cases,
    meets_alert_rule,
    require_alertable,
    scenario_signals,
    scenario_sql,
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


def test_inventory_snapshot_can_precede_forecast_anchor_without_relabeling():
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


def test_every_case_carries_the_snapshot_as_of_not_its_last_ledger_change():
    # A stocked-out SKU can be unchanged for over a week while the snapshot is
    # current. It must not be stamped with its last movement, or the agent
    # query and the case notebook would drop exactly that SKU as stale.
    slow = source_row(product_id=17, source_as_of=NOW - timedelta(days=11))
    signals = scenario_signals([source_row(), slow], NOW)
    snapshot = source_row()["source_as_of"].isoformat()
    assert [signal["source_as_of"] for signal in signals] == [snapshot, snapshot]
    assert current_alert_cases(signals, [], NOW) == signals
    with pytest.raises(ValueError, match="snapshot is stale"):
        scenario_signals([slow], NOW)
    assert "risk.predicted_at AS source_as_of" in scenario_sql(plan_cohort=False)


def test_duplicate_source_and_out_of_scope_batches_are_rejected():
    with pytest.raises(ValueError, match="unique"):
        scenario_signals([source_row(), source_row()], NOW)
    with pytest.raises(ValueError, match=f"1-{MAX_SIGNALS}"):
        scenario_signals(
            [source_row(product_id=i) for i in range(1, MAX_SIGNALS + 2)], NOW
        )
    with pytest.raises(ValueError, match="positive"):
        scenario_signals([source_row(store_id=0)], NOW)


def test_any_network_position_can_be_seeded_without_a_plan():
    # Risk, not a fixed ID range, selects which positions the agent watches.
    signal = scenario_signals([source_row(store_id=12, product_id=1828)], NOW)[0]
    assert (signal["store_id"], signal["product_id"]) == (12, 1828)
    assert "BETWEEN 1 AND" not in scenario_sql(plan_cohort=False)


def test_plan_reviewed_signals_stay_inside_the_locked_plan_cohort():
    outside = source_row(product_id=PLAN_PRODUCT_IDS.stop)
    with pytest.raises(ValueError, match="Plan cohort"):
        scenario_signals([outside], NOW, plan_cohort=True)
    assert scenario_signals([source_row()], NOW, plan_cohort=True)
    query = scenario_sql(plan_cohort=True)
    assert "risk.store_id BETWEEN 1 AND 5" in query
    assert "risk.product_id BETWEEN 1 AND 20" in query


def test_watchlist_ranks_positions_that_meet_the_agent_rules_first():
    instructions = (
        Path(__file__).resolve().parents[2]
        / "fabric"
        / "operations"
        / "availability.instructions.md"
    ).read_text(encoding="utf-8")
    assert f"stockout_probability >= {HIGH_STOCKOUT_PROBABILITY}" in instructions
    assert f"days_of_inventory < {LOW_DAYS_OF_INVENTORY}" in instructions
    for plan_cohort in (False, True):
        query = scenario_sql(plan_cohort=plan_cohort)
        ranking = query.split("ORDER BY")[-1]
        assert f"stockout_probability >= {HIGH_STOCKOUT_PROBABILITY}" in ranking
        assert f"days_of_inventory < {LOW_DAYS_OF_INVENTORY}" in ranking
        assert f"TOP ({MAX_SIGNALS})" in query


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
    content = decode_definition_parts(notebook)["notebook-content.ipynb"].decode()
    assert "retail-replenishment" not in content
    # Without a Plan the case notebook must not enforce the Plan's locked cohort.
    assert "PLAN_COHORT = False" in content


def test_watchlist_without_an_alertable_case_is_refused():
    quiet = source_row(stockout_probability=0.2, days_of_inventory=9.0)
    signals = scenario_signals([quiet], NOW)
    assert not meets_alert_rule(signals[0])
    with pytest.raises(ValueError, match="never alert"):
        require_alertable(signals)
    assert require_alertable(scenario_signals([quiet, source_row(product_id=3)], NOW)) == 1


def test_signals_record_the_cohort_mode_for_the_scheduled_refresh():
    assert scenario_signals([source_row()], NOW)[0]["plan_cohort"] is False
    assert scenario_signals([source_row()], NOW, plan_cohort=True)[0]["plan_cohort"] is True
    query = signal_ingestion_query(scenario_signals([source_row()], NOW))
    assert "plan_cohort:bool" in query


def test_agent_query_suppresses_acted_and_superseded_cases():
    first = scenario_signals([source_row(), source_row(product_id=3)], NOW)
    refreshed = scenario_signals(
        [source_row(forecast_as_of=NOW - timedelta(hours=2))], NOW + timedelta(hours=1)
    )
    signals = first + refreshed
    later = NOW + timedelta(hours=2)
    cases = current_alert_cases(signals, [], later)
    # The refreshed case supersedes the first case for store 1 product 2.
    assert {case["case_id"] for case in cases} == {
        refreshed[0]["case_id"], first[1]["case_id"]
    }
    actions = [
        {"case_id": refreshed[0]["case_id"], "status": "DRAFT"},
        {"case_id": first[1]["case_id"], "status": "REJECTED"},
    ]
    assert [case["case_id"] for case in current_alert_cases(signals, actions, later)] == [
        first[1]["case_id"]
    ]
    stale = NOW + timedelta(days=8)
    assert current_alert_cases(signals, [], stale) == []


def test_canonical_agent_query_expresses_the_reference_semantics():
    instructions = (
        Path(__file__).resolve().parents[2]
        / "fabric"
        / "operations"
        / "availability.instructions.md"
    ).read_text(encoding="utf-8")
    query = instructions.split("```kusto", 1)[1].split("```", 1)[0]
    steps = [line.strip() for line in query.strip().splitlines()]
    assert steps[0].startswith("let acted_cases = ops_case_actions")
    assert '| where status in ("DRAFT", "APPROVED", "RESOLVED")' in steps
    assert "| summarize arg_max(event_ts, *) by store_id, product_id" in steps
    assert "| join kind=leftanti acted_cases on case_id" in steps
    # Superseded cases are collapsed before the action filter, so acting on the
    # newest case for a position silences that position.
    assert steps.index("| summarize arg_max(event_ts, *) by store_id, product_id") < steps.index(
        "| join kind=leftanti acted_cases on case_id"
    )
    assert "by case_id" not in query
