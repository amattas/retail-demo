from __future__ import annotations

import ast
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
TABLES_DIR = (
    REPO_ROOT
    / "fabric"
    / "powerbi"
    / "retail_model.SemanticModel"
    / "definition"
    / "tables"
)
SCHEMAS_PATH = REPO_ROOT / "utility" / "src" / "retail_setup" / "generation" / "schemas.py"
METRICS_PATH = REPO_ROOT / "fabric" / "twins" / "metrics.json"

# These tests are static TMDL/source contract tests and deterministic Python
# oracles for the intended semantics. They do not execute DAX; a live model
# engine is required for DAX evaluation and is intentionally not called here.
DAX_ENGINE_EXECUTED = False

EXPECTED_METRICS: dict[str, list[tuple[str, str]]] = {
    "dim_stores": [
        ("Twin Store State", "String"),
        ("Twin Store State As Of", "DateTime"),
        ("Twin Store Signed Stock", "Int64"),
        ("Twin Store Available Stock", "Int64"),
        ("Twin Store Inventory As Of", "DateTime"),
        ("Twin Store Stockout Risk", "Double"),
        ("Twin Store Zone Activity", "Int64"),
    ],
    "dim_distribution_centers": [
        ("Twin Warehouse Signed Stock", "Int64"),
        ("Twin Warehouse Available Stock", "Int64"),
        ("Twin Warehouse Inventory As Of", "DateTime"),
        ("Twin Warehouse Shipment Count", "Int64"),
    ],
    "dim_trucks": [
        ("Twin Truck Last Status", "String"),
        ("Twin Truck State As Of", "DateTime"),
        ("Twin Truck Last Shipment", "String"),
        ("Twin Truck Last Store", "Int64"),
        ("Twin Truck Last DC", "Int64"),
        ("Twin Truck Recorded ETA", "DateTime"),
        ("Twin Truck Recorded ETD", "DateTime"),
        ("Twin Truck Cargo Balance", "Int64"),
        ("Twin Truck Cargo As Of", "DateTime"),
        ("Twin Truck Delay Scenario ETA", "DateTime"),
        ("Twin Truck Delay Minutes", "Int64"),
    ],
}

REQUIRED_SOURCE_BINDINGS = {
    "fact_store_ops": {"Event Timestamp": "event_ts"},
    "fact_foot_traffic": {"Event Timestamp": "event_ts"},
    "inventory_position_current": {"As Of": "as_of"},
    "dc_inventory_position_current": {"As Of": "as_of"},
    "fact_truck_moves": {
        "Event Timestamp": "event_ts",
        "Recorded ETA": "eta",
        "Recorded ETD": "etd",
        "Departure Time": "departure_time",
    },
    "fact_truck_inventory": {"Event Timestamp": "event_ts"},
}


def _table_text(table_name: str) -> str:
    return (TABLES_DIR / f"{table_name}.tmdl").read_text(encoding="utf-8")


def _measure_block(table_name: str, measure_name: str) -> str:
    match = re.search(
        rf"^\tmeasure '{re.escape(measure_name)}' = "
        r"(?P<body>.*?)(?=^\t(?:measure|column|partition) |\Z)",
        _table_text(table_name),
        re.MULTILINE | re.DOTALL,
    )
    assert match, f"Missing measure {table_name}[{measure_name}]"
    return match.group("body")


def _column_block(table_name: str, column_name: str) -> str:
    match = re.search(
        rf"^\tcolumn '{re.escape(column_name)}'\n"
        r"(?P<body>.*?)(?=^\t(?:column|partition) |\Z)",
        _table_text(table_name),
        re.MULTILINE | re.DOTALL,
    )
    assert match, f"Missing column {table_name}[{column_name}]"
    return match.group("body")


def _schema_tables() -> dict[str, dict[str, str]]:
    tree = ast.parse(SCHEMAS_PATH.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == "TABLES":
            value = ast.literal_eval(node.value)
            return {table: dict(columns) for table, columns in value.items()}
    raise AssertionError("TABLES assignment not found in schemas.py")


def _metrics() -> list[dict[str, str]]:
    return json.loads(METRICS_PATH.read_text(encoding="utf-8"))["metrics"]


def test_unknown_balances_are_not_mapped_to_available_zero():
    for table, name in (
        ("dim_stores", "Twin Store Available Stock"),
        ("dim_distribution_centers", "Twin Warehouse Available Stock"),
    ):
        assert "IF(ISBLANK(_KnownBalance), BLANK()," in _measure_block(table, name)


def test_route_filters_intersect_existing_fact_selection():
    for table in ("dim_trucks", "dim_distribution_centers"):
        lines = [line.strip() for line in _table_text(table).splitlines() if "TREATAS(" in line]
        assert lines
        assert all(line.startswith("KEEPFILTERS(TREATAS(") for line in lines)


def test_unknown_or_invalid_cargo_is_not_summed_as_known_stock():
    block = _measure_block("dim_trucks", "Twin Truck Cargo Balance")
    assert "_InvalidCargoRows > 0, BLANK()" in block
    assert "ISBLANK('fact_truck_inventory'[Quantity])" in block
    assert "'fact_truck_inventory'[Quantity] < 0" in block
    before_validation = block.split("VAR _InvalidCargoRows", 1)[0]
    assert before_validation.count("(") == before_validation.count(")")


def _latest_single_row(
    rows: list[dict[str, Any]],
    observation_cutoff: datetime | None = None,
) -> dict[str, Any] | None:
    observable_rows = [
        row
        for row in rows
        if observation_cutoff is None or row["event_ts"] <= observation_cutoff
    ]
    if not observable_rows:
        return None
    latest_ts = max(row["event_ts"] for row in observable_rows)
    latest_rows = [row for row in observable_rows if row["event_ts"] == latest_ts]
    if len(latest_rows) != 1:
        return None
    return latest_rows[0]


def _cargo_balance(
    rows: list[dict[str, Any]],
    observation_cutoff: datetime | None = None,
) -> int | None:
    observable_rows = [
        row
        for row in rows
        if observation_cutoff is None or row["event_ts"] <= observation_cutoff
    ]
    if not observable_rows:
        return None
    total = 0
    observed = False
    for row in observable_rows:
        if row["action"] == "LOAD":
            total += row["quantity"]
            observed = True
        elif row["action"] == "UNLOAD":
            total -= row["quantity"]
            observed = True
    return total if observed else None


def _conservative_as_of(rows: list[dict[str, Any]]) -> datetime | None:
    timestamps = [row["as_of"] for row in rows if row.get("as_of") is not None]
    return min(timestamps) if timestamps else None


def _source_anchored_shipment_count(
    rows: list[dict[str, Any]],
    observation_cutoff: datetime | None = None,
) -> int | None:
    observable_rows = [
        row
        for row in rows
        if observation_cutoff is None or row["event_ts"] <= observation_cutoff
    ]
    if not observable_rows:
        return None
    source_anchor = max(row["event_ts"] for row in observable_rows)
    window_start = source_anchor - timedelta(days=14)
    return len(
        {
            row["shipment_id"]
            for row in observable_rows
            if row.get("shipment_id")
            and window_start < row["event_ts"] <= source_anchor
        }
    )


def test_static_tests_do_not_claim_to_execute_dax() -> None:
    assert DAX_ENGINE_EXECUTED is False


def test_metrics_json_publishes_exact_interface() -> None:
    metrics = _metrics()
    assert isinstance(metrics, list)

    expected_pairs = {
        (table, name): data_type
        for table, entries in EXPECTED_METRICS.items()
        for name, data_type in entries
    }
    actual_pairs = {
        (metric["table"], metric["name"]): metric["data_type"]
        for metric in metrics
    }

    assert actual_pairs == expected_pairs
    for metric in metrics:
        assert metric["description"].strip()
        assert metric["table"] in EXPECTED_METRICS
        assert _measure_block(metric["table"], metric["name"])


def test_required_source_only_columns_are_bound_and_schema_backed() -> None:
    schema = _schema_tables()
    for table_name, columns in REQUIRED_SOURCE_BINDINGS.items():
        assert table_name in schema
        for model_column, source_column in columns.items():
            block = _column_block(table_name, model_column)
            assert f"sourceColumn: {source_column}" in block
            assert "dataType: dateTime" in block
            assert "isHidden" in block
            assert source_column in schema[table_name]
            assert schema[table_name][source_column] in {"timestamp", "date"}


def test_store_state_requires_one_store_and_non_tied_latest_row() -> None:
    state = _measure_block("dim_stores", "Twin Store State")
    as_of = _measure_block("dim_stores", "Twin Store State As Of")

    assert "SELECTEDVALUE('dim_stores'[ID])" in state
    assert "SELECTEDVALUE('dim_stores'[ID])" in as_of
    assert "MAX('fact_store_ops'[Event Timestamp])" in as_of
    assert "UTCNOW()" in as_of
    assert "KEEPFILTERS('fact_store_ops'[Event Timestamp] <= _ObservationCutoff)" in as_of
    assert "KEEPFILTERS('fact_store_ops'[Event Timestamp] = _AsOf)" in state
    assert "COUNTROWS(_LatestRows) = 1" in state
    assert 'ISBLANK(_AsOf), "not observed"' in state

    rows = [
        {"event_ts": datetime(2026, 1, 1, 6), "state": "opened"},
        {"event_ts": datetime(2026, 1, 1, 22), "state": "closed"},
    ]
    assert _latest_single_row(rows)["state"] == "closed"
    assert (
        _latest_single_row(
            [
                {"event_ts": datetime(2026, 1, 1, 6), "state": "opened"},
                {"event_ts": datetime(2026, 1, 2, 6), "state": "future_opened"},
            ],
            observation_cutoff=datetime(2026, 1, 1, 23),
        )["state"]
        == "opened"
    )
    assert _latest_single_row([]) is None
    assert (
        _latest_single_row(
            [
                {"event_ts": datetime(2026, 1, 1, 22), "state": "closed"},
                {"event_ts": datetime(2026, 1, 1, 22), "state": "opened"},
            ]
        )
        is None
    )


def test_inventory_measures_preserve_known_balances_and_conservative_as_of() -> None:
    for table, prefix, fact_table in [
        ("dim_stores", "Twin Store", "inventory_position_current"),
        ("dim_distribution_centers", "Twin Warehouse", "dc_inventory_position_current"),
    ]:
        signed = _measure_block(table, f"{prefix} Signed Stock")
        available = _measure_block(table, f"{prefix} Available Stock")
        as_of = _measure_block(table, f"{prefix} Inventory As Of")

        assert f"SUMMARIZE(\n\t\t\t\t\t\t'{fact_table}'" in signed
        assert f"CALCULATE(SUM('{fact_table}'[On Hand]))" in signed
        assert "ISEMPTY(_KnownBalances)" in available
        assert "IF(_KnownBalance > 0, _KnownBalance, 0)" in available
        assert "MINX(_KnownPositions" in as_of
        assert f"'{fact_table}'[As Of]" in as_of

    positions = [
        {"sku": 1, "balance": -3, "as_of": datetime(2026, 2, 1, 10)},
        {"sku": 2, "balance": 8, "as_of": datetime(2026, 2, 1, 12)},
    ]
    assert sum(row["balance"] for row in positions) == 5
    assert sum(max(0, row["balance"]) for row in positions) == 8
    assert _conservative_as_of(positions) == datetime(2026, 2, 1, 10)
    assert _conservative_as_of([]) is None


def test_stockout_risk_and_zone_activity_are_observed_source_metrics() -> None:
    risk = _measure_block("dim_stores", "Twin Store Stockout Risk")
    activity = _measure_block("dim_stores", "Twin Store Zone Activity")

    assert "MAX('stockout_risk'[Stockout Probability])" in risk
    assert "Forecast Horizon Days" not in risk
    assert "SUM('fact_foot_traffic'[Count])" in activity
    assert "DISTINCTCOUNT" not in activity


def test_warehouse_shipment_count_uses_utc_capped_source_anchored_last_14_days() -> None:
    block = _measure_block("dim_distribution_centers", "Twin Warehouse Shipment Count")

    assert "MAX('fact_truck_moves'[Event Timestamp])" in block
    assert "UTCNOW()" in block
    assert "KEEPFILTERS('fact_truck_moves'[Event Timestamp] <= _ObservationCutoff)" in block
    assert "_SourceAnchor - 14" in block
    assert "DISTINCTCOUNT('fact_truck_moves'[Shipment ID])" in block
    assert "TREATAS(_VisibleStoreIds, 'fact_truck_moves'[Store ID])" in block
    assert "TREATAS(_VisibleDCIds, 'fact_truck_moves'[DC ID])" in block
    assert "KEEPFILTERS('fact_truck_moves'[Event Timestamp] > _WindowStart)" in block
    assert "TODAY(" not in block

    rows = [
        {"event_ts": datetime(2026, 2, 20), "shipment_id": "S1"},
        {"event_ts": datetime(2026, 3, 1), "shipment_id": "S2"},
        {"event_ts": datetime(2026, 3, 5), "shipment_id": "S2"},
        {"event_ts": datetime(2026, 3, 6), "shipment_id": "S3"},
    ]
    assert _source_anchored_shipment_count(rows) == 2
    assert (
        _source_anchored_shipment_count(
            rows + [{"event_ts": datetime(2026, 3, 20), "shipment_id": "FUTURE"}],
            observation_cutoff=datetime(2026, 3, 6),
        )
        == 2
    )
    assert _source_anchored_shipment_count([]) is None


def test_truck_latest_identity_measures_use_utc_capped_observed_rows() -> None:
    for measure_name in [
        "Twin Truck Last Status",
        "Twin Truck Last Shipment",
        "Twin Truck Last Store",
        "Twin Truck Last DC",
        "Twin Truck Recorded ETA",
        "Twin Truck Recorded ETD",
    ]:
        block = _measure_block("dim_trucks", measure_name)
        assert "SELECTEDVALUE('dim_trucks'[ID])" in block
        assert "COUNTROWS(_LatestRows) = 1" in block
        assert "KEEPFILTERS('fact_truck_moves'[Event Timestamp] = _AsOf)" in block
        assert "TREATAS(_VisibleStoreIds, 'fact_truck_moves'[Store ID])" in block
        assert "TREATAS(_VisibleDCIds, 'fact_truck_moves'[DC ID])" in block

    state_as_of = _measure_block("dim_trucks", "Twin Truck State As Of")
    assert "UTCNOW()" in state_as_of
    assert (
        "KEEPFILTERS('fact_truck_moves'[Event Timestamp] <= _ObservationCutoff)"
        in state_as_of
    )

    rows = [
        {"event_ts": datetime(2026, 3, 1, 8), "status": "LOADING"},
        {"event_ts": datetime(2026, 3, 1, 10), "status": "IN_TRANSIT"},
    ]
    assert _latest_single_row(rows)["status"] == "IN_TRANSIT"
    assert (
        _latest_single_row(
            rows + [{"event_ts": datetime(2026, 3, 2), "status": "FUTURE"}],
            observation_cutoff=datetime(2026, 3, 1, 23),
        )["status"]
        == "IN_TRANSIT"
    )
    assert (
        _latest_single_row(
            [
                {"event_ts": datetime(2026, 3, 1, 10), "status": "ARRIVED"},
                {"event_ts": datetime(2026, 3, 1, 10), "status": "UNLOADING"},
            ]
        )
        is None
    )


def test_truck_cargo_uses_source_load_unload_semantics_and_latest_shipment() -> None:
    cargo = _measure_block("dim_trucks", "Twin Truck Cargo Balance")
    cargo_as_of = _measure_block("dim_trucks", "Twin Truck Cargo As Of")
    last_shipment = _measure_block("dim_trucks", "Twin Truck Last Shipment")

    assert "[Twin Truck Last Shipment]" in cargo
    assert "'fact_truck_inventory'[Truck ID] = _SelectedTruckId" in cargo
    assert "'fact_truck_inventory'[Shipment ID] = _LastShipment" in cargo
    assert "UTCNOW()" in cargo
    assert "UTCNOW()" in cargo_as_of
    assert (
        "KEEPFILTERS('fact_truck_inventory'[Event Timestamp] <= _ObservationCutoff)"
        in cargo
    )
    assert (
        "KEEPFILTERS('fact_truck_inventory'[Event Timestamp] <= _ObservationCutoff)"
        in cargo_as_of
    )
    assert "'fact_truck_inventory'[Action] = \"LOAD\", 'fact_truck_inventory'[Quantity]" in cargo
    assert "'fact_truck_inventory'[Action] = \"UNLOAD\", -'fact_truck_inventory'[Quantity]" in cargo
    assert "MAXX(_CargoEvents, 'fact_truck_inventory'[Event Timestamp])" in cargo_as_of
    assert "'fact_truck_inventory'[Product ID]" not in last_shipment

    source_rows = [
        {"action": "LOAD", "quantity": 10},
        {"action": "UNLOAD", "quantity": 4},
    ]
    assert _cargo_balance(source_rows) == 6
    assert _cargo_balance([{"action": "UNLOAD", "quantity": 10}]) == -10
    assert (
        _cargo_balance(
            [
                {"event_ts": datetime(2026, 3, 1, 8), "action": "LOAD", "quantity": 10},
                {"event_ts": datetime(2026, 3, 2, 8), "action": "UNLOAD", "quantity": 10},
            ],
            observation_cutoff=datetime(2026, 3, 1, 23),
        )
        == 10
    )
    assert _cargo_balance([]) is None


def test_truck_delay_scenario_is_what_if_only_with_source_eta() -> None:
    scenario = _measure_block("dim_trucks", "Twin Truck Delay Scenario ETA")
    minutes = _measure_block("dim_trucks", "Twin Truck Delay Minutes")

    assert "[Twin Truck Recorded ETA]" in scenario
    assert "_RecordedETA + TIME(1, 0, 0)" in scenario
    assert "[Twin Truck Recorded ETA]" in minutes
    assert "ISBLANK(_RecordedETA)" in minutes
    assert "60" in minutes


def test_twin_changes_do_not_add_native_planning_or_operations_dependencies() -> None:
    owned_text = "\n".join(
        [
            _table_text("dim_stores"),
            _table_text("dim_distribution_centers"),
            _table_text("dim_trucks"),
            METRICS_PATH.read_text(encoding="utf-8"),
        ]
    )
    assert "Planning" not in owned_text
    assert "OperationsAgent" not in owned_text
    assert "Connection" not in owned_text
    assert "credential" not in owned_text.lower()
