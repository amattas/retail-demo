"""Calibrated thresholds: consumers must carry the calibration constants."""

from __future__ import annotations

import ast
import json
import re
from datetime import date, datetime
from pathlib import Path

import pytest

from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation import calibration as cal
from retail_setup.generation import vocabulary as vocab
from retail_setup.generation.dims import VOLUME_CLASSES
from retail_setup.generation.stream_calendar import (
    VISIT_CONVERSION_CALIBRATION,
    WEATHER_FACTOR_RANGE,
    holiday_factor,
    store_class,
    store_visits_per_hour,
)

REPO = Path(__file__).resolve().parents[3]
RULES = (REPO / "fabric" / "rules" / "definitions.kql").read_text(encoding="utf-8")
ANOMALY_KQL = (REPO / "fabric" / "kql_database" / "06-ml-anomaly-detection.kql").read_text(
    encoding="utf-8"
)
TRUCK_DWELL_TMDL = (
    REPO
    / "fabric"
    / "powerbi"
    / "retail_model.SemanticModel"
    / "definition"
    / "tables"
    / "truck_dwell_daily.tmdl"
).read_text(encoding="utf-8")
KQL_FUNCTIONS = (REPO / "fabric" / "kql_database" / "03-create-functions.kql").read_text(
    encoding="utf-8"
)
KQL_VIEWS = (REPO / "fabric" / "kql_database" / "04-create-materialized-views.kql").read_text(
    encoding="utf-8"
)
TRUCK_MOVES_TMDL = (
    REPO
    / "fabric"
    / "powerbi"
    / "retail_model.SemanticModel"
    / "definition"
    / "tables"
    / "fact_truck_moves.tmdl"
).read_text(encoding="utf-8")
DELIVERY_NOTEBOOK = (REPO / "fabric" / "lakehouse" / "13-ml-delivery-prediction.ipynb").read_text(
    encoding="utf-8"
)
DASHBOARD = (REPO / "fabric" / "dashboards" / "retail-ops.template.json").read_text(
    encoding="utf-8"
)
STREAM_TEMPLATE = (REPO / "utility" / "notebooks" / "templates" / "driver-05-stream.py").read_text(
    encoding="utf-8"
)
STREAM_EVENTS = (
    REPO / "utility" / "src" / "retail_setup" / "generation" / "stream_events.py"
).read_text(encoding="utf-8")
STREAM_SUPPLY = (
    REPO / "utility" / "src" / "retail_setup" / "generation" / "stream_supply.py"
).read_text(encoding="utf-8")
# Delivery timing and the unload model the stream shares with history.
SHARED_DELIVERY = "\n".join(
    (REPO / "utility" / "src" / "retail_setup" / "generation" / name).read_text(encoding="utf-8")
    for name in ("network.py", "inventory_policy.py")
)


def _rule(title_prefix: str) -> str:
    """One rule query from definitions.kql: its comment header to the next blank line."""
    blocks = re.split(r"\n\s*\n", RULES)
    matches = [block for block in blocks if block.lstrip().startswith(f"// {title_prefix}")]
    assert len(matches) == 1, f"expected one rule titled {title_prefix!r}"
    return matches[0]


def _window_minutes(rule: str) -> int:
    windows = {int(m) for m in re.findall(r"ago\((\d+)m\)", rule)}
    assert len(windows) == 1, rule
    return windows.pop()


def _kql_object(source: str, name: str) -> str:
    """Body of the KQL function or materialized view ``name``."""
    match = re.search(rf"\b{name}\b[^\n]*\n\{{\n(.*?)\n\}}\n", source, re.DOTALL)
    assert match is not None, name
    return match.group(1)


def test_dashboard_late_truck_and_incident_tiles_read_calibrated_sources() -> None:
    tiles = {tile["title"]: tile["query"] for tile in json.loads(DASHBOARD)["tiles"]}
    # Late trucks come from the hourly delay view, which counts them at the
    # calibrated threshold; no tile repeats a delay or dwell literal of its own.
    late = tiles["Late Trucks by Lane (24h)"]
    assert late.startswith("mv_truck_delay_hourly")
    assert "late_trucks=sum(late_trucks)" in late
    for query in tiles.values():
        assert not re.search(r"delay_minutes\s*>=?\s*\d", query), query
        assert not re.search(r"dwell_minutes\s*>=?\s*\d", query), query
    # Incident tiles read the open-incident functions.
    assert tiles["Active Logistics Incidents"].startswith("fn_active_incidents()")
    assert tiles["Stores Impacted by Open Incidents"].startswith("fn_incident_impacted_stores()")
    active = _kql_object(KQL_FUNCTIONS, "fn_active_incidents")
    assert "join kind=leftanti" in active and "where started_at <= now()" in active
    assert "fn_active_incidents()" in _kql_object(KQL_FUNCTIONS, "fn_incident_impacted_stores")


def test_truck_dwell_sla_is_one_value_everywhere() -> None:
    rule = _rule("Truck dwell breach")
    match = re.search(r"let threshold_minutes = ([0-9.]+);", rule)
    assert match is not None
    assert float(match.group(1)) == cal.TRUCK_DWELL_SLA_MINUTES
    assert f"{cal.TRUCK_DWELL_SLA_MINUTES}-minute SLA" in rule
    assert _window_minutes(rule) == cal.TRUCK_DWELL_RULE_WINDOW_MINUTES
    # The rule follows Eventhouse ingestion, not the payload's own timestamp.
    assert "where departure_ingested_at > ago(" in rule
    assert "departure_ingested_at = ingestion_time()" in KQL_FUNCTIONS

    measure = re.search(
        r"measure 'Truck Dwell Breach Days' = .*\[Average Dwell Minutes\] > ([0-9.]+)\)",
        TRUCK_DWELL_TMDL,
    )
    assert measure is not None
    assert float(measure.group(1)) == cal.TRUCK_DWELL_SLA_MINUTES

    # The live unload model keeps normal loads under the SLA: an empty truck
    # dwells the base time, and a load up to the normal maximum stays inside
    # the normal range at the slowest dock with the most noise.
    assert (
        0
        < cal.TRUCK_NORMAL_DWELL_MINUTES_MIN
        <= cal.TRUCK_UNLOAD_BASE_MINUTES + cal.TRUCK_UNLOAD_NOISE_MINUTES
        < cal.TRUCK_NORMAL_DWELL_MINUTES_MAX
        < cal.TRUCK_DWELL_SLA_MINUTES
    )
    low, high = cal.TRUCK_DOCK_FACTOR_RANGE
    assert 0 < low <= 1 <= high


def test_inventory_rules_use_calibrated_windows() -> None:
    stockout = _rule("Stockout detected")
    assert _window_minutes(stockout) == cal.STOCKOUT_RULE_WINDOW_MINUTES
    urgent = _rule("Reorder URGENT")
    assert "priority == 'URGENT'" in urgent
    assert _window_minutes(urgent) == cal.URGENT_REORDER_RULE_WINDOW_MINUTES
    # Deduped per event and one alert per store (or DC) and window.
    for rule in (stockout, urgent):
        assert "by trace_id" in rule and "by store_id, dc_id" in rule
        assert "dcount(product_id)" in rule
    # A stockout is a store's balance reaching zero, as in history's fact_stockouts.
    assert cal.STOCKOUT_QUANTITY_THRESHOLD == 1


def test_logistics_rules_use_calibrated_thresholds() -> None:
    late = _rule("Late truck")
    assert _window_minutes(late) == cal.TRUCK_LATE_RULE_WINDOW_MINUTES
    match = re.search(r"where delay_minutes >= (\d+)", late)
    assert match is not None and int(match.group(1)) == cal.TRUCK_LATE_MINUTES

    lane = _rule("Lane delay over threshold")
    assert _window_minutes(lane) == cal.LANE_DELAY_RULE_WINDOW_MINUTES
    match = re.search(r"where avg_delay_minutes >= ([0-9.]+)", lane)
    assert match is not None and float(match.group(1)) == cal.LANE_DELAY_RULE_MINUTES
    assert "by dc_id, store_id" in lane

    outage = _rule("DC outage started")
    assert _window_minutes(outage) == cal.INCIDENT_RULE_WINDOW_MINUTES
    assert "incident_type == 'DC_OUTAGE'" in outage and "by incident_id" in outage

    stockouts = _rule("Incident stockouts")
    assert _window_minutes(stockouts) == cal.INCIDENT_STOCKOUT_RULE_WINDOW_MINUTES
    match = re.search(r"where stockouts >= (\d+)", stockouts)
    assert match is not None and int(match.group(1)) == cal.INCIDENT_STOCKOUT_RULE_MIN_STOCKOUTS
    # Deduped per stockout event and aggregated per incident.
    assert "dcount(trace_id)" in stockouts and "by incident_id" in stockouts


def test_late_truck_threshold_is_one_value_everywhere() -> None:
    # KQL lane summaries and the hourly delay view count late trucks alike.
    for source in (KQL_FUNCTIONS, KQL_VIEWS):
        literals = {int(v) for v in re.findall(r"countif\(delay_minutes >= (\d+)\)", source)}
        assert literals == {cal.TRUCK_LATE_MINUTES}
    late = f"countif(delay_minutes >= {cal.TRUCK_LATE_MINUTES})"
    assert late in _kql_object(KQL_VIEWS, "mv_truck_delay_hourly")
    assert late in _kql_object(KQL_FUNCTIONS, "fn_lane_delays")
    measure = re.search(
        r"measure 'Late Truck Arrivals' = .*\[Delay Minutes\] >= (\d+)\)", TRUCK_MOVES_TMDL
    )
    assert measure is not None and int(measure.group(1)) == cal.TRUCK_LATE_MINUTES
    match = re.search(r"TRUCK_LATE_MINUTES = (\d+)", DELIVERY_NOTEBOOK)
    assert match is not None and int(match.group(1)) == cal.TRUCK_LATE_MINUTES


def _rule_thresholds_function() -> dict[str, float]:
    """Literal values of fn_rule_thresholds() in 03-create-functions.kql."""
    body = re.search(r"fn_rule_thresholds\(\)\s*\{([^{}]*)\}", KQL_FUNCTIONS)
    assert body is not None
    values = re.findall(r"(\w+) = (?:real|long)\(([0-9.]+)\)", body.group(1))
    return {name: float(value) for name, value in values}


def test_rule_threshold_function_carries_the_reference_store_type() -> None:
    profile = load_dictionaries(default_dictionary_root(), cal.REFERENCE_STORE_TYPE).profile
    expected = cal.rule_thresholds(profile, profile.resolved_transactions_per_store_day())
    assert _rule_thresholds_function() == {name: float(v) for name, v in expected.items()}


def test_zone_surge_rule_uses_calibrated_thresholds() -> None:
    rule = _rule("Zone arrival surge")
    assert _window_minutes(rule) == cal.ZONE_SURGE_WINDOW_MINUTES
    assert "toscalar(fn_rule_thresholds() | project zone_surge_arrivals)" in rule
    assert "where arrivals > surge_arrivals" in rule
    assert "by trace_id" in rule and "by store_id, zone" in rule
    # Expected dwell never reaches a long-dwell bound: the longest zone dwell
    # range ends well under ten minutes, so the rule alerts on arrivals only.
    assert "avg_arrival_dwell_seconds >" not in rule
    assert max(high for _, high in vocab.FOOT_TRAFFIC_ZONE_DWELL.values()) < 600


def test_zone_surge_threshold_follows_the_stream_visit_rate() -> None:
    # The traffic values deployment uses equal the generators' own.
    assert cal.TRAFFIC_VISIT_CONVERSION == VISIT_CONVERSION_CALIBRATION
    assert cal.TRAFFIC_STORE_MULTIPLIER_MAX == max(high for _, _, (_, high) in VOLUME_CLASSES)
    assert cal.TRAFFIC_WEATHER_FACTOR_MAX == WEATHER_FACTOR_RANGE[1]
    for store_type in ("supercenter", "hardware", "luxury"):
        profile = load_dictionaries(default_dictionary_root(), store_type).profile
        transactions = profile.resolved_transactions_per_store_day()
        hours = profile.resolved_operating_hours()
        # The stream's own visit rate at its busiest non-holiday local hour.
        busiest = max(
            store_visits_per_hour(
                profile, transactions, 1.0, pattern, datetime(2025, month, day, hour)
            )
            for pattern in hours
            for month in range(1, 13)
            for day in range(3, 10)
            for hour in range(24)
            if holiday_factor(date(2025, month, day), store_class(profile)) == 1.0
        )
        assert cal.peak_store_visits_per_hour(profile, transactions) == pytest.approx(busiest)
        # Every visit passes the entrance: the busiest zone carries the store's rate.
        assert max(vocab.zone_passage_shares(vocab.ble_layout(profile)).values()) == 1.0
        mean = cal.zone_peak_arrivals(profile, transactions)
        threshold = cal.zone_surge_arrivals(profile, transactions)
        assert mean + 3.9 * mean**0.5 < threshold <= mean + 4 * mean**0.5 + 1
    luxury = load_dictionaries(default_dictionary_root(), "luxury").profile
    assert cal.zone_surge_arrivals(luxury, 1) == cal.ZONE_SURGE_MIN_ARRIVALS


def test_high_value_rule_is_a_multiple_of_the_reference_average_ticket() -> None:
    rule = _rule("High-value receipt")
    assert _window_minutes(rule) == cal.HIGH_VALUE_RULE_WINDOW_MINUTES
    assert "toscalar(fn_rule_thresholds() | project high_value_receipt_total)" in rule
    assert "where total > high_value_total" in rule and "by trace_id" in rule
    profile = load_dictionaries(default_dictionary_root(), cal.REFERENCE_STORE_TYPE).profile
    assert _rule_thresholds_function()["high_value_receipt_total"] == (
        cal.high_value_receipt_threshold(profile)
    )


@pytest.mark.parametrize("store_type", ["supercenter", "hardware", "luxury"])
def test_high_value_threshold_scales_with_the_store_type(store_type: str) -> None:
    profile = load_dictionaries(default_dictionary_root(), store_type).profile
    threshold = cal.high_value_receipt_threshold(profile)
    assert threshold == pytest.approx(cal.HIGH_VALUE_TICKET_MULTIPLE * profile.avg_ticket_target)
    assert threshold > profile.avg_ticket_target


def test_anomaly_functions_use_calibrated_detector_parameters() -> None:
    calls = re.findall(
        r"series_decompose_anomalies\(\w+, ([0-9.]+), (-?\d+), '(\w+)'\)",
        ANOMALY_KQL,
    )
    assert len(calls) == 5
    for threshold, seasonality, trend in calls:
        assert float(threshold) == cal.ANOMALY_THRESHOLD
        assert int(seasonality) == cal.ANOMALY_SEASONALITY
        assert trend == cal.ANOMALY_TREND

    lookbacks = re.findall(r"\(lookback_hours:int = (\d+)", ANOMALY_KQL)
    assert lookbacks and {int(value) for value in lookbacks} == {cal.ANOMALY_LOOKBACK_HOURS}

    bands = re.findall(r'abs\(anomaly_score\) >= ([0-9.]+), "(\w+)"', ANOMALY_KQL)
    expected = [(str(bound), label) for label, bound in cal.ANOMALY_SEVERITY_BANDS]
    assert len(bands) == 5 * len(expected)
    for index in range(0, len(bands), len(expected)):
        assert bands[index : index + len(expected)] == expected
    assert cal.anomaly_severity(-3.2) == "critical"
    assert cal.anomaly_severity(1.49) == "low"


def _anomaly_functions() -> dict[str, str]:
    """Body of each ``fn_detect_*_anomalies`` function in 06-ml-anomaly-detection.kql."""
    found = re.findall(
        r"function with \([^)]*\) (fn_detect_\w+_anomalies)\(lookback_hours:int = \d+\)\n"
        r"\{\n(.*?)\n\}\n",
        ANOMALY_KQL,
        re.DOTALL,
    )
    return {name: body for name, body in found if name != "fn_detect_all_anomalies"}


def test_anomaly_series_cover_closed_hours_from_the_first_event_per_store() -> None:
    functions = _anomaly_functions()
    assert len(functions) == 5
    for name, body in functions.items():
        # The window ends at the open hour, which is never scored (S-9).
        assert "let end_time = bin(now(), 1h);" in body, name
        assert "+ 1h" not in body, name
        # The series starts at the first event, not lookback hours of zeros (C-13).
        assert "summarize first_hour = bin(min(ingest_timestamp), 1h)" in body, name
        assert "from start_time to end_time step 1h" in body, name
        # One series per store; online payments and DC stock (no store) are left out.
        assert "| where isnotnull(store_id)" in body, name
        series = re.search(r"make-series (.*?);", body, re.DOTALL)
        assert series is not None and series.group(1).split("step 1h")[1].split()[:2] == [
            "by",
            "store_id",
        ], name
    # Averages are missing, not zero, in hours without events, then interpolated.
    for name, column in (
        ("fn_detect_basket_size_anomalies", "basket_list"),
        ("fn_detect_payment_anomalies", "rate_list"),
    ):
        body = functions[name]
        assert re.search(rf"{column} = avg\(\w+\) default = real\(null\)", body), name
        assert f"{column} = series_fill_linear({column})" in body, name
    # Inventory: per store (C-14), scored only with enough active hours.
    inventory = functions["fn_detect_inventory_movement_anomalies"]
    assert "product_id" not in inventory
    match = re.search(
        r"array_sum\(array_iff\(series_greater\(delta_list, 0\), 1, 0\)\) >= (\d+)", inventory
    )
    assert match is not None and int(match.group(1)) == cal.ANOMALY_MIN_ACTIVE_HOURS
    # Persistence scores the same closed hours.
    assert "let window_end = iif(isnull(closed_end), bin(now(), 1h), closed_end);" in ANOMALY_KQL


def test_stream_reads_thresholds_from_calibration() -> None:
    assigned = {
        target.id
        for source in (STREAM_TEMPLATE, STREAM_EVENTS)
        for node in ast.parse(source).body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    constants = {name for name in vars(cal) if name.isupper()}
    assert not assigned & constants, "stream redefines calibration constants"
    supply_assigned = {
        target.id
        for node in ast.parse(STREAM_SUPPLY).body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    assert not supply_assigned & constants, "stream_supply redefines calibration constants"
    for name in ("TRUCK_LOAD_MINUTES", "STOCKOUT_QUANTITY_THRESHOLD"):
        assert re.search(rf"\b{name}\b", STREAM_SUPPLY), name
    # The unload and lane models are shared with history (network, inventory_policy).
    for helper in ("unload_minutes", "lane_transit_minutes", "next_slot_ms", "dock_factor"):
        assert helper in STREAM_SUPPLY, helper
    for name in (
        "TRUCK_NORMAL_DWELL_MINUTES_MIN",
        "TRUCK_UNLOAD_BASE_MINUTES",
        "TRUCK_UNLOAD_UNITS_PER_MINUTE",
        "TRUCK_UNLOAD_NOISE_MINUTES",
        "TRUCK_UNLOAD_MINUTES_MAX",
        "TRUCK_DOCK_FACTOR_RANGE",
        "TRUCK_TRANSIT_MINUTES_MIN",
        "TRUCK_TRANSIT_MINUTES_MAX",
        "TRUCK_TURNAROUND_MINUTES",
    ):
        assert re.search(rf"\b{name}\b", SHARED_DELIVERY), name
    # Reorder points and priorities come from the shared replenishment policy.
    for helper in ("reorder_levels(", "reorder_priority(", "reorder_quantity("):
        assert helper in STREAM_SUPPLY, helper
    # No lane-and-coin-flip inventory or truck model is left in the stream.
    assert "TRUCK_LATE" not in STREAM_EVENTS and "pmod(v" not in STREAM_EVENTS
