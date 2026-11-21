"""Calibrated thresholds: consumers must carry the calibration constants."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation import calibration as cal

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
STREAM_TEMPLATE = (REPO / "utility" / "notebooks" / "templates" / "driver-05-stream.py").read_text(
    encoding="utf-8"
)
STREAM_EVENTS = (
    REPO / "utility" / "src" / "retail_setup" / "generation" / "stream_events.py"
).read_text(encoding="utf-8")
STREAM_SUPPLY = (
    REPO / "utility" / "src" / "retail_setup" / "generation" / "stream_supply.py"
).read_text(encoding="utf-8")


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


def test_truck_dwell_sla_is_one_value_everywhere() -> None:
    rule = _rule("Truck dwell breach")
    match = re.search(r"let threshold_minutes = ([0-9.]+);", rule)
    assert match is not None
    assert float(match.group(1)) == cal.TRUCK_DWELL_SLA_MINUTES
    assert f"{cal.TRUCK_DWELL_SLA_MINUTES}-minute SLA" in rule
    assert _window_minutes(rule) == cal.TRUCK_DWELL_RULE_WINDOW_MINUTES

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
    assert _window_minutes(_rule("Stockout detected")) == cal.STOCKOUT_RULE_WINDOW_MINUTES
    urgent = _rule("Reorder URGENT")
    assert "priority == 'URGENT'" in urgent
    assert _window_minutes(urgent) == cal.URGENT_REORDER_RULE_WINDOW_MINUTES
    # A stockout is a store's balance reaching zero, as in history's fact_stockouts.
    assert cal.STOCKOUT_QUANTITY_THRESHOLD == 1


def test_zone_surge_rule_uses_calibrated_thresholds() -> None:
    rule = _rule("Zone arrival surge")
    assert _window_minutes(rule) == cal.ZONE_SURGE_WINDOW_MINUTES
    match = re.search(
        r"where arrivals > (\d+) or avg_arrival_dwell_seconds > (\d+)",
        rule,
    )
    assert match is not None
    assert int(match.group(1)) == cal.ZONE_SURGE_ARRIVALS
    assert int(match.group(2)) == cal.ZONE_LONG_DWELL_SECONDS


def test_high_value_rule_is_a_multiple_of_the_reference_average_ticket() -> None:
    rule = _rule("High-value receipt")
    assert _window_minutes(rule) == cal.HIGH_VALUE_RULE_WINDOW_MINUTES
    match = re.search(r"where total > ([0-9.]+)", rule)
    assert match is not None
    profile = load_dictionaries(default_dictionary_root(), cal.REFERENCE_STORE_TYPE).profile
    assert float(match.group(1)) == cal.high_value_receipt_threshold(profile)


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
    for name in (
        "TRUCK_NORMAL_DWELL_MINUTES_MIN",
        "TRUCK_UNLOAD_BASE_MINUTES",
        "TRUCK_UNLOAD_UNITS_PER_MINUTE",
        "TRUCK_UNLOAD_NOISE_MINUTES",
        "TRUCK_DOCK_FACTOR_RANGE",
        "TRUCK_LOAD_MINUTES",
        "TRUCK_TRANSIT_MINUTES_MIN",
        "TRUCK_TRANSIT_MINUTES_MAX",
        "STOCKOUT_QUANTITY_THRESHOLD",
    ):
        assert re.search(rf"\b{name}\b", STREAM_SUPPLY), name
    # Reorder points and priorities come from the shared replenishment policy.
    for helper in ("reorder_levels(", "reorder_priority(", "reorder_quantity("):
        assert helper in STREAM_SUPPLY, helper
    # No lane-and-coin-flip inventory or truck model is left in the stream.
    assert "TRUCK_LATE" not in STREAM_EVENTS and "pmod(v" not in STREAM_EVENTS
