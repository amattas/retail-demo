"""Shared vocabulary: identifier formats and the batch/stream agreement contract."""

from __future__ import annotations

import ast
import re
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pytest

from retail_setup.generation import vocabulary as vocab

UTILITY = Path(__file__).resolve().parents[2]
STREAM_TEMPLATE = UTILITY / "notebooks" / "templates" / "driver-05-stream.py"
STREAM_EVENTS = UTILITY / "src" / "retail_setup" / "generation" / "stream_events.py"
TEMPLATE_SOURCE = STREAM_TEMPLATE.read_text(encoding="utf-8")
EVENTS_SOURCE = STREAM_EVENTS.read_text(encoding="utf-8")
# The stream's code: the notebook template plus the event module it inlines.
STREAM_SOURCE = TEMPLATE_SOURCE + "\n" + EVENTS_SOURCE
STREAM_TREES = [ast.parse(TEMPLATE_SOURCE), ast.parse(EVENTS_SOURCE)]


def _module_literals(tree: ast.Module) -> dict[str, object]:
    """Module-level ``NAME = <literal>`` assignments of a template."""
    found: dict[str, object] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        try:
            found[target.id] = ast.literal_eval(node.value)
        except ValueError:
            continue
    return found


STREAM_LITERALS = {
    name: value for tree in STREAM_TREES for name, value in _module_literals(tree).items()
}


def _shared_values() -> dict[str, object]:
    return {
        name: value
        for name, value in vars(vocab).items()
        if name.isupper() and isinstance(value, (list, dict, tuple, str))
    }


def _present(pattern: str) -> Callable[[], bool]:
    return lambda: re.search(pattern, STREAM_SOURCE) is not None


# Every remaining difference between the live stream and the shared vocabulary.
# Each detector returns True while the gap still exists. When a later change
# makes the stream use the shared definition, its detector turns False and this
# test fails until the entry is removed, so the allowlist only shrinks.
KNOWN_STREAM_GAPS: dict[str, tuple[str, Callable[[], bool]]] = {}


def test_format_helpers_match_the_historical_dimension_formats() -> None:
    assert vocab.ble_id(1) == "BLE000001"
    assert vocab.ble_id(36) == "BLE000010"
    assert vocab.ble_id(36**6) == "BLE1000000"
    assert vocab.ad_id(123) == "AD00000123"
    assert vocab.ad_id(123_456_789) == "AD123456789"
    assert vocab.beacon_id(7, "CHECKOUT") == "BEACON_007_CHECKOUT"
    assert vocab.sensor_id(1234, "AISLES_A") == "SENSOR_1234_AISLES_A"
    assert vocab.campaign_id(date(2025, 1, 7), 2) == "CAMP2025010702"
    with pytest.raises(ValueError):
        vocab.campaign_id(date(2025, 1, 7), 100)


def test_decline_model_and_promo_rules() -> None:
    assert vocab.decline_probability("CASH") == 0.0
    assert vocab.decline_probability("MOBILE_PAY") == pytest.approx(0.03)
    assert vocab.decline_probability("PAYPAL", vocab.ONLINE_TENDERS) == pytest.approx(0.0275)
    with pytest.raises(ValueError):
        vocab.decline_probability("MOBILE")

    index = {code: i for i, (code, *_rest) in enumerate(vocab.PROMO_CATALOG)}
    assert vocab.promo_eligible(index["BFRIDAY40"], 11, 10_000, 1)
    assert not vocab.promo_eligible(index["BFRIDAY40"], 11, 9_999, 1)
    assert not vocab.promo_eligible(index["BFRIDAY40"], 10, 10_000, 1)
    assert not vocab.promo_eligible(index["BOGO50"], 6, 500, 1)
    assert vocab.promo_eligible(index["BOGO50"], 6, 500, 2)


def test_profile_zones_drive_ble_zones_when_declared() -> None:
    class _Profile:
        zones = ["entrance", " Produce ", "checkout", "entrance"]

    assert vocab.ble_zones_for_profile(None) == vocab.BLE_ZONES
    assert vocab.ble_zones_for_profile(_Profile()) == ["ENTRANCE", "PRODUCE", "CHECKOUT"]  # type: ignore[arg-type]


def test_ble_layout_orders_a_store_types_zones_into_a_path() -> None:
    class _Grocery:
        zones = ["entrance", "produce", "bakery", "checkout", "exit"]

    layout = vocab.ble_layout(_Grocery())  # type: ignore[arg-type]
    assert (layout.entry, layout.middle, layout.checkout, layout.exit) == (
        "ENTRANCE",
        ["PRODUCE", "BAKERY"],
        "CHECKOUT",
        "EXIT",
    )
    assert layout.zones == ["ENTRANCE", "PRODUCE", "BAKERY", "CHECKOUT", "EXIT"]
    assert layout.dwell["CHECKOUT"] == vocab.BLE_ZONE_DWELL["CHECKOUT"]
    assert layout.dwell["EXIT"] == vocab.BLE_EXIT_DWELL
    assert layout.dwell["BAKERY"] == vocab.BLE_DEPARTMENT_DWELL

    class _Custom:
        zones = ["lobby", "pro-desk", "yard"]

    custom = vocab.ble_layout(_Custom())  # type: ignore[arg-type]
    assert (custom.entry, custom.middle, custom.checkout, custom.exit) == (
        "LOBBY",
        ["PRO-DESK", "YARD"],
        None,
        None,
    )


def test_stream_does_not_redefine_shared_vocabulary() -> None:
    shared = _shared_values()
    duplicates = {
        name: other
        for name, value in STREAM_LITERALS.items()
        for other, shared_value in shared.items()
        if value == shared_value and isinstance(value, (list, dict, tuple))
    }
    assert not duplicates, f"stream redefines shared vocabulary: {duplicates}"


def test_stream_defines_no_live_only_vocabulary() -> None:
    assert sorted(name for name in STREAM_LITERALS if name.startswith("LIVE_")) == []


@pytest.mark.parametrize("gap", sorted(KNOWN_STREAM_GAPS))
def test_known_stream_vocabulary_gaps_still_exist(gap: str) -> None:
    finding, detector = KNOWN_STREAM_GAPS[gap]
    assert detector(), f"gap {gap!r} is closed ({finding}); remove it from KNOWN_STREAM_GAPS"


def test_stream_uses_the_shared_definitions() -> None:
    for usage in (
        'tender_column(d, [F.col("receipt_id")])',
        'payment_status_column(d, [F.col("receipt_id")], F.col("tender"))',
        "price_basket(",
        'ble_id_column(F.col("customer_id"))',
        "anonymous_ble_id_column(",
        "beacon_id_column(",
        "sensor_id_column(",
        "campaign_id_column(",
        "impression_device_cost(",
        'ad_id_column(F.col("customer_pick"))',
        "ONLINE_TENDERS",
        "ble_layout(ctx.profile)",
        # Online orders: the historical fulfillment mix and the decline model.
        'd.pick_by_weights(key, "online-mode", ONLINE_FULFILLMENT_MIX)',
        'payment_status_column(d, key, F.col("tender"), ONLINE_TENDERS, "online-decline")',
        "ONLINE_CANCEL_RATE",
        "ONLINE_BACKORDER_RATE",
        "ONLINE_PROMOS",
    ):
        assert usage in EVENTS_SOURCE, usage
    assert set(vocab.BLE_ZONE_DWELL) == set(vocab.BLE_ZONES)
    assert vocab.FULFILLMENT_MODES == ["SHIP_FROM_DC", "SHIP_FROM_STORE", "BOPIS"]


@pytest.mark.spark
def test_column_formats_equal_python_formats(spark) -> None:
    from pyspark.sql import functions as F

    ids = [0, 1, 35, 36, 1_295, 46_656, 2_176_782_336, 99_999_999, 123_456_789]
    rows = (
        spark.createDataFrame([(i,) for i in ids], "id long")
        .select(
            "id",
            vocab.ble_id_column(F.col("id")).alias("ble"),
            vocab.ad_id_column(F.col("id")).alias("ad"),
            vocab.campaign_id_column(F.lit(date(2025, 1, 7)), F.lit(3)).alias("campaign"),
        )
        .collect()
    )
    for row in rows:
        assert row.ble == vocab.ble_id(row.id)
        assert row.ad == vocab.ad_id(row.id)
        assert row.campaign == vocab.campaign_id(date(2025, 1, 7), 3)
