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
STREAM_SOURCE = STREAM_TEMPLATE.read_text(encoding="utf-8")
STREAM_TREE = ast.parse(STREAM_SOURCE)


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


STREAM_LITERALS = _module_literals(STREAM_TREE)


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
KNOWN_STREAM_GAPS: dict[str, tuple[str, Callable[[], bool]]] = {
    "tender-names": (
        "S-15: live tenders use MOBILE, the vocabulary uses MOBILE_PAY",
        lambda: (
            "MOBILE" in STREAM_LITERALS["LIVE_TENDERS"] and "MOBILE" not in vocab.tender_names()
        ),
    ),
    "tender-mix": (
        "S-15: live tenders are picked uniformly instead of by TENDERS weights",
        _present(r'_pick\(F\.col\("v"\), "tender", LIVE_TENDERS\)'),
    ),
    "payment-declines": (
        "S-15/C-12: live payments are always APPROVED (no decline model)",
        lambda: STREAM_SOURCE.count('F.lit("APPROVED").alias("status")') == 2,
    ),
    "marketing-channels": (
        "S-14: live impressions use a 4-channel subset",
        lambda: set(STREAM_LITERALS["LIVE_CHANNELS"]) < set(vocab.MARKETING_CHANNELS),
    ),
    "device-casing": (
        "S-14: live device types are lower case",
        lambda: (
            [d.upper() for d in STREAM_LITERALS["LIVE_DEVICES"]] == vocab.DEVICE_TYPES
            and STREAM_LITERALS["LIVE_DEVICES"] != vocab.DEVICE_TYPES
        ),
    ),
    "campaign-id-format": (
        "S-14: live campaign IDs are CMP-<n>, not CAMP<yyyyMMdd><NN>",
        _present(r'F\.lit\("CMP-"\)'),
    ),
    "impression-cost": (
        "S-14/B-6: live cost per impression is uniform $0.10-$2.10, not by channel",
        _present(r"F\.lit\(2\.0\) \+ F\.lit\(0\.1\)"),
    ),
    "organic-ad-id-padding": (
        "S-14/L-25: organic live customer_ad_id is AD + unpadded customer ID",
        _present(r'F\.concat\(F\.lit\("AD"\), _id\('),
    ),
    "ble-id-format": (
        "S-6: live BLE IDs are BLE + decimal customer ID, not base-36 padded to 6",
        _present(r'F\.concat\(F\.lit\("BLE"\), F\.col\("customer_id"\)'),
    ),
    "ble-zones": (
        "S-6/B-18: live BLE pings use the foot-traffic zones instead of BLE_ZONES",
        lambda: "BLE_ZONES" not in STREAM_SOURCE,
    ),
    "beacon-id-format": (
        "S-6: live beacon IDs are BEACON_<store>_<zone>, not BEACON_%03d_%s",
        _present(r'F\.lit\("BEACON_"\)'),
    ),
    "sensor-id-format": (
        "S-6: live sensor IDs are SENSOR_<store>_<zone>, not SENSOR_%03d_%s",
        _present(r'F\.lit\("SENSOR_"\)'),
    ),
    "promo-catalog": (
        "S-22: the live catalog lacks SAVE15, SAVE25 and BFRIDAY40, and minimums are dollars",
        lambda: (
            {code for code, *_ in STREAM_LITERALS["LIVE_NAMED_PROMOS"]}
            < {code for code, *_ in vocab.PROMO_CATALOG}
        ),
    ),
    "evergreen-promos": (
        "S-22: the live evergreen fallback lacks DEAL15",
        lambda: set(STREAM_LITERALS["LIVE_EVERGREEN_PROMOS"]) < set(vocab.EVERGREEN_PROMOS),
    ),
    "promo-selection": (
        "S-22/B-9: live promos pick one code per receipt on the gross subtotal, not per line",
        _present(
            r'_promo_elig\(\s*F\.col\("_pidx"\), F\.month\(F\.col\("ts"\)\), F\.col\("subtotal"\)'
        ),
    ),
    "fulfillment-mix": (
        "S-12: live fulfillment modes are picked uniformly, not by ONLINE_FULFILLMENT_MIX",
        _present(r'_pick\(F\.col\("v"\), "omode", FULFILLMENT_MODES\)'),
    ),
    "online-tenders": (
        "S-12: live online orders reuse in-store tenders (including CASH)",
        lambda: "ONLINE_TENDERS" not in STREAM_SOURCE,
    ),
}


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


def test_stream_does_not_redefine_shared_vocabulary() -> None:
    shared = _shared_values()
    duplicates = {
        name: other
        for name, value in STREAM_LITERALS.items()
        for other, shared_value in shared.items()
        if value == shared_value and isinstance(value, (list, dict, tuple))
    }
    assert not duplicates, f"stream redefines shared vocabulary: {duplicates}"


def test_live_only_constants_are_listed_as_known_gaps() -> None:
    live = sorted(name for name in STREAM_LITERALS if name.startswith("LIVE_"))
    assert live == [
        "LIVE_CHANNELS",
        "LIVE_DEVICES",
        "LIVE_EVERGREEN_PROMOS",
        "LIVE_NAMED_PROMOS",
        "LIVE_TENDERS",
    ]
    for name in live:
        assert STREAM_LITERALS[name] not in _shared_values().values(), name


@pytest.mark.parametrize("gap", sorted(KNOWN_STREAM_GAPS))
def test_known_stream_vocabulary_gaps_still_exist(gap: str) -> None:
    finding, detector = KNOWN_STREAM_GAPS[gap]
    assert detector(), f"gap {gap!r} is closed ({finding}); remove it from KNOWN_STREAM_GAPS"


def test_stream_uses_shared_definitions_where_values_already_agree() -> None:
    assert '_pick(F.col("v"), "zone", FOOT_TRAFFIC_ZONES)' in STREAM_SOURCE
    assert 'attr_customer_ad_id = ad_id_column(F.col("customer_id"))' in STREAM_SOURCE
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
