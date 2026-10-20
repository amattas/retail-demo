"""Correctness contracts for Eventhouse KQL objects."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
KQL_DIR = ROOT / "fabric" / "kql_database"
FUNCTIONS = KQL_DIR / "03-create-functions.kql"
MATERIALIZED_VIEWS = KQL_DIR / "04-create-materialized-views.kql"
ANOMALIES = KQL_DIR / "06-ml-anomaly-detection.kql"
PRICING = KQL_DIR / "07-pricing-approval-tables.kql"
PRICING_DASHBOARD = ROOT / "fabric" / "dashboards" / "pricing-approval.template.json"

EXPECTED_MATERIALIZED_VIEWS = {
    "mv_store_sales_minute",
    "mv_top_products_15m",
    "mv_sales_product_minute",
    "mv_tender_mix_15m",
    "mv_zone_dwell_minute",
    "mv_pricing_approval_metrics",
    "mv_pricing_rejection_metrics",
}
ANOMALY_FUNCTIONS = {
    "fn_detect_transaction_velocity_anomalies",
    "fn_detect_basket_size_anomalies",
    "fn_detect_inventory_movement_anomalies",
    "fn_detect_payment_anomalies",
    "fn_detect_traffic_anomalies",
}


def _object_body(source: str, object_name: str) -> str:
    match = re.search(
        rf"\b{re.escape(object_name)}\([^)]*\)\s*\n\{{(?P<body>.*?)^\}}",
        source,
        re.MULTILINE | re.DOTALL,
    )
    assert match, f"Missing KQL function {object_name}"
    return match.group("body")


def _materialized_views() -> dict[str, tuple[Path, str, str]]:
    views: dict[str, tuple[Path, str, str]] = {}
    for path in sorted(KQL_DIR.glob("*.kql")):
        source = path.read_text(encoding="utf-8-sig")
        for match in re.finditer(
            r"^\.create(?:-or-alter)? materialized-view (?P<name>\w+)"
            r" on table \w+\s*\n\{(?P<body>.*?)^\}",
            source,
            re.MULTILINE | re.DOTALL,
        ):
            name = match.group("name")
            assert name not in views, f"Duplicate materialized view {name}"
            views[name] = (path, match.group("body"), source)
    return views


def _latest_valid_touch(
    purchase_ts: datetime,
    touches: list[tuple[str, datetime]],
) -> str | None:
    valid = [
        (touch_ts, impression_id)
        for impression_id, touch_ts in touches
        if touch_ts <= purchase_ts and purchase_ts - touch_ts <= timedelta(days=7)
    ]
    return max(valid, default=(None, None))[1]


def test_all_materialized_views_are_time_independent_and_retained() -> None:
    views = _materialized_views()

    assert set(views) == EXPECTED_MATERIALIZED_VIEWS
    for name, (path, body, source) in views.items():
        location = f"{path.name}:{name}"
        assert not re.search(
            r"\b(?:ago|now)\s*\(", body, re.IGNORECASE
        ), location
        assert (
            f".alter-merge materialized-view {name} policy retention "
            "softdelete = 7d recoverability = disabled"
        ) in source, location


def test_pending_recommendations_use_latest_cross_event_state() -> None:
    source = PRICING.read_text(encoding="utf-8")
    body = _object_body(source, "fn_pending_recommendations")

    assert ".create-or-alter materialized-view mv_pending_recommendations" not in source
    assert ".drop materialized-view mv_pending_recommendations ifexists" in source
    for table in (
        "pricing_recommendation_created",
        "pricing_recommendation_approved",
        "pricing_recommendation_rejected",
    ):
        assert table in body
    assert 'current_status = "APPROVED", state_priority = 1' in body
    assert 'current_status = "REJECTED", state_priority = 2' in body
    assert "format_datetime(event_ts" in body
    assert "arg_max(state_sort, *)" in body
    assert 'where current_status == "PENDING"' in body


def test_pending_dashboard_queries_use_current_state_function() -> None:
    dashboard = json.loads(PRICING_DASHBOARD.read_text(encoding="utf-8"))
    pending_titles = {
        "Pending Recommendations Summary",
        "Recommendations by Department (Pending)",
        "Top Pending Recommendations by Revenue Impact",
        "Price Change Distribution (Pending)",
        "Confidence Level Breakdown",
        "Constraint Compliance Status",
    }
    pending_queries = [
        tile["query"] for tile in dashboard["tiles"] if tile["title"] in pending_titles
    ]

    assert {tile["title"] for tile in dashboard["tiles"]} >= pending_titles
    assert all("fn_pending_recommendations()" in query for query in pending_queries)
    assert all(
        "pricing_recommendation_created | where status == 'PENDING'" not in query
        for query in pending_queries
    )


def test_attribution_filters_candidates_before_last_touch_selection() -> None:
    source = FUNCTIONS.read_text(encoding="utf-8")
    body = _object_body(source, "fn_marketing_attribution")

    join_position = body.index("| join kind=inner touches")
    nonfuture_position = body.index("| where touch_ts <= purchase_ts")
    window_position = body.index("| where lag_seconds between (0 .. 604800)")
    latest_position = body.index("| summarize arg_max(touch_rank, *) by attribution_id")

    assert join_position < nonfuture_position < window_position < latest_position


def test_attribution_public_functions_return_only_reconciled_rows() -> None:
    source = FUNCTIONS.read_text(encoding="utf-8")
    attribution = _object_body(source, "fn_attribution")
    window = _object_body(source, "fn_attribution_window")
    campaign = _object_body(source, "fn_campaign_performance")

    assert "fn_marketing_attribution()" in attribution
    assert 'where attribution_status == "ATTRIBUTED"' in attribution
    assert "fn_attribution()" in window
    assert "lag_seconds >= 0" in window
    assert "lag_seconds <= windowHours * 3600" in window
    assert "fn_attribution()" in campaign


@pytest.mark.parametrize(
    ("purchase_ts", "touches", "expected"),
    [
        (
            datetime(2026, 1, 10, tzinfo=UTC),
            [
                ("valid", datetime(2026, 1, 9, tzinfo=UTC)),
                ("future", datetime(2026, 1, 11, tzinfo=UTC)),
            ],
            "valid",
        ),
        (
            datetime(2026, 1, 10, tzinfo=UTC),
            [
                ("outside", datetime(2026, 1, 2, tzinfo=UTC)),
                ("inside", datetime(2026, 1, 3, tzinfo=UTC)),
            ],
            "inside",
        ),
        (
            datetime(2026, 1, 10, tzinfo=UTC),
            [
                ("older", datetime(2026, 1, 8, tzinfo=UTC)),
                ("latest-a", datetime(2026, 1, 9, tzinfo=UTC)),
                ("latest-b", datetime(2026, 1, 9, tzinfo=UTC)),
            ],
            "latest-b",
        ),
        (
            datetime(2026, 1, 10, tzinfo=UTC),
            [("future", datetime(2026, 1, 11, tzinfo=UTC))],
            None,
        ),
        (
            datetime(2026, 1, 10, tzinfo=UTC),
            [("outside", datetime(2026, 1, 2, tzinfo=UTC))],
            None,
        ),
    ],
)
def test_last_touch_contract(
    purchase_ts: datetime,
    touches: list[tuple[str, datetime]],
    expected: str | None,
) -> None:
    assert _latest_valid_touch(purchase_ts, touches) == expected


def test_anomaly_functions_fill_explicit_hourly_series() -> None:
    source = ANOMALIES.read_text(encoding="utf-8")

    assert "make_list(" not in source
    for name in ANOMALY_FUNCTIONS:
        body = _object_body(source, name)
        assert "make-series" in body, name
        assert "from start_time to end_time step 1h" in body, name
        assert "default = 0" in body, name


def test_anomaly_order_uses_numeric_severity_rank_without_schema_drift() -> None:
    source = ANOMALIES.read_text(encoding="utf-8")
    body = _object_body(source, "fn_detect_all_anomalies")

    assert "severity_rank = case(" in body
    assert "| order by severity_rank desc" in body
    assert "| project-away severity_rank" in body
    assert "| order by severity desc" not in body
    assert (
        "| project alert_id, detection_time, metric_type, entity_type, entity_id,"
        in body
    )
