"""Execute bounded reconciliation against equivalent in-memory SQL tables."""

import sqlite3
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from deploy.scripts.data_consistency import (
    CONSISTENCY_FIELDS,
    STREAMING_CONSISTENCY_SQL,
    consistency_counts,
)
from deploy.scripts.verify_readiness import (
    CheckFailed,
    EvidenceUnknown,
    FabricReadinessAdapter,
    ReadinessRunner,
)


def _date(value):
    return datetime(1900, 1, 1) if value == 0 else datetime.fromisoformat(value)


@pytest.fixture
def database():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row

    def dateadd(unit, amount, value):
        if value is None:
            return None
        delta = timedelta(hours=amount) if unit == "hour" else timedelta(days=amount)
        return (_date(value) + delta).isoformat(sep=" ")

    def datediff(unit, start, end):
        if start is None or end is None:
            return None
        seconds = (_date(end) - _date(start)).total_seconds()
        return int(seconds // (3600 if unit == "hour" else 86400))

    connection.create_function("DATEADD", 3, dateadd)
    connection.create_function("DATEDIFF", 3, datediff)
    for schema in ("ag", "au", "cusn"):
        connection.execute(f"ATTACH DATABASE ':memory:' AS {schema}")
    connection.executescript("""
        CREATE TABLE ag._watermarks(source_table TEXT, last_processed_ts TEXT);
        INSERT INTO ag._watermarks VALUES
            ('receipt_created', '2026-09-04 12:00:00'),
            ('inventory_updated', '2026-09-04 12:00:00');
        CREATE TABLE ag.fact_receipts(
            receipt_id_ext TEXT, store_id INTEGER, customer_id INTEGER,
            event_ts TEXT, total_cents INTEGER
        );
        INSERT INTO ag.fact_receipts VALUES
            ('R1', 1, 1, '2026-09-04 08:00:00', 1234),
            ('R2', 1, 1, '2026-09-04 10:00:00', 2000);
        CREATE TABLE ag.fact_store_inventory_txn(trace_id TEXT, event_ts TEXT, balance INTEGER);
        INSERT INTO ag.fact_store_inventory_txn VALUES ('I1', '2026-09-04 08:00:00', 42);
        CREATE TABLE ag.dim_stores(ID INTEGER);
        INSERT INTO ag.dim_stores VALUES (1);
        CREATE TABLE ag.dim_customers(ID INTEGER);
        INSERT INTO ag.dim_customers VALUES (1);
        CREATE TABLE au.sales_minute_store(
            store_id INTEGER, ts TEXT, receipts INTEGER, total_sales REAL
        );
        INSERT INTO au.sales_minute_store VALUES
            (1, '2026-09-04 08:00:00', 1, 12.34),
            (1, '2026-09-04 10:00:00', 1, 20.00);
        CREATE TABLE cusn.receipt_created(receipt_id_ext TEXT, ingest_timestamp TEXT);
        INSERT INTO cusn.receipt_created VALUES
            ('R1', '2026-09-04 08:00:00'), ('R2', '2026-09-04 10:00:00'),
            ('NOT_VISIBLE_YET', '2026-09-04 12:05:00');
        CREATE TABLE cusn.inventory_updated(trace_id TEXT, ingest_timestamp TEXT);
        INSERT INTO cusn.inventory_updated VALUES ('I1', '2026-09-04 08:00:00');
    """)
    try:
        yield connection
    finally:
        connection.close()


def _query(database):
    query = STREAMING_CONSISTENCY_SQL.replace("COUNT_BIG(", "COUNT(")
    for function in ("DATEADD", "DATEDIFF"):
        for unit in ("hour", "day"):
            query = query.replace(f"{function}({unit},", f"{function}('{unit}',")
    return consistency_counts(dict(database.execute(query).fetchone()))


def test_reconciliation_accepts_consistent_settled_data_and_ignores_pending_arrivals(
    database,
):
    result = _query(database)
    assert all(result[key] == 0 for key in CONSISTENCY_FIELDS)
    assert result["settled_receipts"] == 1
    assert result["settled_inventory"] == 1
    assert result["settled_sales"] == 1


@pytest.mark.parametrize(
    ("mutation", "field"),
    [
        (
            "DELETE FROM ag.fact_receipts WHERE receipt_id_ext = 'R1'",
            "missing_receipts",
        ),
        ("DELETE FROM ag.fact_store_inventory_txn", "missing_inventory"),
        ("UPDATE ag.fact_store_inventory_txn SET balance = NULL", "null_balances"),
        ("DELETE FROM ag.dim_customers", "orphan_receipts"),
        ("UPDATE au.sales_minute_store SET total_sales = 0", "gold_sales_mismatches"),
    ],
)
def test_reconciliation_detects_actual_key_balance_and_aggregate_corruption(
    database, mutation, field
):
    database.execute(mutation)
    assert _query(database)[field] > 0


def test_missing_consistency_observation_is_not_silently_zero():
    with pytest.raises(ValueError, match="consistency observation"):
        consistency_counts({})


def test_readiness_adapter_executes_authoritative_consistency_query(database):
    adapter = object.__new__(FabricReadinessAdapter)
    queries = []
    result = _query(database)
    adapter._execute_sql = lambda query: queries.append(query) or [result]
    assert adapter.data_consistency_signal() == result
    assert queries == [STREAMING_CONSISTENCY_SQL]


@pytest.mark.parametrize("mode", ["valid", "corrupt", "empty"])
def test_watermark_readiness_requires_observed_reconciliation(database, mode):
    counts = _query(database)
    if mode == "corrupt":
        counts["missing_receipts"] = 1
    if mode == "empty":
        counts["settled_sales"] = 0
    runner = object.__new__(ReadinessRunner)
    runner.context = SimpleNamespace(
        observed_at=datetime.fromisoformat("2026-09-04T12:00:00+00:00")
    )
    runner.adapter = SimpleNamespace(
        watermark_signal=lambda: {
            "source_count": 17,
            "updated_at": "2026-09-04T11:59:00+00:00",
        },
        data_consistency_signal=lambda: counts,
    )
    if mode == "corrupt":
        with pytest.raises(CheckFailed, match="failed reconciliation"):
            runner._watermark_freshness()
    elif mode == "empty":
        with pytest.raises(EvidenceUnknown, match="insufficient"):
            runner._watermark_freshness()
    else:
        observation = runner._watermark_freshness()
        assert observation.evidence["consistency"] == counts
