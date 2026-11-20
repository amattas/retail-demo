"""Execute bounded reconciliation against equivalent in-memory SQL tables."""

import json
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from retail_setup.contracts import load_repository_manifest

from deploy.scripts._ontology_model import load_schemas
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

ROOT = Path(__file__).resolve().parents[2]


def _watermark_columns() -> tuple[str, ...]:
    notebook = json.loads(
        (ROOT / "fabric" / "lakehouse" / "03-streaming-to-silver.ipynb").read_text(
            encoding="utf-8"
        )
    )
    source = "".join("".join(cell["source"]) for cell in notebook["cells"])
    body = re.search(
        r"CREATE TABLE IF NOT EXISTS \{WATERMARK_TABLE\} \((.*?)\)\s*USING DELTA",
        source,
        re.DOTALL,
    )
    assert body, "notebook 03 no longer defines the watermark table"
    return tuple(line.split()[0] for line in body.group(1).split(",") if line.strip())


def _physical_schemas() -> dict[str, dict[str, tuple[str, ...]]]:
    """Build every table the query reads from its authoritative definition.

    ``cusn`` comes from the Eventhouse DDL, ``ag``/``au`` from the Lakehouse
    contract, and ``ag._watermarks`` from notebook 03, so a column renamed in
    any source makes the reconciliation query fail here as it would live.
    """

    schemas = load_schemas(ROOT)
    lakehouse = {
        name: tuple(column for column, _ in columns)
        for name, columns in schemas.silver.items()
    }
    return {
        "ag": {**lakehouse, "_watermarks": _watermark_columns()},
        "au": lakehouse,
        "cusn": {
            name: tuple(column for column, _ in columns)
            for name, columns in schemas.bronze.items()
        },
    }


def _insert(connection, table, rows):
    for row in rows:
        names = ", ".join(f'"{name}"' for name in row)
        marks = ", ".join("?" for _ in row)
        connection.execute(f"INSERT INTO {table}({names}) VALUES ({marks})", tuple(row.values()))


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
    for schema, tables in _physical_schemas().items():
        for table, columns in tables.items():
            column_sql = ", ".join(f'"{name}"' for name in columns)
            connection.execute(f"CREATE TABLE {schema}.{table}({column_sql})")
    _insert(connection, "ag._watermarks", [
        {"source_table": "receipt_created", "last_processed_ts": "2026-09-04 12:00:00"},
        {"source_table": "inventory_updated", "last_processed_ts": "2026-09-04 12:00:00"},
    ])
    _insert(connection, "ag.fact_receipts", [
        {"receipt_id_ext": "R1", "store_id": 1, "customer_id": 1,
         "event_ts": "2026-09-04 08:00:00", "total_cents": 1234},
        {"receipt_id_ext": "R2", "store_id": 1, "customer_id": 1,
         "event_ts": "2026-09-04 10:00:00", "total_cents": 2000},
    ])
    _insert(connection, "ag.fact_store_inventory_txn", [
        {"trace_id": "I1", "event_ts": "2026-09-04 08:00:00", "balance": 42},
    ])
    _insert(connection, "ag.dim_stores", [{"ID": 1}])
    _insert(connection, "ag.dim_customers", [{"ID": 1}])
    _insert(connection, "au.sales_minute_store", [
        {"store_id": 1, "ts": "2026-09-04 08:00:00", "receipts": 1, "total_sales": 12.34},
        {"store_id": 1, "ts": "2026-09-04 10:00:00", "receipts": 1, "total_sales": 20.00},
    ])
    _insert(connection, "cusn.receipt_created", [
        {"receipt_id": "R1", "ingest_timestamp": "2026-09-04 08:00:00"},
        {"receipt_id": "R2", "ingest_timestamp": "2026-09-04 10:00:00"},
        {"receipt_id": "NOT_VISIBLE_YET", "ingest_timestamp": "2026-09-04 12:05:00"},
    ])
    _insert(connection, "cusn.inventory_updated", [
        {"trace_id": "I1", "ingest_timestamp": "2026-09-04 08:00:00"},
    ])
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
        observed_at=datetime.fromisoformat("2026-09-04T12:00:00+00:00"),
        manifest=load_repository_manifest(ROOT)[0],
        repo_root=ROOT,
    )
    runner.adapter = SimpleNamespace(
        watermark_signal=lambda: {
            "source_count": 17,
            "updated_at": "2026-09-04T11:59:00+00:00",
            "sources": {"receipt_created": "2026-09-04T11:58:00+00:00"},
        },
        eventhouse_signal=lambda _tables: {
            "table_ingestion": {
                "receipt_created": {
                    "earliest": "2026-09-01T00:00:00+00:00",
                    "latest": "2026-09-04T11:58:00+00:00",
                }
            }
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
