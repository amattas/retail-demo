"""Execute the action notebook body with explicit Fabric/Delta/Kusto boundaries."""

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import pytest

SOURCE = (
    Path(__file__).resolve().parents[2]
    / "fabric"
    / "operations"
    / "prepare-replenishment-case.py"
)
CASE_ID = "11111111-1111-4111-8111-111111111111"


def runtime(monkeypatch, *, status="DRAFT", source_time=None):
    sql_module = ModuleType("pyspark.sql")
    sql_module.functions = SimpleNamespace(
        **{name: MagicMock() for name in ("col", "lit", "current_timestamp")},
    )
    monkeypatch.setitem(sys.modules, "pyspark", ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql_module)
    spark = MagicMock()
    reader = spark.read.format.return_value
    reader.option.return_value = reader
    source = reader.load.return_value
    row = SimpleNamespace(
        store_id=1, product_id=2,
        source_as_of=source_time or datetime.now(UTC) - timedelta(days=1),
        forecast_as_of=datetime.now(UTC) - timedelta(hours=1),
        shortage_units=10,
        forecast_units_7d=20.0,
    )
    source.collect.return_value = [row]
    spark.table.return_value.where.return_value.select.return_value.collect.return_value = [
        SimpleNamespace(status=status),
    ]
    namespace = {
        "spark": spark,
        "notebookutils": MagicMock(),
        "CASE_ID": CASE_ID,
        "LAKEHOUSE_NAME": "retail_lakehouse",
        "KUSTO_URI": "https://example.kusto.fabric.microsoft.com",
        "KQL_DATABASE": "retail",
        "PLANNING_REVIEW_TARGET": "the existing report through SSO",
    }
    return namespace, spark, source, row


def test_action_freezes_inspected_evidence_and_writes_only_draft(monkeypatch, capsys):
    namespace, spark, source, row = runtime(monkeypatch)
    exec(compile(SOURCE.read_text(), str(SOURCE), "exec"), namespace)
    source.collect.assert_called_once()
    assert spark.createDataFrame.call_args_list[0].args == ([row],)
    assert spark.createDataFrame.call_args_list[0].kwargs == {"schema": source.schema}
    assert "No order or transfer has been executed" in capsys.readouterr().out
    statements = [call.args[0] for call in spark.sql.call_args_list]
    assert all(
        ".ag." not in statement and ".au." not in statement for statement in statements
    )


@pytest.mark.parametrize("status", ["APPROVED", "RESOLVED", "UNKNOWN"])
def test_non_draft_lifecycle_is_never_reported_or_audited_as_draft(monkeypatch, status):
    namespace, spark, _source, _row = runtime(monkeypatch, status=status)
    with pytest.raises(RuntimeError):
        exec(compile(SOURCE.read_text(), str(SOURCE), "exec"), namespace)
    assert spark.createDataFrame.call_count == 1


@pytest.mark.parametrize("store_id,product_id", [(6, 2), (1, 21)])
def test_out_of_cohort_cases_fail_before_writes(monkeypatch, store_id, product_id):
    namespace, spark, _source, row = runtime(monkeypatch)
    row.store_id, row.product_id = store_id, product_id
    with pytest.raises(RuntimeError, match="cohort"):
        exec(compile(SOURCE.read_text(), str(SOURCE), "exec"), namespace)
    spark.sql.assert_not_called()


@pytest.mark.parametrize("offset", [-8, 1])
def test_stale_and_future_evidence_fails_before_delta_writes(monkeypatch, offset):
    namespace, spark, _source, _row = runtime(
        monkeypatch,
        source_time=datetime.now(UTC) + timedelta(days=offset),
    )
    with pytest.raises(RuntimeError, match="stale or future"):
        exec(compile(SOURCE.read_text(), str(SOURCE), "exec"), namespace)
    spark.sql.assert_not_called()
