"""Unit tests for writer.py's Fabric Lakehouse Delta backend and identifier
sanitization. These exercise ``_LakehouseBackend`` in isolation with a mocked
Spark session (no JVM/Java or real Delta table needed) so they run in any
environment that has the ``pyspark`` Python package installed, matching the
publication coordinator's own no-Fabric-required test strategy.
"""

from unittest.mock import MagicMock

import pytest

from retail_setup.generation.publication import TableTarget, TargetState
from retail_setup.generation.writer import _LakehouseBackend, sanitize_identifier


def _field(name: str, type_str: str) -> MagicMock:
    field = MagicMock()
    field.name = name
    field.dataType.simpleString.return_value = type_str
    return field


def _fake_df(schema_fields: list, count: int) -> MagicMock:
    df = MagicMock()
    df.schema.fields = schema_fields
    df.count.return_value = count
    return df


def test_sanitize_identifier_replaces_unsafe_characters():
    assert (
        sanitize_identifier("setup-hardware-42-20260101T000000Z-abcd1234")
        == "setup_hardware_42_20260101T000000Z_abcd1234"
    )
    assert sanitize_identifier("already_safe_123") == "already_safe_123"


def test_lakehouse_backend_stage_creates_staging_db_once_and_writes_delta():
    spark = MagicMock()
    df = MagicMock()
    spark.table.return_value.count.return_value = 42
    target = TableTarget(name="fact_receipts", db="ag", staging_name="lh.ag_stage.run1__fact_receipts")
    backend = _LakehouseBackend(spark, "lh", "run-1", {("ag", "fact_receipts"): df})

    count = backend.stage(target)

    spark.sql.assert_called_once_with("CREATE DATABASE IF NOT EXISTS lh.ag_stage")
    df.write.format.assert_called_once_with("delta")
    df.write.format.return_value.mode.assert_called_once_with("overwrite")
    df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
        target.staging_name
    )
    spark.table.assert_called_once_with(target.staging_name)
    assert count == 42

    # a second table in the same db must not re-create the staging schema.
    target2 = TableTarget(name="dim_date", db="ag", staging_name="lh.ag_stage.run1__dim_date")
    backend.sources[("ag", "dim_date")] = MagicMock()
    backend.stage(target2)
    assert spark.sql.call_count == 1  # still just the one CREATE DATABASE call


def test_lakehouse_backend_validate_passes_on_matching_schema_and_count():
    spark = MagicMock()
    source_df = _fake_df([_field("id", "bigint")], count=5)
    spark.table.return_value = _fake_df([_field("id", "bigint")], count=5)
    target = TableTarget(name="t", db="ag", staging_name="lh.ag_stage.run1__t")
    backend = _LakehouseBackend(spark, "lh", "run-1", {("ag", "t"): source_df})

    backend.validate(target, staged_row_count=5)  # no raise


def test_lakehouse_backend_validate_raises_on_row_count_mismatch():
    spark = MagicMock()
    source_df = _fake_df([_field("id", "bigint")], count=5)
    spark.table.return_value = _fake_df([_field("id", "bigint")], count=5)
    target = TableTarget(name="t", db="ag", staging_name="lh.ag_stage.run1__t")
    backend = _LakehouseBackend(spark, "lh", "run-1", {("ag", "t"): source_df})

    with pytest.raises(ValueError, match="row count mismatch"):
        backend.validate(target, staged_row_count=4)


def test_lakehouse_backend_validate_raises_on_schema_mismatch():
    spark = MagicMock()
    source_df = _fake_df([_field("id", "bigint")], count=5)
    spark.table.return_value = _fake_df([_field("id", "string")], count=5)
    target = TableTarget(name="t", db="ag", staging_name="lh.ag_stage.run1__t")
    backend = _LakehouseBackend(spark, "lh", "run-1", {("ag", "t"): source_df})

    with pytest.raises(ValueError, match="schema mismatch"):
        backend.validate(target, staged_row_count=5)


def test_lakehouse_backend_target_state_reports_absent_target():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = False
    backend = _LakehouseBackend(spark, "lh", "run-1", {})
    target = TableTarget(name="fact_receipts", db="ag", staging_name="lh.ag_stage.run1__fact_receipts")

    state = backend.target_state(target)

    assert state == TargetState(existed=False)
    spark.catalog.tableExists.assert_called_once_with("lh.ag.fact_receipts")


def test_lakehouse_backend_target_state_captures_delta_version_for_existing_target():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    history_row = MagicMock()
    history_row.__getitem__.return_value = 9
    spark.sql.return_value.collect.return_value = [history_row]
    backend = _LakehouseBackend(spark, "lh", "run-1", {})
    target = TableTarget(name="fact_receipts", db="ag", staging_name="lh.ag_stage.run1__fact_receipts")

    state = backend.target_state(target)

    assert state.existed is True
    assert state.restore_token == 9
    spark.sql.assert_called_once_with("DESCRIBE HISTORY lh.ag.fact_receipts LIMIT 1")


def test_lakehouse_backend_promote_uses_create_or_replace_table():
    spark = MagicMock()
    spark.table.return_value.count.return_value = 7
    backend = _LakehouseBackend(spark, "lh", "run-1", {})
    target = TableTarget(name="fact_receipts", db="ag", staging_name="lh.ag_stage.run1__fact_receipts")

    count = backend.promote(target)

    spark.sql.assert_called_once_with(
        "CREATE OR REPLACE TABLE lh.ag.fact_receipts USING DELTA AS "
        "SELECT * FROM lh.ag_stage.run1__fact_receipts"
    )
    assert count == 7


def test_lakehouse_backend_restore_uses_restore_table_to_version_as_of():
    spark = MagicMock()
    backend = _LakehouseBackend(spark, "lh", "run-1", {})
    target = TableTarget(name="fact_receipts", db="ag", staging_name="lh.ag_stage.run1__fact_receipts")

    backend.restore(target, TargetState(existed=True, restore_token=3))

    spark.sql.assert_called_once_with("RESTORE TABLE lh.ag.fact_receipts TO VERSION AS OF 3")


def test_lakehouse_backend_drop_and_cleanup_use_drop_table_if_exists():
    spark = MagicMock()
    backend = _LakehouseBackend(spark, "lh", "run-1", {})
    target = TableTarget(name="fact_receipts", db="ag", staging_name="lh.ag_stage.run1__fact_receipts")

    backend.drop(target)
    spark.sql.assert_called_once_with("DROP TABLE IF EXISTS lh.ag.fact_receipts")

    spark.reset_mock()
    backend.cleanup(target)
    spark.sql.assert_called_once_with("DROP TABLE IF EXISTS lh.ag_stage.run1__fact_receipts")
