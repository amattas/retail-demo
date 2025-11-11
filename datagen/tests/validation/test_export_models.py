#!/usr/bin/env python3
"""Quick validation script for export models."""

from datetime import date, datetime
from src.retail_datagen.api.export_models import (
    ExportRequest,
    FactExportRequest,
    ExportOperationResult,
    ExportStatusResponse,
    ExportSummaryResponse,
    ExportTableInfo,
    validate_table_names,
    VALID_MASTER_TABLES,
    VALID_FACT_TABLES,
)


def test_basic_imports():
    """Test that all models can be imported."""
    print("✓ All models imported successfully")


def test_constants():
    """Test that table constants are defined correctly."""
    assert len(VALID_MASTER_TABLES) == 6, f"Expected 6 master tables, got {len(VALID_MASTER_TABLES)}"
    assert len(VALID_FACT_TABLES) == 9, f"Expected 9 fact tables, got {len(VALID_FACT_TABLES)}"
    print(f"✓ VALID_MASTER_TABLES: {len(VALID_MASTER_TABLES)} tables")
    print(f"✓ VALID_FACT_TABLES: {len(VALID_FACT_TABLES)} tables")


def test_export_request():
    """Test ExportRequest model."""
    # Test with 'all' tables
    req1 = ExportRequest(format="parquet", tables="all")
    assert req1.format == "parquet"
    assert req1.tables == "all"
    print(f"✓ ExportRequest created: format={req1.format}, tables={req1.tables}")

    # Test with specific tables
    req2 = ExportRequest(format="parquet", tables=["stores", "customers"])
    assert req2.format == "parquet"
    assert len(req2.tables) == 2
    print(f"✓ ExportRequest created with specific tables: {req2.tables}")


def test_fact_export_request():
    """Test FactExportRequest model."""
    req = FactExportRequest(
        format="parquet",
        tables=["receipts"],
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31)
    )
    assert req.format == "parquet"
    assert req.start_date == date(2024, 1, 1)
    assert req.end_date == date(2024, 1, 31)
    print(f"✓ FactExportRequest created with date range: {req.start_date} to {req.end_date}")


def test_date_range_validation():
    """Test that date range validation works."""
    try:
        # This should fail - end date before start date
        FactExportRequest(
            format="parquet",
            tables="all",
            start_date=date(2024, 1, 31),
            end_date=date(2024, 1, 1)
        )
        print("✗ Should have raised ValueError for invalid date range")
        return False
    except ValueError as e:
        print(f"✓ Correctly rejected invalid date range: {str(e)[:60]}...")
        return True


def test_export_operation_result():
    """Test ExportOperationResult model."""
    result = ExportOperationResult(
        success=True,
        message="Export started",
        task_id="export_test_123",
        started_at=datetime.now()
    )
    assert result.success is True
    assert result.task_id == "export_test_123"
    print(f"✓ ExportOperationResult created: task_id={result.task_id}")


def test_export_status_response():
    """Test ExportStatusResponse model."""
    status = ExportStatusResponse(
        task_id="export_test_123",
        status="running",
        progress=0.5,
        message="Exporting stores",
        tables_completed=["geographies_master"],
        tables_remaining=["stores", "customers"],
        current_table="stores"
    )
    assert status.progress == 0.5
    assert status.status == "running"
    assert len(status.tables_completed) == 1
    print(f"✓ ExportStatusResponse created: progress={status.progress}, status={status.status}")


def test_validate_table_names():
    """Test validate_table_names function."""
    # Test with valid master tables
    result = validate_table_names(["stores", "customers"], "master")
    assert len(result) == 2
    print(f"✓ validate_table_names works: {result}")

    # Test with 'all' for master
    result = validate_table_names("all", "master")
    assert len(result) == 6
    print(f"✓ validate_table_names 'all' master: {len(result)} tables")

    # Test with 'all' for facts
    result = validate_table_names("all", "facts")
    assert len(result) == 9
    print(f"✓ validate_table_names 'all' facts: {len(result)} tables")

    # Test with invalid table
    try:
        validate_table_names(["invalid_table"], "master")
        print("✗ Should have raised ValueError for invalid table")
        return False
    except ValueError as e:
        print(f"✓ Correctly rejected invalid table: {str(e)[:60]}...")
        return True


def test_export_table_info():
    """Test ExportTableInfo model."""
    info = ExportTableInfo(
        table_name="receipts",
        row_count=1000,
        file_count=3,
        file_paths=["file1.parquet", "file2.parquet", "file3.parquet"],
        export_format="parquet",
        date_range=(date(2024, 1, 1), date(2024, 1, 3))
    )
    assert info.table_name == "receipts"
    assert info.row_count == 1000
    assert info.file_count == 3
    print(f"✓ ExportTableInfo created: table={info.table_name}, rows={info.row_count}")


def test_export_summary_response():
    """Test ExportSummaryResponse model."""
    summary = ExportSummaryResponse(
        task_id="export_test_123",
        status="completed",
        total_tables=3,
        total_files=9,
        total_rows=5000,
        output_directory="/path/to/exports",
        export_format="parquet",
        tables=[],
        started_at=datetime.now(),
        completed_at=datetime.now(),
        duration_seconds=120.5
    )
    assert summary.status == "completed"
    assert summary.total_tables == 3
    assert summary.duration_seconds == 120.5
    print(f"✓ ExportSummaryResponse created: tables={summary.total_tables}, rows={summary.total_rows}")


def main():
    """Run all tests."""
    print("Testing export models...\n")

    test_basic_imports()
    test_constants()
    test_export_request()
    test_fact_export_request()
    test_date_range_validation()
    test_export_operation_result()
    test_export_status_response()
    test_validate_table_names()
    test_export_table_info()
    test_export_summary_response()

    print("\n✓ All tests passed!")


if __name__ == "__main__":
    main()
