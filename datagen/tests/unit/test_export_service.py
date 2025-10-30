"""
Unit tests for ExportService orchestrator.

Tests the main export service that coordinates database reading,
format writing, and file management for master and fact table exports.
"""

import pytest
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch, call
import pandas as pd

from retail_datagen.services.export_service import ExportService


class TestExportServiceInit:
    """Test ExportService initialization."""

    def test_init_with_valid_path(self, tmp_path):
        """Should initialize with valid base directory."""
        service = ExportService(base_dir=tmp_path)

        assert service.base_dir == tmp_path
        assert service.file_manager is not None
        assert service.file_manager.base_dir == tmp_path

    def test_init_creates_file_manager(self, tmp_path):
        """Should create ExportFileManager instance."""
        service = ExportService(base_dir=tmp_path)

        from retail_datagen.services.file_manager import ExportFileManager
        assert isinstance(service.file_manager, ExportFileManager)


class TestExportServiceGetWriter:
    """Test writer factory method."""

    def test_get_writer_csv(self, tmp_path):
        """Should return CSVWriter for csv format."""
        service = ExportService(base_dir=tmp_path)

        writer = service._get_writer("csv")

        from retail_datagen.services.writers import CSVWriter
        assert isinstance(writer, CSVWriter)
        assert writer.index is False

    def test_get_writer_parquet(self, tmp_path):
        """Should return ParquetWriter for parquet format."""
        service = ExportService(base_dir=tmp_path)

        writer = service._get_writer("parquet")

        from retail_datagen.services.writers import ParquetWriter
        assert isinstance(writer, ParquetWriter)
        assert writer.engine == "pyarrow"
        assert writer.compression == "snappy"

    def test_get_writer_invalid_format(self, tmp_path):
        """Should raise ValueError for invalid format."""
        service = ExportService(base_dir=tmp_path)

        with pytest.raises(ValueError, match="Unsupported format"):
            service._get_writer("invalid")  # type: ignore


class TestExportMasterTables:
    """Test export_master_tables method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        return AsyncMock()

    @pytest.fixture
    def sample_master_data(self):
        """Create sample master table data."""
        return {
            "dim_geographies": pd.DataFrame({
                "ID": [1, 2, 3],
                "City": ["Springfield", "Riverside", "Franklin"],
                "State": ["IL", "CA", "TN"],
            }),
            "dim_stores": pd.DataFrame({
                "ID": [1, 2],
                "StoreNumber": ["ST001", "ST002"],
                "Address": ["123 Main St", "456 Oak Ave"],
            }),
            "dim_customers": pd.DataFrame({
                "ID": [1, 2, 3, 4],
                "FirstName": ["Alex", "Blake", "Casey", "Drew"],
                "LastName": ["Anderson", "Brightwell", "Clearwater", "Dalewood"],
            }),
        }

    @pytest.mark.asyncio
    async def test_export_master_tables_csv_success(
        self, tmp_path, mock_session, sample_master_data
    ):
        """Should export all master tables to CSV format successfully."""
        service = ExportService(base_dir=tmp_path)

        with patch('retail_datagen.services.db_reader.read_all_master_tables') as mock_read:
            mock_read.return_value = sample_master_data

            result = await service.export_master_tables(
                mock_session,
                format="csv"
            )

        # Verify all tables were exported
        assert len(result) == 3
        assert "dim_geographies" in result
        assert "dim_stores" in result
        assert "dim_customers" in result

        # Verify files exist
        for table_name, file_path in result.items():
            assert file_path.exists()
            assert file_path.suffix == ".csv"
            assert file_path.parent.name == "master"

            # Verify content
            df = pd.read_csv(file_path)
            assert len(df) == len(sample_master_data[table_name])

    @pytest.mark.asyncio
    async def test_export_master_tables_parquet_success(
        self, tmp_path, mock_session, sample_master_data
    ):
        """Should export all master tables to Parquet format successfully."""
        service = ExportService(base_dir=tmp_path)

        with patch('retail_datagen.services.db_reader.read_all_master_tables') as mock_read:
            mock_read.return_value = sample_master_data

            result = await service.export_master_tables(
                mock_session,
                format="parquet"
            )

        # Verify all tables were exported
        assert len(result) == 3

        # Verify files exist with correct format
        for table_name, file_path in result.items():
            assert file_path.exists()
            assert file_path.suffix == ".parquet"

            # Verify content
            df = pd.read_parquet(file_path)
            assert len(df) == len(sample_master_data[table_name])

    @pytest.mark.asyncio
    async def test_export_master_tables_with_progress_callback(
        self, tmp_path, mock_session, sample_master_data
    ):
        """Should invoke progress callback for each table."""
        service = ExportService(base_dir=tmp_path)
        progress_calls = []

        def progress_callback(message: str, current: int, total: int):
            progress_calls.append((message, current, total))

        with patch('retail_datagen.services.db_reader.read_all_master_tables') as mock_read:
            mock_read.return_value = sample_master_data

            await service.export_master_tables(
                mock_session,
                format="csv",
                progress_callback=progress_callback
            )

        # Verify progress callback was called for each table
        assert len(progress_calls) == 3
        assert all(total == 3 for _, _, total in progress_calls)
        assert progress_calls[0][1] == 1  # First table
        assert progress_calls[1][1] == 2  # Second table
        assert progress_calls[2][1] == 3  # Third table

    @pytest.mark.asyncio
    async def test_export_master_tables_skips_empty_tables(
        self, tmp_path, mock_session
    ):
        """Should skip empty tables without error."""
        service = ExportService(base_dir=tmp_path)

        master_data = {
            "dim_stores": pd.DataFrame({
                "ID": [1, 2],
                "StoreNumber": ["ST001", "ST002"],
            }),
            "dim_empty": pd.DataFrame(),  # Empty table
        }

        with patch('retail_datagen.services.db_reader.read_all_master_tables') as mock_read:
            mock_read.return_value = master_data

            result = await service.export_master_tables(
                mock_session,
                format="csv"
            )

        # Only non-empty table should be in result
        assert len(result) == 1
        assert "dim_stores" in result
        assert "dim_empty" not in result

    @pytest.mark.asyncio
    async def test_export_master_tables_cleanup_on_failure(
        self, tmp_path, mock_session, sample_master_data
    ):
        """Should cleanup partial exports on failure."""
        service = ExportService(base_dir=tmp_path)

        with patch('retail_datagen.services.db_reader.read_all_master_tables') as mock_read:
            mock_read.return_value = sample_master_data

            # Mock writer to fail on second table
            with patch.object(service, '_get_writer') as mock_get_writer:
                mock_writer = Mock()
                mock_writer.write.side_effect = [
                    None,  # First table succeeds
                    IOError("Disk full"),  # Second table fails
                ]
                mock_get_writer.return_value = mock_writer

                with pytest.raises(IOError, match="Disk full"):
                    await service.export_master_tables(
                        mock_session,
                        format="csv"
                    )

        # Verify cleanup was attempted
        # File manager should have no tracked files after cleanup
        assert service.file_manager.get_tracked_file_count() == 0

    @pytest.mark.asyncio
    async def test_export_master_tables_tracks_files_correctly(
        self, tmp_path, mock_session, sample_master_data
    ):
        """Should track files during export for potential rollback."""
        service = ExportService(base_dir=tmp_path)

        with patch('retail_datagen.services.db_reader.read_all_master_tables') as mock_read:
            mock_read.return_value = sample_master_data

            # Mock file manager to spy on tracking calls
            with patch.object(service.file_manager, 'track_file', wraps=service.file_manager.track_file) as mock_track:
                await service.export_master_tables(
                    mock_session,
                    format="csv"
                )

                # Verify track_file was called for each table
                assert mock_track.call_count == 3

    @pytest.mark.asyncio
    async def test_export_master_tables_resets_tracking_on_success(
        self, tmp_path, mock_session, sample_master_data
    ):
        """Should reset file tracking on successful export."""
        service = ExportService(base_dir=tmp_path)

        with patch('retail_datagen.services.db_reader.read_all_master_tables') as mock_read:
            mock_read.return_value = sample_master_data

            await service.export_master_tables(
                mock_session,
                format="csv"
            )

        # File tracking should be reset after success
        assert service.file_manager.get_tracked_file_count() == 0


class TestExportFactTables:
    """Test export_fact_tables method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        return AsyncMock()

    @pytest.fixture
    def sample_fact_data(self):
        """Create sample fact table data with event_ts column."""
        return {
            "fact_receipts": pd.DataFrame({
                "TraceId": ["trace1", "trace2", "trace3"],
                "event_ts": pd.to_datetime([
                    "2024-01-01 10:00:00",
                    "2024-01-01 14:30:00",
                    "2024-01-02 09:15:00",
                ]),
                "StoreID": [1, 2, 1],
                "Total": [25.99, 49.98, 35.50],
            }),
            "fact_receipt_lines": pd.DataFrame({
                "TraceId": ["trace1", "trace2"],
                "event_ts": pd.to_datetime([
                    "2024-01-01 10:00:00",
                    "2024-01-02 09:15:00",
                ]),
                "ReceiptId": ["RCP001", "RCP002"],
                "ProductID": [1, 2],
                "Qty": [2, 1],
            }),
        }

    @pytest.mark.asyncio
    async def test_export_fact_tables_csv_success(
        self, tmp_path, mock_session, sample_fact_data
    ):
        """Should export all fact tables to CSV format with partitioning."""
        service = ExportService(base_dir=tmp_path)

        with patch('retail_datagen.services.db_reader.read_all_fact_tables') as mock_read:
            mock_read.return_value = sample_fact_data

            result = await service.export_fact_tables(
                mock_session,
                format="csv"
            )

        # Verify all tables were exported
        assert len(result) == 2
        assert "fact_receipts" in result
        assert "fact_receipt_lines" in result

        # Verify partition structure for fact_receipts (2 dates)
        receipts_files = result["fact_receipts"]
        assert len(receipts_files) == 2

        # Verify partition file paths follow pattern
        for file_path in receipts_files:
            assert file_path.exists()
            assert file_path.suffix == ".csv"
            assert "dt=" in str(file_path.parent)  # Partition directory

    @pytest.mark.asyncio
    async def test_export_fact_tables_parquet_success(
        self, tmp_path, mock_session, sample_fact_data
    ):
        """Should export all fact tables to Parquet format with partitioning."""
        service = ExportService(base_dir=tmp_path)

        with patch('retail_datagen.services.db_reader.read_all_fact_tables') as mock_read:
            mock_read.return_value = sample_fact_data

            result = await service.export_fact_tables(
                mock_session,
                format="parquet"
            )

        # Verify files have correct extension
        for table_name, files in result.items():
            for file_path in files:
                assert file_path.suffix == ".parquet"

    @pytest.mark.asyncio
    async def test_export_fact_tables_with_date_filtering(
        self, tmp_path, mock_session, sample_fact_data
    ):
        """Should pass date filters to database reader."""
        service = ExportService(base_dir=tmp_path)
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 31)

        with patch('retail_datagen.services.db_reader.read_all_fact_tables') as mock_read:
            mock_read.return_value = sample_fact_data

            await service.export_fact_tables(
                mock_session,
                format="csv",
                start_date=start_date,
                end_date=end_date
            )

            # Verify date filters were passed to reader
            mock_read.assert_called_once_with(
                mock_session,
                start_date,
                end_date
            )

    @pytest.mark.asyncio
    async def test_export_fact_tables_with_progress_callback(
        self, tmp_path, mock_session, sample_fact_data
    ):
        """Should invoke progress callback for each table."""
        service = ExportService(base_dir=tmp_path)
        progress_calls = []

        def progress_callback(message: str, current: int, total: int):
            progress_calls.append((message, current, total))

        with patch('retail_datagen.services.db_reader.read_all_fact_tables') as mock_read:
            mock_read.return_value = sample_fact_data

            await service.export_fact_tables(
                mock_session,
                format="csv",
                progress_callback=progress_callback
            )

        # Verify progress callback was called for each table
        assert len(progress_calls) == 2
        assert all(total == 2 for _, _, total in progress_calls)

    @pytest.mark.asyncio
    async def test_export_fact_tables_skips_empty_tables(
        self, tmp_path, mock_session
    ):
        """Should skip empty tables and return empty list."""
        service = ExportService(base_dir=tmp_path)

        fact_data = {
            "fact_receipts": pd.DataFrame({
                "TraceId": ["trace1"],
                "event_ts": pd.to_datetime(["2024-01-01 10:00:00"]),
            }),
            "fact_empty": pd.DataFrame(),  # Empty table
        }

        with patch('retail_datagen.services.db_reader.read_all_fact_tables') as mock_read:
            mock_read.return_value = fact_data

            result = await service.export_fact_tables(
                mock_session,
                format="csv"
            )

        # Empty table should have empty list
        assert "fact_empty" in result
        assert result["fact_empty"] == []

    @pytest.mark.asyncio
    async def test_export_fact_tables_missing_event_ts_column(
        self, tmp_path, mock_session
    ):
        """Should raise ValueError if event_ts column is missing."""
        service = ExportService(base_dir=tmp_path)

        fact_data = {
            "fact_receipts": pd.DataFrame({
                "TraceId": ["trace1"],
                "StoreID": [1],
                # Missing event_ts column
            }),
        }

        with patch('retail_datagen.services.db_reader.read_all_fact_tables') as mock_read:
            mock_read.return_value = fact_data

            with pytest.raises(ValueError, match="must have event_ts column"):
                await service.export_fact_tables(
                    mock_session,
                    format="csv"
                )

    @pytest.mark.asyncio
    async def test_export_fact_tables_partitions_by_date(
        self, tmp_path, mock_session
    ):
        """Should create separate files for each date partition."""
        service = ExportService(base_dir=tmp_path)

        # Data spanning 3 dates
        fact_data = {
            "fact_receipts": pd.DataFrame({
                "TraceId": ["t1", "t2", "t3", "t4"],
                "event_ts": pd.to_datetime([
                    "2024-01-01 10:00:00",
                    "2024-01-01 14:00:00",
                    "2024-01-02 09:00:00",
                    "2024-01-03 11:00:00",
                ]),
                "StoreID": [1, 2, 1, 2],
            }),
        }

        with patch('retail_datagen.services.db_reader.read_all_fact_tables') as mock_read:
            mock_read.return_value = fact_data

            result = await service.export_fact_tables(
                mock_session,
                format="csv"
            )

        # Should have 3 partition files (one per date)
        assert len(result["fact_receipts"]) == 3

        # Verify partition directories
        partition_dirs = {p.parent.name for p in result["fact_receipts"]}
        assert "dt=2024-01-01" in partition_dirs
        assert "dt=2024-01-02" in partition_dirs
        assert "dt=2024-01-03" in partition_dirs

    @pytest.mark.asyncio
    async def test_export_fact_tables_cleanup_on_failure(
        self, tmp_path, mock_session, sample_fact_data
    ):
        """Should cleanup partial exports on failure."""
        service = ExportService(base_dir=tmp_path)

        with patch('retail_datagen.services.db_reader.read_all_fact_tables') as mock_read:
            mock_read.return_value = sample_fact_data

            # Mock writer to fail during processing
            with patch.object(service, '_get_writer') as mock_get_writer:
                mock_writer = Mock()
                mock_writer.write.side_effect = IOError("Write failed")
                mock_get_writer.return_value = mock_writer

                with pytest.raises(IOError):
                    await service.export_fact_tables(
                        mock_session,
                        format="csv"
                    )

        # Verify cleanup was called
        assert service.file_manager.get_tracked_file_count() == 0

    @pytest.mark.asyncio
    async def test_export_fact_tables_removes_temp_dt_column(
        self, tmp_path, mock_session, sample_fact_data
    ):
        """Should remove temporary dt column before writing."""
        service = ExportService(base_dir=tmp_path)

        with patch('retail_datagen.services.db_reader.read_all_fact_tables') as mock_read:
            mock_read.return_value = sample_fact_data

            result = await service.export_fact_tables(
                mock_session,
                format="csv"
            )

        # Verify written files don't have dt column
        for file_path in result["fact_receipts"]:
            df = pd.read_csv(file_path)
            assert "dt" not in df.columns
            assert "event_ts" in df.columns

    @pytest.mark.asyncio
    async def test_export_fact_tables_resets_tracking_on_success(
        self, tmp_path, mock_session, sample_fact_data
    ):
        """Should reset file tracking on successful export."""
        service = ExportService(base_dir=tmp_path)

        with patch('retail_datagen.services.db_reader.read_all_fact_tables') as mock_read:
            mock_read.return_value = sample_fact_data

            await service.export_fact_tables(
                mock_session,
                format="csv"
            )

        # File tracking should be reset after success
        assert service.file_manager.get_tracked_file_count() == 0


class TestExportServiceIntegration:
    """Integration tests for ExportService with real writers."""

    @pytest.mark.asyncio
    async def test_full_export_workflow_csv(self, tmp_path):
        """Should perform complete export workflow with CSV format."""
        service = ExportService(base_dir=tmp_path)
        mock_session = AsyncMock()

        master_data = {
            "dim_stores": pd.DataFrame({
                "ID": [1, 2],
                "StoreNumber": ["ST001", "ST002"],
            }),
        }

        with patch('retail_datagen.services.db_reader.read_all_master_tables') as mock_read:
            mock_read.return_value = master_data

            result = await service.export_master_tables(
                mock_session,
                format="csv"
            )

        # Verify end-to-end: file exists and has correct content
        stores_file = result["dim_stores"]
        assert stores_file.exists()

        df = pd.read_csv(stores_file)
        assert len(df) == 2
        assert list(df.columns) == ["ID", "StoreNumber"]

    @pytest.mark.asyncio
    async def test_full_export_workflow_parquet(self, tmp_path):
        """Should perform complete export workflow with Parquet format."""
        service = ExportService(base_dir=tmp_path)
        mock_session = AsyncMock()

        master_data = {
            "dim_products": pd.DataFrame({
                "ID": [1, 2, 3],
                "ProductName": ["Widget", "Gadget", "Tool"],
            }),
        }

        with patch('retail_datagen.services.db_reader.read_all_master_tables') as mock_read:
            mock_read.return_value = master_data

            result = await service.export_master_tables(
                mock_session,
                format="parquet"
            )

        # Verify end-to-end: file exists and has correct content
        products_file = result["dim_products"]
        assert products_file.exists()

        df = pd.read_parquet(products_file)
        assert len(df) == 3
        assert list(df.columns) == ["ID", "ProductName"]
