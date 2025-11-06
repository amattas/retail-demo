"""
Unit tests for database reader service.

Tests database reading functionality for master and fact tables
with support for chunking, filtering, and async operations.
"""

import pytest
from datetime import date, datetime, time
from unittest.mock import AsyncMock, Mock, patch
import pandas as pd

from retail_datagen.services import db_reader
from retail_datagen.services.db_reader import (
    MASTER_TABLES,
    FACT_TABLES,
    DEFAULT_CHUNK_SIZE,
)


class TestGetTableRowCount:
    """Test get_table_row_count function."""

    @pytest.mark.asyncio
    async def test_get_row_count_with_data(self):
        """Should return correct row count for non-empty table."""
        mock_session = AsyncMock()
        mock_result = Mock()
        mock_result.scalar_one.return_value = 42
        mock_session.execute.return_value = mock_result

        from retail_datagen.db.models.master import Store

        count = await db_reader.get_table_row_count(mock_session, Store)

        assert count == 42
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_row_count_empty_table(self):
        """Should return 0 for empty table."""
        mock_session = AsyncMock()
        mock_result = Mock()
        mock_result.scalar_one.return_value = 0
        mock_session.execute.return_value = mock_result

        from retail_datagen.db.models.master import Customer

        count = await db_reader.get_table_row_count(mock_session, Customer)

        assert count == 0

    @pytest.mark.asyncio
    async def test_get_row_count_database_error(self):
        """Should raise exception on database error."""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Database connection failed")

        from retail_datagen.db.models.master import Product

        with pytest.raises(Exception, match="Database connection failed"):
            await db_reader.get_table_row_count(mock_session, Product)


class TestReadMasterTable:
    """Test read_master_table function."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        return AsyncMock()

    @pytest.fixture
    def mock_store_rows(self):
        """Create mock store ORM objects."""
        mock_rows = []
        for i in range(1, 4):
            row = Mock()
            row.ID = i
            row.StoreNumber = f"ST{i:03d}"
            row.Address = f"{i*100} Main St"
            row.GeographyID = i
            mock_rows.append(row)
        return mock_rows

    @pytest.mark.asyncio
    async def test_read_master_table_success(self, mock_session, mock_store_rows):
        """Should read master table and return DataFrame."""
        from retail_datagen.db.models.master import Store

        # Mock count query
        count_result = Mock()
        count_result.scalar_one.return_value = 3

        # Mock data query
        data_result = Mock()
        data_result.scalars.return_value.all.return_value = mock_store_rows

        mock_session.execute.side_effect = [count_result, data_result]

        df = await db_reader.read_master_table(mock_session, Store)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        # At minimum these core columns must be present; additional columns are allowed
        for col in ["ID", "StoreNumber", "Address", "GeographyID"]:
            assert col in df.columns
        assert df["StoreNumber"].tolist() == ["ST001", "ST002", "ST003"]

    @pytest.mark.asyncio
    async def test_read_master_table_empty(self, mock_session):
        """Should return empty DataFrame for empty table."""
        from retail_datagen.db.models.master import Customer

        # Mock count query returning 0
        count_result = Mock()
        count_result.scalar_one.return_value = 0
        mock_session.execute.return_value = count_result

        df = await db_reader.read_master_table(mock_session, Customer)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    @pytest.mark.asyncio
    async def test_read_master_table_invalid_table(self, mock_session):
        """Should raise ValueError for non-master table."""
        from retail_datagen.db.models.facts import Receipt

        with pytest.raises(ValueError, match="Invalid master table"):
            await db_reader.read_master_table(mock_session, Receipt)

    @pytest.mark.asyncio
    async def test_read_master_table_chunking(self, mock_session):
        """Should read large table in chunks."""
        from retail_datagen.db.models.master import Store

        # Create more rows than chunk size
        chunk_size = 2
        total_rows = 5

        # Mock count
        count_result = Mock()
        count_result.scalar_one.return_value = total_rows

        # Mock chunked results
        chunk1 = [Mock(ID=1, StoreNumber="ST001", Address="100 Main", GeographyID=1),
                  Mock(ID=2, StoreNumber="ST002", Address="200 Main", GeographyID=2)]
        chunk2 = [Mock(ID=3, StoreNumber="ST003", Address="300 Main", GeographyID=3),
                  Mock(ID=4, StoreNumber="ST004", Address="400 Main", GeographyID=4)]
        chunk3 = [Mock(ID=5, StoreNumber="ST005", Address="500 Main", GeographyID=5)]

        result1 = Mock()
        result1.scalars.return_value.all.return_value = chunk1
        result2 = Mock()
        result2.scalars.return_value.all.return_value = chunk2
        result3 = Mock()
        result3.scalars.return_value.all.return_value = chunk3

        mock_session.execute.side_effect = [count_result, result1, result2, result3]

        df = await db_reader.read_master_table(
            mock_session,
            Store,
            chunk_size=chunk_size
        )

        # Should combine all chunks
        assert len(df) == 5
        assert df["StoreNumber"].tolist() == ["ST001", "ST002", "ST003", "ST004", "ST005"]

    @pytest.mark.asyncio
    async def test_read_master_table_database_error(self, mock_session):
        """Should raise exception on database error."""
        from retail_datagen.db.models.master import Product

        mock_session.execute.side_effect = Exception("Connection lost")

        with pytest.raises(Exception, match="Connection lost"):
            await db_reader.read_master_table(mock_session, Product)


class TestReadAllMasterTables:
    """Test read_all_master_tables function."""

    @pytest.mark.asyncio
    async def test_read_all_master_tables_success(self):
        """Should read all 6 master tables."""
        mock_session = AsyncMock()

        with patch('retail_datagen.services.db_reader.read_master_table') as mock_read:
            # Mock successful reads for all tables
            mock_read.return_value = pd.DataFrame({"ID": [1, 2, 3]})

            result = await db_reader.read_all_master_tables(mock_session)

        # Should return dict with all 6 master tables
        assert len(result) == 6
        assert "dim_geographies" in result
        assert "dim_stores" in result
        assert "dim_distribution_centers" in result
        assert "dim_trucks" in result
        assert "dim_customers" in result
        assert "dim_products" in result

        # Each table should have data
        for df in result.values():
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 3

    @pytest.mark.asyncio
    async def test_read_all_master_tables_partial_failure(self):
        """Should continue reading other tables if one fails."""
        mock_session = AsyncMock()

        def mock_read_side_effect(session, table_model, chunk_size=DEFAULT_CHUNK_SIZE):
            table_name = table_model.__tablename__
            if table_name == "stores":
                raise Exception("Read failed")
            return pd.DataFrame({"ID": [1, 2]})

        with patch('retail_datagen.services.db_reader.read_master_table') as mock_read:
            mock_read.side_effect = mock_read_side_effect

            result = await db_reader.read_all_master_tables(mock_session)

        # Should have all 6 tables, but failed one is empty
        assert len(result) == 6
        assert len(result["dim_stores"]) == 0  # Failed table
        assert len(result["dim_customers"]) == 2  # Successful table

    @pytest.mark.asyncio
    async def test_read_all_master_tables_passes_chunk_size(self):
        """Should pass chunk_size parameter to read_master_table."""
        mock_session = AsyncMock()
        custom_chunk_size = 5000

        with patch('retail_datagen.services.db_reader.read_master_table') as mock_read:
            mock_read.return_value = pd.DataFrame()

            await db_reader.read_all_master_tables(
                mock_session,
                chunk_size=custom_chunk_size
            )

            # Verify chunk_size was passed to all calls
            for call in mock_read.call_args_list:
                assert call[1].get('chunk_size') == custom_chunk_size


class TestReadFactTable:
    """Test read_fact_table function."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        return AsyncMock()

    @pytest.fixture
    def mock_receipt_rows(self):
        """Create mock receipt ORM objects."""
        mock_rows = []
        for i in range(1, 4):
            row = Mock()
            row.TraceId = f"trace{i}"
            row.event_ts = datetime(2024, 1, i, 10, 0, 0)
            row.StoreID = i
            row.Total = 10.00 * i
            mock_rows.append(row)
        return mock_rows

    @pytest.mark.asyncio
    async def test_read_fact_table_without_filters(
        self, mock_session, mock_receipt_rows
    ):
        """Should read all rows when no date filters provided."""
        from retail_datagen.db.models.facts import Receipt

        # Mock count query
        count_result = Mock()
        count_result.scalar_one.return_value = 3

        # Mock data query
        data_result = Mock()
        data_result.scalars.return_value.all.return_value = mock_receipt_rows

        mock_session.execute.side_effect = [count_result, data_result]

        df = await db_reader.read_fact_table(mock_session, Receipt)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert "event_ts" in df.columns
        assert "TraceId" in df.columns

    @pytest.mark.asyncio
    async def test_read_fact_table_with_start_date(self, mock_session):
        """Should filter by start_date when provided."""
        from retail_datagen.db.models.facts import Receipt

        start_date = date(2024, 1, 15)

        # Mock count and data
        count_result = Mock()
        count_result.scalar_one.return_value = 2
        data_result = Mock()
        data_result.scalars.return_value.all.return_value = [
            Mock(TraceId="t1", event_ts=datetime(2024, 1, 16, 10, 0), StoreID=1, Total=10.0),
        ]

        mock_session.execute.side_effect = [count_result, data_result]

        df = await db_reader.read_fact_table(
            mock_session,
            Receipt,
            start_date=start_date
        )

        assert len(df) == 1

        # Verify WHERE clause was added (check execute was called with filtered statement)
        assert mock_session.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_read_fact_table_with_end_date(self, mock_session):
        """Should filter by end_date when provided."""
        from retail_datagen.db.models.facts import Receipt

        end_date = date(2024, 1, 10)

        count_result = Mock()
        count_result.scalar_one.return_value = 1
        data_result = Mock()
        data_result.scalars.return_value.all.return_value = [
            Mock(TraceId="t1", event_ts=datetime(2024, 1, 5, 10, 0), StoreID=1, Total=10.0),
        ]

        mock_session.execute.side_effect = [count_result, data_result]

        df = await db_reader.read_fact_table(
            mock_session,
            Receipt,
            end_date=end_date
        )

        assert len(df) == 1

    @pytest.mark.asyncio
    async def test_read_fact_table_with_date_range(self, mock_session):
        """Should filter by both start and end dates."""
        from retail_datagen.db.models.facts import TruckMove

        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 31)

        count_result = Mock()
        count_result.scalar_one.return_value = 10
        data_result = Mock()
        data_result.scalars.return_value.all.return_value = []

        mock_session.execute.side_effect = [count_result, data_result]

        df = await db_reader.read_fact_table(
            mock_session,
            TruckMove,
            start_date=start_date,
            end_date=end_date
        )

        # Verify both count and data queries were executed
        assert mock_session.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_read_fact_table_invalid_date_range(self, mock_session):
        """Should raise ValueError if start_date > end_date."""
        from retail_datagen.db.models.facts import Receipt

        start_date = date(2024, 2, 1)
        end_date = date(2024, 1, 1)

        with pytest.raises(ValueError, match="Invalid date range"):
            await db_reader.read_fact_table(
                mock_session,
                Receipt,
                start_date=start_date,
                end_date=end_date
            )

    @pytest.mark.asyncio
    async def test_read_fact_table_empty_result(self, mock_session):
        """Should return empty DataFrame when no rows match."""
        from retail_datagen.db.models.facts import Receipt

        count_result = Mock()
        count_result.scalar_one.return_value = 0
        mock_session.execute.return_value = count_result

        df = await db_reader.read_fact_table(mock_session, Receipt)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    @pytest.mark.asyncio
    async def test_read_fact_table_invalid_table(self, mock_session):
        """Should raise ValueError for non-fact table."""
        from retail_datagen.db.models.master import Store

        with pytest.raises(ValueError, match="Invalid fact table"):
            await db_reader.read_fact_table(mock_session, Store)

    @pytest.mark.asyncio
    async def test_read_fact_table_chunking(self, mock_session):
        """Should read large result set in chunks."""
        from retail_datagen.db.models.facts import Receipt

        chunk_size = 2
        total_rows = 5

        # Mock count
        count_result = Mock()
        count_result.scalar_one.return_value = total_rows

        # Mock chunked results
        chunk1 = [
            Mock(TraceId="t1", event_ts=datetime(2024, 1, 1), StoreID=1, Total=10.0),
            Mock(TraceId="t2", event_ts=datetime(2024, 1, 2), StoreID=2, Total=20.0),
        ]
        chunk2 = [
            Mock(TraceId="t3", event_ts=datetime(2024, 1, 3), StoreID=3, Total=30.0),
            Mock(TraceId="t4", event_ts=datetime(2024, 1, 4), StoreID=4, Total=40.0),
        ]
        chunk3 = [
            Mock(TraceId="t5", event_ts=datetime(2024, 1, 5), StoreID=5, Total=50.0),
        ]

        result1 = Mock()
        result1.scalars.return_value.all.return_value = chunk1
        result2 = Mock()
        result2.scalars.return_value.all.return_value = chunk2
        result3 = Mock()
        result3.scalars.return_value.all.return_value = chunk3

        mock_session.execute.side_effect = [count_result, result1, result2, result3]

        df = await db_reader.read_fact_table(
            mock_session,
            Receipt,
            chunk_size=chunk_size
        )

        # Should combine all chunks
        assert len(df) == 5
        assert df["TraceId"].tolist() == ["t1", "t2", "t3", "t4", "t5"]


class TestReadAllFactTables:
    """Test read_all_fact_tables function."""

    @pytest.mark.asyncio
    async def test_read_all_fact_tables_success(self):
        """Should read all 9 fact tables."""
        mock_session = AsyncMock()

        with patch('retail_datagen.services.db_reader.read_fact_table') as mock_read:
            mock_read.return_value = pd.DataFrame({
                "TraceId": ["t1", "t2"],
                "event_ts": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            })

            result = await db_reader.read_all_fact_tables(mock_session)

        # Should return dict with all 9 fact tables
        assert len(result) == 9
        assert "fact_dc_inventory_txn" in result
        assert "fact_truck_moves" in result
        assert "fact_store_inventory_txn" in result
        assert "fact_receipts" in result
        assert "fact_receipt_lines" in result
        assert "fact_foot_traffic" in result
        assert "fact_ble_pings" in result
        assert "fact_marketing" in result
        assert "fact_online_orders" in result

    @pytest.mark.asyncio
    async def test_read_all_fact_tables_with_date_filters(self):
        """Should pass date filters to read_fact_table."""
        mock_session = AsyncMock()
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 31)

        with patch('retail_datagen.services.db_reader.read_fact_table') as mock_read:
            mock_read.return_value = pd.DataFrame()

            await db_reader.read_all_fact_tables(
                mock_session,
                start_date=start_date,
                end_date=end_date
            )

            # Verify date filters were passed to all calls
            for call in mock_read.call_args_list:
                assert call[1].get('start_date') == start_date
                assert call[1].get('end_date') == end_date

    @pytest.mark.asyncio
    async def test_read_all_fact_tables_partial_failure(self):
        """Should continue reading other tables if one fails."""
        mock_session = AsyncMock()

        def mock_read_side_effect(
            session,
            table_model,
            start_date=None,
            end_date=None,
            chunk_size=DEFAULT_CHUNK_SIZE
        ):
            table_name = table_model.__tablename__
            if table_name == "receipts":
                raise Exception("Read failed")
            return pd.DataFrame({"TraceId": ["t1"], "event_ts": [datetime.now()]})

        with patch('retail_datagen.services.db_reader.read_fact_table') as mock_read:
            mock_read.side_effect = mock_read_side_effect

            result = await db_reader.read_all_fact_tables(mock_session)

        # Should have all 9 tables, but failed one is empty
        assert len(result) == 9
        assert len(result["fact_receipts"]) == 0  # Failed table
        assert len(result["fact_truck_moves"]) == 1  # Successful table


class TestGetFactTableDateRange:
    """Test get_fact_table_date_range function."""

    @pytest.mark.asyncio
    async def test_get_date_range_with_data(self):
        """Should return min and max timestamps."""
        mock_session = AsyncMock()

        min_ts = datetime(2024, 1, 1, 0, 0, 0)
        max_ts = datetime(2024, 1, 31, 23, 59, 59)

        mock_result = Mock()
        mock_result.one.return_value = (min_ts, max_ts)
        mock_session.execute.return_value = mock_result

        from retail_datagen.db.models.facts import Receipt

        result = await db_reader.get_fact_table_date_range(mock_session, Receipt)

        assert result == (min_ts, max_ts)

    @pytest.mark.asyncio
    async def test_get_date_range_empty_table(self):
        """Should return (None, None) for empty table."""
        mock_session = AsyncMock()

        mock_result = Mock()
        mock_result.one.return_value = (None, None)
        mock_session.execute.return_value = mock_result

        from retail_datagen.db.models.facts import TruckMove

        result = await db_reader.get_fact_table_date_range(mock_session, TruckMove)

        assert result == (None, None)

    @pytest.mark.asyncio
    async def test_get_date_range_invalid_table(self):
        """Should raise ValueError for non-fact table."""
        mock_session = AsyncMock()

        from retail_datagen.db.models.master import Store

        with pytest.raises(ValueError, match="Invalid fact table"):
            await db_reader.get_fact_table_date_range(mock_session, Store)


class TestGetAllFactTableDateRanges:
    """Test get_all_fact_table_date_ranges function."""

    @pytest.mark.asyncio
    async def test_get_all_date_ranges_success(self):
        """Should return date ranges for all 9 fact tables."""
        mock_session = AsyncMock()

        min_ts = datetime(2024, 1, 1)
        max_ts = datetime(2024, 1, 31)

        with patch('retail_datagen.services.db_reader.get_fact_table_date_range') as mock_get:
            mock_get.return_value = (min_ts, max_ts)

            result = await db_reader.get_all_fact_table_date_ranges(mock_session)

        # Should return dict with all 9 fact tables
        assert len(result) == 9
        for table_name in FACT_TABLES.keys():
            assert table_name in result
            assert result[table_name] == (min_ts, max_ts)

    @pytest.mark.asyncio
    async def test_get_all_date_ranges_partial_failure(self):
        """Should continue getting ranges if one fails."""
        mock_session = AsyncMock()

        def mock_get_side_effect(session, table_model):
            table_name = table_model.__tablename__
            if table_name == "receipts":
                raise Exception("Query failed")
            return (datetime(2024, 1, 1), datetime(2024, 1, 31))

        with patch('retail_datagen.services.db_reader.get_fact_table_date_range') as mock_get:
            mock_get.side_effect = mock_get_side_effect

            result = await db_reader.get_all_fact_table_date_ranges(mock_session)

        # Failed table should have (None, None)
        assert result["fact_receipts"] == (None, None)

        # Other tables should have valid ranges
        assert result["fact_truck_moves"] == (datetime(2024, 1, 1), datetime(2024, 1, 31))


class TestTableMappings:
    """Test table mapping constants."""

    def test_master_tables_count(self):
        """Should have exactly 6 master tables."""
        assert len(MASTER_TABLES) == 6

    def test_fact_tables_count(self):
        """Should have exactly 9 fact tables."""
        assert len(FACT_TABLES) == 9

    def test_master_table_names(self):
        """Should include all expected master table names."""
        expected = {
            "dim_geographies",
            "dim_stores",
            "dim_distribution_centers",
            "dim_trucks",
            "dim_customers",
            "dim_products",
        }
        assert set(MASTER_TABLES.keys()) == expected

    def test_fact_table_names(self):
        """Should include all expected fact table names."""
        expected = {
            "fact_dc_inventory_txn",
            "fact_truck_moves",
            "fact_store_inventory_txn",
            "fact_receipts",
            "fact_receipt_lines",
            "fact_foot_traffic",
            "fact_ble_pings",
            "fact_marketing",
            "fact_online_orders",
        }
        assert set(FACT_TABLES.keys()) == expected

    def test_default_chunk_size(self):
        """Should have reasonable default chunk size."""
        assert DEFAULT_CHUNK_SIZE == 10000
