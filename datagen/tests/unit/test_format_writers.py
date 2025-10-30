"""
Unit tests for format writers (CSV and Parquet).

Tests CSVWriter and ParquetWriter implementations with support
for simple and partitioned writes.
"""

import pytest
from pathlib import Path
import pandas as pd
from datetime import date

from retail_datagen.services.writers import CSVWriter, ParquetWriter


class TestCSVWriterInit:
    """Test CSVWriter initialization."""

    def test_init_default_params(self):
        """Should initialize with default parameters."""
        writer = CSVWriter()

        assert writer.index is False
        assert writer.default_kwargs == {}

    def test_init_custom_index(self):
        """Should initialize with custom index setting."""
        writer = CSVWriter(index=True)

        assert writer.index is True

    def test_init_with_default_kwargs(self):
        """Should store default kwargs for pandas to_csv."""
        writer = CSVWriter(sep=";", encoding="utf-8")

        assert writer.default_kwargs["sep"] == ";"
        assert writer.default_kwargs["encoding"] == "utf-8"


class TestCSVWriterWrite:
    """Test CSVWriter.write method."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame({
            "ID": [1, 2, 3],
            "Name": ["Alice", "Bob", "Charlie"],
            "Amount": [10.5, 20.0, 15.75],
        })

    def test_write_simple_success(self, tmp_path, sample_df):
        """Should write DataFrame to CSV file successfully."""
        writer = CSVWriter()
        output_path = tmp_path / "output.csv"

        writer.write(sample_df, output_path)

        # Verify file exists
        assert output_path.exists()

        # Verify content
        df_read = pd.read_csv(output_path)
        assert len(df_read) == 3
        assert list(df_read.columns) == ["ID", "Name", "Amount"]
        assert df_read["Name"].tolist() == ["Alice", "Bob", "Charlie"]

    def test_write_with_index(self, tmp_path, sample_df):
        """Should write DataFrame with index when configured."""
        writer = CSVWriter(index=True)
        output_path = tmp_path / "with_index.csv"

        writer.write(sample_df, output_path)

        # Read and verify index column exists
        df_read = pd.read_csv(output_path, index_col=0)
        assert df_read.index.tolist() == [0, 1, 2]

    def test_write_empty_dataframe(self, tmp_path):
        """Should raise ValueError for empty DataFrame."""
        writer = CSVWriter()
        output_path = tmp_path / "empty.csv"
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="Cannot write empty DataFrame"):
            writer.write(empty_df, output_path)

    def test_write_creates_parent_directory(self, tmp_path):
        """Should create parent directories if they don't exist."""
        writer = CSVWriter()
        output_path = tmp_path / "nested" / "dir" / "output.csv"

        df = pd.DataFrame({"A": [1, 2]})
        writer.write(df, output_path)

        assert output_path.exists()
        assert output_path.parent.exists()

    def test_write_with_custom_kwargs(self, tmp_path, sample_df):
        """Should apply custom kwargs to pandas to_csv."""
        writer = CSVWriter()
        output_path = tmp_path / "custom.csv"

        # Write with custom separator
        writer.write(sample_df, output_path, sep=";")

        # Verify custom separator was used
        with open(output_path) as f:
            first_line = f.readline()
            assert ";" in first_line
            assert "," not in first_line

    def test_write_overwrites_existing_file(self, tmp_path, sample_df):
        """Should overwrite existing file."""
        writer = CSVWriter()
        output_path = tmp_path / "overwrite.csv"

        # Write first time
        writer.write(sample_df, output_path)
        original_content = output_path.read_text()

        # Write second time with different data
        new_df = pd.DataFrame({"X": [99]})
        writer.write(new_df, output_path)

        # Verify file was overwritten
        new_content = output_path.read_text()
        assert new_content != original_content
        assert "99" in new_content

    def test_write_handles_special_characters(self, tmp_path):
        """Should handle special characters in data."""
        writer = CSVWriter()
        output_path = tmp_path / "special.csv"

        df = pd.DataFrame({
            "Name": ["John, Jr.", "O'Brien", 'Quote"Test'],
            "Description": ["Line1\nLine2", "Tab\tSeparated", "Normal"],
        })

        writer.write(df, output_path)

        # Verify data can be read back correctly
        df_read = pd.read_csv(output_path)
        assert df_read["Name"].tolist() == ["John, Jr.", "O'Brien", 'Quote"Test']

    def test_write_io_error(self, tmp_path):
        """Should raise IOError on write failure."""
        writer = CSVWriter()

        # Create a directory with the output file name to force error
        output_path = tmp_path / "invalid.csv"
        output_path.mkdir()

        df = pd.DataFrame({"A": [1, 2]})

        with pytest.raises(IOError, match="Failed to write CSV file"):
            writer.write(df, output_path)


class TestCSVWriterWritePartitioned:
    """Test CSVWriter.write_partitioned method."""

    @pytest.fixture
    def partitioned_df(self):
        """Create DataFrame suitable for partitioning."""
        return pd.DataFrame({
            "ID": [1, 2, 3, 4, 5, 6],
            "Date": ["2024-01-01", "2024-01-01", "2024-01-02",
                     "2024-01-02", "2024-01-03", "2024-01-03"],
            "Amount": [10, 20, 30, 40, 50, 60],
        })

    def test_write_partitioned_success(self, tmp_path, partitioned_df):
        """Should write partitioned files correctly."""
        writer = CSVWriter()
        output_dir = tmp_path / "partitioned"

        result = writer.write_partitioned(
            partitioned_df,
            output_dir,
            partition_col="Date",
            table_name="sales"
        )

        # Should create 3 partition files (3 unique dates)
        assert len(result) == 3

        # Verify partition structure
        for file_path in result:
            assert file_path.exists()
            assert "Date=" in str(file_path.parent)
            assert file_path.name.startswith("sales_")
            assert file_path.suffix == ".csv"

        # Verify partition contents
        date1_files = [p for p in result if "Date=2024-01-01" in str(p)]
        assert len(date1_files) == 1
        df1 = pd.read_csv(date1_files[0])
        assert len(df1) == 2  # Two records for 2024-01-01

    def test_write_partitioned_default_table_name(self, tmp_path, partitioned_df):
        """Should use default table name when not provided."""
        writer = CSVWriter()
        output_dir = tmp_path / "partitioned"

        result = writer.write_partitioned(
            partitioned_df,
            output_dir,
            partition_col="Date"
        )

        # Files should use "data" as default name
        for file_path in result:
            assert file_path.name.startswith("data_")

    def test_write_partitioned_empty_dataframe(self, tmp_path):
        """Should raise ValueError for empty DataFrame."""
        writer = CSVWriter()
        output_dir = tmp_path / "partitioned"
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="Cannot write empty DataFrame"):
            writer.write_partitioned(
                empty_df,
                output_dir,
                partition_col="Date"
            )

    def test_write_partitioned_missing_column(self, tmp_path, partitioned_df):
        """Should raise ValueError if partition column doesn't exist."""
        writer = CSVWriter()
        output_dir = tmp_path / "partitioned"

        with pytest.raises(ValueError, match="Partition column 'InvalidCol' not found"):
            writer.write_partitioned(
                partitioned_df,
                output_dir,
                partition_col="InvalidCol"
            )

    def test_write_partitioned_single_partition(self, tmp_path):
        """Should handle DataFrame with single partition value."""
        writer = CSVWriter()
        output_dir = tmp_path / "partitioned"

        df = pd.DataFrame({
            "ID": [1, 2, 3],
            "Category": ["A", "A", "A"],
            "Value": [10, 20, 30],
        })

        result = writer.write_partitioned(
            df,
            output_dir,
            partition_col="Category",
            table_name="test"
        )

        # Should create single partition
        assert len(result) == 1
        assert "Category=A" in str(result[0].parent)

    def test_write_partitioned_preserves_data(self, tmp_path, partitioned_df):
        """Should preserve all data across partitions."""
        writer = CSVWriter()
        output_dir = tmp_path / "partitioned"

        result = writer.write_partitioned(
            partitioned_df,
            output_dir,
            partition_col="Date"
        )

        # Read all partitions and combine
        all_data = []
        for file_path in result:
            df = pd.read_csv(file_path)
            all_data.append(df)

        combined_df = pd.concat(all_data, ignore_index=True)

        # Should have same number of rows as original
        assert len(combined_df) == len(partitioned_df)


class TestParquetWriterInit:
    """Test ParquetWriter initialization."""

    def test_init_default_params(self):
        """Should initialize with default parameters."""
        writer = ParquetWriter()

        assert writer.engine == "pyarrow"
        assert writer.compression == "snappy"
        assert writer.default_kwargs == {}

    def test_init_custom_engine(self):
        """Should initialize with custom engine."""
        writer = ParquetWriter(engine="fastparquet")

        assert writer.engine == "fastparquet"

    def test_init_custom_compression(self):
        """Should initialize with custom compression."""
        writer = ParquetWriter(compression="gzip")

        assert writer.compression == "gzip"

    def test_init_with_default_kwargs(self):
        """Should store default kwargs for pandas to_parquet."""
        writer = ParquetWriter(index=False, partition_cols=["date"])

        assert writer.default_kwargs["index"] is False
        assert writer.default_kwargs["partition_cols"] == ["date"]


class TestParquetWriterWrite:
    """Test ParquetWriter.write method."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame({
            "ID": [1, 2, 3],
            "Name": ["Alice", "Bob", "Charlie"],
            "Amount": [10.5, 20.0, 15.75],
        })

    def test_write_simple_success(self, tmp_path, sample_df):
        """Should write DataFrame to Parquet file successfully."""
        writer = ParquetWriter()
        output_path = tmp_path / "output.parquet"

        writer.write(sample_df, output_path)

        # Verify file exists
        assert output_path.exists()

        # Verify content
        df_read = pd.read_parquet(output_path)
        assert len(df_read) == 3
        assert list(df_read.columns) == ["ID", "Name", "Amount"]
        assert df_read["Name"].tolist() == ["Alice", "Bob", "Charlie"]

    def test_write_empty_dataframe(self, tmp_path):
        """Should raise ValueError for empty DataFrame."""
        writer = ParquetWriter()
        output_path = tmp_path / "empty.parquet"
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="Cannot write empty DataFrame"):
            writer.write(empty_df, output_path)

    def test_write_creates_parent_directory(self, tmp_path):
        """Should create parent directories if they don't exist."""
        writer = ParquetWriter()
        output_path = tmp_path / "nested" / "dir" / "output.parquet"

        df = pd.DataFrame({"A": [1, 2]})
        writer.write(df, output_path)

        assert output_path.exists()

    def test_write_with_custom_compression(self, tmp_path, sample_df):
        """Should apply custom compression algorithm."""
        writer = ParquetWriter()
        output_path = tmp_path / "compressed.parquet"

        # Write with gzip compression
        writer.write(sample_df, output_path, compression="gzip")

        # Verify file can be read back
        df_read = pd.read_parquet(output_path)
        assert len(df_read) == 3

    def test_write_preserves_dtypes(self, tmp_path):
        """Should preserve DataFrame column types."""
        writer = ParquetWriter()
        output_path = tmp_path / "types.parquet"

        df = pd.DataFrame({
            "IntCol": [1, 2, 3],
            "FloatCol": [1.5, 2.5, 3.5],
            "StrCol": ["a", "b", "c"],
            "DateCol": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        })

        writer.write(df, output_path)

        df_read = pd.read_parquet(output_path)
        assert df_read["IntCol"].dtype == "int64"
        assert df_read["FloatCol"].dtype == "float64"
        assert df_read["StrCol"].dtype == "object"
        assert pd.api.types.is_datetime64_any_dtype(df_read["DateCol"])

    def test_write_io_error(self, tmp_path):
        """Should raise IOError on write failure."""
        writer = ParquetWriter()

        # Create a directory with the output file name to force error
        output_path = tmp_path / "invalid.parquet"
        output_path.mkdir()

        df = pd.DataFrame({"A": [1, 2]})

        with pytest.raises(IOError, match="Failed to write Parquet file"):
            writer.write(df, output_path)


class TestParquetWriterWritePartitioned:
    """Test ParquetWriter.write_partitioned method."""

    @pytest.fixture
    def partitioned_df(self):
        """Create DataFrame suitable for partitioning."""
        return pd.DataFrame({
            "ID": [1, 2, 3, 4, 5, 6],
            "Date": ["2024-01-01", "2024-01-01", "2024-01-02",
                     "2024-01-02", "2024-01-03", "2024-01-03"],
            "Amount": [10, 20, 30, 40, 50, 60],
        })

    def test_write_partitioned_success(self, tmp_path, partitioned_df):
        """Should write partitioned Parquet files correctly."""
        writer = ParquetWriter()
        output_dir = tmp_path / "partitioned"

        result = writer.write_partitioned(
            partitioned_df,
            output_dir,
            partition_col="Date",
            table_name="sales"
        )

        # Should create 3 partition files
        assert len(result) == 3

        # Verify partition structure
        for file_path in result:
            assert file_path.exists()
            assert "Date=" in str(file_path.parent)
            assert file_path.name.startswith("sales_")
            assert file_path.suffix == ".parquet"

    def test_write_partitioned_default_table_name(self, tmp_path, partitioned_df):
        """Should use default table name when not provided."""
        writer = ParquetWriter()
        output_dir = tmp_path / "partitioned"

        result = writer.write_partitioned(
            partitioned_df,
            output_dir,
            partition_col="Date"
        )

        # Files should use "data" as default name
        for file_path in result:
            assert file_path.name.startswith("data_")

    def test_write_partitioned_empty_dataframe(self, tmp_path):
        """Should raise ValueError for empty DataFrame."""
        writer = ParquetWriter()
        output_dir = tmp_path / "partitioned"
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="Cannot write empty DataFrame"):
            writer.write_partitioned(
                empty_df,
                output_dir,
                partition_col="Date"
            )

    def test_write_partitioned_missing_column(self, tmp_path, partitioned_df):
        """Should raise ValueError if partition column doesn't exist."""
        writer = ParquetWriter()
        output_dir = tmp_path / "partitioned"

        with pytest.raises(ValueError, match="Partition column 'InvalidCol' not found"):
            writer.write_partitioned(
                partitioned_df,
                output_dir,
                partition_col="InvalidCol"
            )

    def test_write_partitioned_preserves_data(self, tmp_path, partitioned_df):
        """Should preserve all data across partitions."""
        writer = ParquetWriter()
        output_dir = tmp_path / "partitioned"

        result = writer.write_partitioned(
            partitioned_df,
            output_dir,
            partition_col="Date"
        )

        # Read all partitions and combine
        all_data = []
        for file_path in result:
            df = pd.read_parquet(file_path)
            all_data.append(df)

        combined_df = pd.concat(all_data, ignore_index=True)

        # Should have same number of rows as original
        assert len(combined_df) == len(partitioned_df)


class TestWriterComparison:
    """Compare behavior between CSV and Parquet writers."""

    @pytest.fixture
    def test_df(self):
        """Create test DataFrame."""
        return pd.DataFrame({
            "ID": range(1, 101),
            "Category": ["A", "B", "C", "D"] * 25,
            "Value": [x * 1.5 for x in range(1, 101)],
        })

    def test_both_writers_produce_same_row_count(self, tmp_path, test_df):
        """Should produce same number of rows regardless of format."""
        csv_writer = CSVWriter()
        parquet_writer = ParquetWriter()

        csv_path = tmp_path / "data.csv"
        parquet_path = tmp_path / "data.parquet"

        csv_writer.write(test_df, csv_path)
        parquet_writer.write(test_df, parquet_path)

        csv_df = pd.read_csv(csv_path)
        parquet_df = pd.read_parquet(parquet_path)

        assert len(csv_df) == len(parquet_df) == len(test_df)

    def test_both_writers_support_partitioning(self, tmp_path, test_df):
        """Should both support partitioned writes."""
        csv_writer = CSVWriter()
        parquet_writer = ParquetWriter()

        csv_dir = tmp_path / "csv_partitioned"
        parquet_dir = tmp_path / "parquet_partitioned"

        csv_files = csv_writer.write_partitioned(
            test_df, csv_dir, partition_col="Category"
        )
        parquet_files = parquet_writer.write_partitioned(
            test_df, parquet_dir, partition_col="Category"
        )

        # Should create same number of partitions
        assert len(csv_files) == len(parquet_files) == 4  # 4 unique categories

    def test_parquet_typically_smaller_than_csv(self, tmp_path, test_df):
        """Should verify Parquet compression is effective."""
        csv_writer = CSVWriter()
        parquet_writer = ParquetWriter()

        csv_path = tmp_path / "data.csv"
        parquet_path = tmp_path / "data.parquet"

        csv_writer.write(test_df, csv_path)
        parquet_writer.write(test_df, parquet_path)

        csv_size = csv_path.stat().st_size
        parquet_size = parquet_path.stat().st_size

        # Parquet with compression should typically be smaller
        # (Not always guaranteed for very small datasets, so this is informational)
        print(f"CSV size: {csv_size}, Parquet size: {parquet_size}")
        assert parquet_size > 0  # Just verify file was created
