"""
Unit tests for Parquet format writer.
"""

import pandas as pd
import pytest

from retail_datagen.services.writers import ParquetWriter


class TestParquetWriter:
    def test_init_defaults(self):
        w = ParquetWriter()
        assert w.engine == "pyarrow"
        assert w.compression == "snappy"

    def test_write_simple_success(self, tmp_path):
        w = ParquetWriter()
        df = pd.DataFrame({"ID": [1, 2, 3], "Name": ["A", "B", "C"]})
        out = tmp_path / "out.parquet"
        w.write(df, out)
        assert out.exists()
        df2 = pd.read_parquet(out)
        assert len(df2) == 3
        assert df2["Name"].tolist() == ["A", "B", "C"]

    def test_write_empty_dataframe_raises(self, tmp_path):
        w = ParquetWriter()
        out = tmp_path / "empty.parquet"
        with pytest.raises(ValueError):
            w.write(pd.DataFrame(), out)

    def test_write_creates_parent(self, tmp_path):
        w = ParquetWriter()
        out = tmp_path / "nested" / "dir" / "file.parquet"
        w.write(pd.DataFrame({"A": [1]}), out)
        assert out.exists()
        assert out.parent.exists()

    def test_write_partitioned(self, tmp_path):
        w = ParquetWriter()
        df = pd.DataFrame({
            "Month": ["2024-01", "2024-01", "2024-02"],
            "Amount": [10, 20, 30],
        })
        outdir = tmp_path / "parts"
        files = w.write_partitioned(df, outdir, partition_col="Month", table_name="sales")
        # Two months -> two files
        assert len(files) == 2
        for p in files:
            assert p.exists()
            assert p.suffix == ".parquet"


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


class TestParquetCompression:
    """Basic check that Parquet files are produced and non-empty."""

    @pytest.fixture
    def test_df(self):
        return pd.DataFrame({
            "ID": range(1, 101),
            "Category": ["A", "B", "C", "D"] * 25,
            "Value": [x * 1.5 for x in range(1, 101)],
        })

    def test_parquet_non_empty(self, tmp_path, test_df):
        writer = ParquetWriter()
        parquet_path = tmp_path / "data.parquet"
        writer.write(test_df, parquet_path)
        assert parquet_path.exists()
        parquet_size = parquet_path.stat().st_size
        assert parquet_size > 0
