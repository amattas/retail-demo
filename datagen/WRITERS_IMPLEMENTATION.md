# Format Writer Services Implementation

## Overview

This document describes the format writer services implemented for the data export functionality. These services provide a unified interface for writing pandas DataFrames to different file formats (CSV, Parquet) with support for date-partitioned outputs.

## Architecture

### File Structure

```
src/retail_datagen/services/writers/
├── __init__.py              # Module exports
├── base_writer.py          # Abstract base class
├── csv_writer.py           # CSV format implementation
└── parquet_writer.py       # Parquet format implementation
```

### Design Pattern

The implementation follows the **Strategy Pattern** with an abstract base class (`BaseWriter`) defining the interface that concrete implementations (`CSVWriter`, `ParquetWriter`) must follow.

## Components

### 1. BaseWriter (Abstract Base Class)

**File:** `src/retail_datagen/services/writers/base_writer.py`

**Purpose:** Defines the interface for all format writers.

**Methods:**

- `write(df, output_path, **kwargs)`: Write DataFrame to a single file
- `write_partitioned(df, output_dir, partition_col, table_name, **kwargs)`: Write DataFrame partitioned by column value

**Key Features:**

- Consistent API across all format writers
- Partition format: `<output_dir>/<partition_col>=<value>/<table_name>_<value>.<ext>`
- Comprehensive error handling (ValueError, IOError)

### 2. CSVWriter

**File:** `src/retail_datagen/services/writers/csv_writer.py`

**Purpose:** Write DataFrames to CSV format matching existing generator conventions.

**Constructor Parameters:**

- `index` (bool): Whether to write row indices (default: False)
- `**default_kwargs`: Default arguments passed to `pandas.to_csv()`

**Key Features:**

- Automatic directory creation
- Configurable CSV format options
- Partition support with format: `<table>_<value>.csv`
- Detailed logging with record counts
- Thread-safe (uses pandas underlying thread-safety)

**Example Usage:**

```python
from retail_datagen.services.writers import CSVWriter

writer = CSVWriter(index=False)

# Simple write
writer.write(df, Path("output/data.csv"))

# Partitioned write by date
files = writer.write_partitioned(
    df,
    Path("output/facts"),
    partition_col="TransactionDate",
    table_name="receipts"
)
# Creates: output/facts/TransactionDate=2024-01-01/receipts_2024-01-01.csv
```

### 3. ParquetWriter

**File:** `src/retail_datagen/services/writers/parquet_writer.py`

**Purpose:** Write DataFrames to Parquet format with optimized compression.

**Constructor Parameters:**

- `engine` (str): Parquet engine (default: "pyarrow")
- `compression` (str): Compression algorithm (default: "snappy")
- `**default_kwargs`: Default arguments passed to `pandas.to_parquet()`

**Key Features:**

- Pyarrow engine with Snappy compression (optimized for analytical workloads)
- Automatic directory creation
- Graceful handling of missing pyarrow dependency
- Partition support with format: `<table>_<value>.parquet`
- Detailed logging with record counts

**Example Usage:**

```python
from retail_datagen.services.writers import ParquetWriter

writer = ParquetWriter(compression="snappy")

# Simple write
writer.write(df, Path("output/data.parquet"))

# Partitioned write by date
files = writer.write_partitioned(
    df,
    Path("output/facts"),
    partition_col="TransactionDate",
    table_name="receipts"
)
# Creates: output/facts/TransactionDate=2024-01-01/receipts_2024-01-01.parquet
```

## Dependencies

### Added Dependency

**pyarrow>=14.0.0** was added to `requirements.txt` for Parquet support.

**Why pyarrow?**

- Industry-standard columnar format for analytics
- Efficient compression (Snappy default)
- Fast read/write performance
- Native integration with pandas
- Compatible with Spark, Presto, and other analytics engines

## Error Handling

All writers implement consistent error handling:

1. **ValueError**:
   - Empty DataFrame
   - Missing partition column

2. **IOError**:
   - File write failures
   - Permission issues
   - Disk space issues

3. **ImportError** (Parquet only):
   - Missing pyarrow dependency
   - Clear installation instructions in error message

## Logging

All writers use Python's standard logging module:

- **INFO**: Record counts, partition counts, file paths
- **ERROR**: Write failures with detailed error messages

**Example Log Output:**

```
INFO: Writing 10,000 records to 3 partitions by 'TransactionDate'
INFO: Wrote 3,245 records to output/facts/TransactionDate=2024-01-01/receipts_2024-01-01.csv
INFO: Wrote 3,612 records to output/facts/TransactionDate=2024-01-02/receipts_2024-01-02.csv
INFO: Wrote 3,143 records to output/facts/TransactionDate=2024-01-03/receipts_2024-01-03.csv
INFO: Created 3 partitioned CSV files in output/facts
```

## Partitioning Format

Both writers follow the same partitioning convention, compatible with Hive-style partitioning:

```
<output_dir>/
  <partition_col>=<value1>/
    <table_name>_<value1>.<ext>
  <partition_col>=<value2>/
    <table_name>_<value2>.<ext>
```

**Example:**

```
data/export/
  TransactionDate=2024-01-01/
    receipts_2024-01-01.csv
  TransactionDate=2024-01-02/
    receipts_2024-01-02.csv
  TransactionDate=2024-01-03/
    receipts_2024-01-03.csv
```

This format is compatible with:

- Microsoft Fabric Lakehouse
- Azure Data Lake
- Apache Spark
- Apache Hive
- AWS Athena
- Google BigQuery (external tables)

## Testing

A validation script (`test_writers.py`) has been created to test both writers:

**Run tests:**

```bash
# Install pyarrow first
pip install pyarrow>=14.0.0

# Run validation
python test_writers.py
```

**Test Coverage:**

- Simple write functionality
- Partitioned write functionality
- File creation verification
- Content verification (row counts)
- Error handling (empty DataFrames, missing columns)

## Integration Points

These writers are designed to integrate with:

1. **ExportFileManager**: High-level export orchestration
2. **Database readers** (`db_reader.py`): Read from SQLite, write to files
3. **API endpoints**: Export endpoints for master/fact tables
4. **Streaming pipeline**: Archive events to partitioned files

## Performance Considerations

### CSV Writer

- **Pros**: Human-readable, widely compatible, simple
- **Cons**: Larger file sizes, slower reads for analytics
- **Best for**: Small datasets, debugging, manual inspection

### Parquet Writer

- **Pros**: Columnar format, efficient compression, fast analytics
- **Cons**: Not human-readable, requires specialized tools
- **Best for**: Large datasets, analytics workloads, data warehousing

**Benchmark (approximate):**

| Format  | 1M Rows | Compression | Read Speed |
|---------|---------|-------------|------------|
| CSV     | 150 MB  | None        | Slower     |
| Parquet | 25 MB   | Snappy      | 5-10x faster |

## Future Enhancements

Potential improvements:

1. **Additional formats**: JSON, Avro, ORC
2. **Async writes**: Support for async I/O operations
3. **Schema evolution**: Handle schema changes in partitioned writes
4. **Custom partitioning**: Multi-column partitioning support
5. **Compression options**: Configurable compression levels
6. **Validation**: Built-in data quality checks before writing

## Related Files

- `/Users/amattas/GitHub/retail-demo/datagen/src/retail_datagen/services/file_manager.py` - Export file management
- `/Users/amattas/GitHub/retail-demo/datagen/src/retail_datagen/services/db_reader.py` - Database reading utilities
- `/Users/amattas/GitHub/retail-demo/datagen/requirements.txt` - Python dependencies
- `/Users/amattas/GitHub/retail-demo/datagen/test_writers.py` - Validation script

## References

- [pandas.DataFrame.to_csv](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html)
- [pandas.DataFrame.to_parquet](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_parquet.html)
- [Apache Parquet](https://parquet.apache.org/)
- [PyArrow](https://arrow.apache.org/docs/python/)
