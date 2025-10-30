"""
Quick validation script for format writers.
"""

import tempfile
from pathlib import Path
import pandas as pd
from datetime import date

from src.retail_datagen.services.writers import CSVWriter, ParquetWriter


def test_csv_writer():
    """Test CSV writer functionality."""
    print("\n=== Testing CSV Writer ===")

    # Create test DataFrame
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'date': ['2024-01-01', '2024-01-01', '2024-01-02', '2024-01-02', '2024-01-03'],
        'value': [100, 200, 150, 300, 250]
    })

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Test simple write
        print("\n1. Testing simple write...")
        writer = CSVWriter()
        output_file = tmpdir / "test.csv"
        writer.write(df, output_file)
        assert output_file.exists(), "CSV file not created"

        # Verify content
        df_read = pd.read_csv(output_file)
        assert len(df_read) == 5, f"Expected 5 rows, got {len(df_read)}"
        print(f"   ✓ Wrote {len(df)} rows to {output_file}")

        # Test partitioned write
        print("\n2. Testing partitioned write...")
        output_dir = tmpdir / "partitioned"
        files = writer.write_partitioned(df, output_dir, partition_col='date', table_name='test')
        assert len(files) == 3, f"Expected 3 partition files, got {len(files)}"
        print(f"   ✓ Created {len(files)} partitions:")
        for f in files:
            df_part = pd.read_csv(f)
            print(f"     - {f.relative_to(tmpdir)}: {len(df_part)} rows")

        print("\n✓ CSV Writer tests passed!")


def test_parquet_writer():
    """Test Parquet writer functionality."""
    print("\n=== Testing Parquet Writer ===")

    # Create test DataFrame
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'date': ['2024-01-01', '2024-01-01', '2024-01-02', '2024-01-02', '2024-01-03'],
        'value': [100, 200, 150, 300, 250]
    })

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Test simple write
        print("\n1. Testing simple write...")
        writer = ParquetWriter()
        output_file = tmpdir / "test.parquet"

        try:
            writer.write(df, output_file)
            assert output_file.exists(), "Parquet file not created"

            # Verify content
            df_read = pd.read_parquet(output_file)
            assert len(df_read) == 5, f"Expected 5 rows, got {len(df_read)}"
            print(f"   ✓ Wrote {len(df)} rows to {output_file}")

            # Test partitioned write
            print("\n2. Testing partitioned write...")
            output_dir = tmpdir / "partitioned"
            files = writer.write_partitioned(df, output_dir, partition_col='date', table_name='test')
            assert len(files) == 3, f"Expected 3 partition files, got {len(files)}"
            print(f"   ✓ Created {len(files)} partitions:")
            for f in files:
                df_part = pd.read_parquet(f)
                print(f"     - {f.relative_to(tmpdir)}: {len(df_part)} rows")

            print("\n✓ Parquet Writer tests passed!")

        except ImportError as e:
            print(f"\n⚠ Parquet tests skipped: pyarrow not installed")
            print(f"  Install with: pip install pyarrow>=14.0.0")


if __name__ == "__main__":
    test_csv_writer()
    test_parquet_writer()
    print("\n" + "="*50)
    print("All writer tests completed!")
    print("="*50)
