# CSV to SQLite Migration Utilities

## Overview

The migration utilities provide batch processing tools to convert existing CSV data (master dimensions and fact tables) into SQLite databases with progress reporting, error handling, and referential integrity validation.

## Features

- **Batch Processing**: Efficient chunked CSV reading with configurable batch sizes (default: 10,000 rows)
- **Progress Reporting**: Optional callbacks for UI/CLI progress updates
- **Error Handling**: Graceful handling of bad rows with detailed error logging
- **Type Conversion**: Automatic parsing of dates, booleans, numbers, and strings
- **Column Mapping**: Automatic mapping from CSV column names to SQLAlchemy model attributes
- **Referential Integrity**: Validation of foreign key relationships (post-migration)
- **Logging**: Detailed migration logs saved to `logs/migration.log`

## Files Created

### Core Migration Module
- **`src/retail_datagen/db/migration.py`**: Main migration utilities
  - `migrate_master_data_from_csv()`: Migrate all master dimension tables
  - `migrate_fact_data_from_csv()`: Migrate all fact tables from partitioned CSVs
  - `migrate_table_from_csv()`: Migrate a single CSV file to a table
  - `validate_foreign_keys()`: Post-migration FK validation

### CLI Interface
- **`src/retail_datagen/db/__main__.py`**: Command-line interface for running migrations
  - Supports `--master`, `--facts`, `--all`, `--validate`, `--init` flags
  - Configurable batch size and directory paths
  - Progress reporting to console

### Example Script
- **`examples/migrate_csv_to_sqlite.py`**: Complete migration workflow example

### Updated Exports
- **`src/retail_datagen/db/__init__.py`**: Added migration function exports

## Usage

### CLI Usage

```bash
# Migrate master data only
python -m retail_datagen.db.migration --master

# Migrate fact data only
python -m retail_datagen.db.migration --facts

# Migrate everything (master + facts)
python -m retail_datagen.db.migration --all

# Initialize databases first, then migrate
python -m retail_datagen.db.migration --all --init

# Validate foreign keys after migration
python -m retail_datagen.db.migration --validate

# Custom batch size for large datasets
python -m retail_datagen.db.migration --all --batch-size=50000

# Custom CSV directories
python -m retail_datagen.db.migration --master --master-dir=./custom_data/master
```

### Python API Usage

```python
import asyncio
from pathlib import Path
from retail_datagen.db import (
    init_databases,
    migrate_master_data_from_csv,
    migrate_fact_data_from_csv,
    get_master_session,
    get_facts_session,
)

async def migrate_data():
    # Initialize databases (create tables)
    await init_databases()

    # Migrate master data
    async with get_master_session() as session:
        def progress(table, loaded, total):
            print(f"{table}: {loaded}/{total}")

        results = await migrate_master_data_from_csv(
            master_csv_dir=Path("data/master"),
            session=session,
            batch_size=10000,
            progress_callback=progress
        )

    # Migrate fact data
    async with get_facts_session() as session:
        results = await migrate_fact_data_from_csv(
            facts_csv_dir=Path("data/facts"),
            session=session,
            batch_size=10000,
            progress_callback=progress
        )

asyncio.run(migrate_data())
```

## Column Mappings

The migration automatically maps CSV column names to SQLAlchemy model attributes:

### Master Tables

| CSV File | CSV Column | Model Attribute | Type |
|----------|-----------|----------------|------|
| `geographies_master.csv` | `ID` | `geography_id` | int |
| `geographies_master.csv` | `City` | `city` | str |
| `geographies_master.csv` | `State` | `state` | str |
| `geographies_master.csv` | `ZipCode` | `postal_code` | str |
| `stores.csv` | `ID` | `store_id` | int |
| `stores.csv` | `StoreNumber` | `store_number` | str |
| `stores.csv` | `GeographyID` | `geography_id` | int |
| `products_master.csv` | `RequiresRefrigeration` | `requires_refrigeration` | bool |
| `products_master.csv` | `LaunchDate` | `launch_date` | date |

*(See `MASTER_COLUMN_MAPPINGS` in migration.py for complete list)*

### Fact Tables

| CSV File | CSV Column | Model Attribute | Type |
|----------|-----------|----------------|------|
| `receipts/*.csv` | `ReceiptID` | `receipt_id` | int |
| `receipts/*.csv` | `EventTimestamp` | `event_ts` | datetime |
| `receipts/*.csv` | `TotalAmount` | `total_amount` | float |
| `dc_inventory_txn/*.csv` | `TxnID` | `txn_id` | int |
| `dc_inventory_txn/*.csv` | `EventTimestamp` | `event_ts` | datetime |

*(See `FACT_COLUMN_MAPPINGS` in migration.py for complete list)*

## Type Conversion

Automatic type parsing for:

- **datetime**: ISO-8601 timestamps → Python `datetime` objects
- **date**: Date strings → Python `date` objects
- **bool**: `0/1`, `true/false`, `yes/no`, `t/f`, `y/n` → Python `bool`
- **int**: Numeric strings → Python `int`
- **float**: Decimal strings → Python `float`
- **str**: Any value → stripped string

Empty/NULL values are converted to `None`.

## Error Handling

### Row-Level Errors
- Invalid rows are **skipped** (not inserted)
- Errors logged to `logs/migration.log` with row number and reason
- Migration continues processing remaining rows
- Final summary shows success/error counts

### Batch-Level Errors
- If entire batch fails, rolls back that batch
- Logs batch number and error details
- Continues with next batch

### Example Error Log
```
2025-10-27 10:15:23 - retail_datagen.db.migration - WARNING - Skipping row 1543 in products_master.csv: invalid literal for int() with base 10: 'N/A'
2025-10-27 10:15:24 - retail_datagen.db.migration - ERROR - Batch 3 failed for fact_receipts: foreign key constraint
```

## Progress Reporting

### Callback Signature
```python
def progress_callback(table_name: str, rows_loaded: int, total_rows: int) -> None:
    """
    Args:
        table_name: Name of table being migrated
        rows_loaded: Number of rows inserted so far
        total_rows: Total rows to insert (-1 if unknown for partitioned tables)
    """
```

### CLI Progress Output
```
=== Migrating Master Data from data/master ===

  [dim_geographies] 1000 / 1000 rows (100.0%)
  [dim_stores] 214 / 214 rows (100.0%)
  [dim_distribution_centers] 8 / 8 rows (100.0%)
  [dim_customers] 100000 / 100000 rows (100.0%)
  [dim_products] 10000 / 10000 rows (100.0%)
  [dim_trucks] 20 / 20 rows (100.0%)

=== Master Data Migration Summary ===
  dim_geographies: 1,000 rows
  dim_stores: 214 rows
  dim_distribution_centers: 8 rows
  dim_customers: 100,000 rows
  dim_products: 10,000 rows
  dim_trucks: 20 rows

Total rows migrated: 111,242
```

## Migration Order

### Master Data (Dependency Order)
1. **Geography** (no dependencies)
2. **Stores, DCs, Customers** (depend on Geography)
3. **Trucks** (depend on DCs)
4. **Products** (no dependencies)

### Fact Data (Partitioned)
All fact tables are independent and can be migrated in any order. The system processes partitions sequentially:
- `data/facts/<table>/dt=YYYY-MM-DD/*.csv`

## Foreign Key Validation

Post-migration validation checks:
- All geography IDs referenced in master tables exist
- All store IDs referenced in facts exist in master
- All DC IDs referenced in facts exist in master
- All customer IDs referenced in facts exist in master (or are NULL)
- All product IDs referenced in facts exist in master

**Note**: Foreign keys between `master.db` and `facts.db` are **not enforced** at the database level (cross-database references). Validation is performed by the application.

## Performance

### Batch Size Recommendations
- **Small datasets (<100K rows)**: 10,000 rows/batch (default)
- **Medium datasets (100K-1M rows)**: 20,000-50,000 rows/batch
- **Large datasets (>1M rows)**: 50,000-100,000 rows/batch

### Memory Usage
- Chunked reading keeps memory usage low (~batch_size rows in memory)
- Master dimension data loaded into memory for FK validation

### Timing Estimates
- Master data (111K rows): ~5-10 seconds
- Fact data (1M rows): ~30-60 seconds
- Fact data (10M rows): ~5-10 minutes

## Logging

All migration activity is logged to `logs/migration.log`:

```
2025-10-27 10:15:20 - retail_datagen.db.migration - INFO - === Starting Master Data Migration ===
2025-10-27 10:15:20 - retail_datagen.db.migration - INFO - Starting migration: data/master/geographies_master.csv → dim_geographies
2025-10-27 10:15:21 - retail_datagen.db.migration - INFO - Batch 1: Inserted 1000 rows into dim_geographies
2025-10-27 10:15:21 - retail_datagen.db.migration - INFO - Migration complete: dim_geographies - 1000 inserted, 0 errors
```

## Best Practices

1. **Always initialize databases first**: Use `--init` flag or call `init_databases()` before migration
2. **Migrate master before facts**: Master data must exist for FK references
3. **Use progress callbacks**: Monitor long-running migrations
4. **Check error logs**: Review `logs/migration.log` after migration
5. **Validate foreign keys**: Run validation after migration to ensure data integrity
6. **Backup existing databases**: Migration does not clear existing data (appends)

## Troubleshooting

### Issue: "CSV file not found"
- Ensure CSV files exist in expected locations (`data/master/`, `data/facts/`)
- Generate master/historical data first using the web UI or API

### Issue: "Batch failed: foreign key constraint"
- Master data must be migrated before fact data
- Ensure all referenced IDs exist in master tables

### Issue: "Migration too slow"
- Increase batch size: `--batch-size=50000`
- Check disk I/O (SQLite writes)
- Disable progress callbacks for fastest performance

### Issue: "Out of memory"
- Reduce batch size: `--batch-size=5000`
- Process fact tables one at a time instead of `--all`

## Integration with Existing System

The migration utilities integrate seamlessly with:
- **Database initialization**: Uses existing `init_databases()`
- **Session management**: Uses existing session makers
- **SQLAlchemy models**: Uses existing master and fact models
- **Configuration**: Follows existing path conventions

After migration, the system can:
- Generate new data directly into SQLite (Phase 2B)
- Stream events from SQLite to Azure Event Hub (Phase 2C)
- Query data using SQLAlchemy ORM

## Next Steps

After successful migration:
1. **Phase 2B**: Update generators to write directly to SQLite
2. **Phase 2C**: Update streaming to read from SQLite instead of CSVs
3. **Phase 2D**: Performance testing and optimization
4. **Phase 3**: Remove CSV dependencies entirely
