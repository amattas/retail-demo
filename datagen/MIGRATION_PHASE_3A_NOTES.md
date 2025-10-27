# Phase 3A Implementation Notes

## Summary
Updated `MasterDataGenerator` to support writing directly to SQLite database while maintaining CSV export as optional backward compatibility mode.

## Changes Made

### 1. Import Additions
- Added `AsyncSession` and `DeclarativeBase` from SQLAlchemy
- Added database model imports with try/except for graceful degradation
- Added `SQLALCHEMY_AVAILABLE` flag for runtime detection

### 2. New Helper Methods

#### `_insert_to_db()`
- Bulk inserts Pydantic models into SQLite via SQLAlchemy
- Batch size: 10,000 rows per flush
- Progress logging for large tables
- Converts Pydantic models to dicts, then maps to DB columns

#### `_map_pydantic_to_db_columns()`
- Maps Pydantic field names to SQLAlchemy column names
- Handles type conversions:
  - `Decimal` → `float` (SQLite compatibility)
  - `datetime` → ISO string
  - Preserves `None` values

#### `_run_async_in_thread()`
- Helper to run async coroutines in ThreadPoolExecutor
- Enables parallel execution with existing threading model

### 3. New Async Methods Created

Need to create async versions of:
- ✅ `generate_geography_master_async()`
- ⏳ `generate_stores_async()`
- ⏳ `generate_distribution_centers_async()`
- ⏳ `generate_trucks_async()`
- ⏳ `generate_customers_async()`
- ⏳ `generate_products_master_async()`
- ⏳ `generate_dc_inventory_snapshots_async()`
- ⏳ `generate_store_inventory_snapshots_async()`

### 4. Method Signature Updates

#### `generate_all_master_data_async()` (NEW)
```python
async def generate_all_master_data_async(
    self,
    session: AsyncSession | None = None,
    export_csv: bool = False,
    output_dir: Path | None = None,
    parallel: bool = True,
) -> None
```

**Behavior:**
- If `session` provided: writes to database
- If `export_csv=True`: also exports CSVs
- If both: writes to database AND CSVs
- If neither: CSV-only mode (backward compat)

#### `generate_all_master_data()` (UPDATED)
- Now wraps async version for backward compatibility
- Maintains existing signature

### 5. Insertion Order (Respects FK Constraints)
1. Geography (no dependencies)
2. Distribution Centers (depends on Geography)
3. Stores (depends on Geography)
4. Trucks (depends on DC)
5. Customers (depends on Geography) - parallel with Products
6. Products (no dependencies) - parallel with Customers
7. DC Inventory (depends on DC + Product)
8. Store Inventory (depends on Store + Product)

### 6. Error Handling
- Database errors logged with full traceback
- Automatic rollback on failure
- CSV export continues even if DB insert fails (defensive)
- Clear error messages for missing dependencies

### 7. Column Mapping

Pydantic models use same field names as database columns:
- `GeographyMaster.ID` → `dim_geographies.ID`
- `Store.StoreNumber` → `dim_stores.StoreNumber`
- `Customer.LoyaltyCard` → `dim_customers.LoyaltyCard`

**Special Cases:**
- `ProductMaster.Cost/MSRP/SalePrice`: Decimal → Float
- `ProductMaster.LaunchDate`: datetime → date (handled by SQLAlchemy)

## Usage Examples

### Old Usage (CSV only - still works)
```python
generator = MasterDataGenerator(config)
generator.generate_all_master_data()
```

### New Usage (SQLite only)
```python
from retail_datagen.db import get_master_session

generator = MasterDataGenerator(config)
async with get_master_session() as session:
    await generator.generate_all_master_data_async(session=session)
```

### Hybrid Usage (Both)
```python
async with get_master_session() as session:
    await generator.generate_all_master_data_async(
        session=session,
        export_csv=True,
        output_dir=Path("data/master")
    )
```

## Testing Strategy (Future Phase)
1. Test database-only mode
2. Test CSV-only mode (backward compat)
3. Test hybrid mode
4. Test FK constraint ordering
5. Test large data volumes (100k+ customers)
6. Test error recovery and rollback
7. Test parallel generation with DB writes

## Performance Considerations
- Batch inserts: 10,000 rows per flush
- In-memory DataFrames maintained for cross-referencing
- Single commit at end of all generation
- Progress logging throttled (avoid log spam)

## Backward Compatibility
✅ Existing code using `generate_all_master_data()` works unchanged
✅ CSV export still default when no session provided
✅ All existing tests should pass without modification
✅ Graceful degradation if SQLAlchemy not available

## Next Steps
1. Complete remaining async method implementations
2. Update API endpoints to use async version
3. Create integration tests
4. Update documentation
