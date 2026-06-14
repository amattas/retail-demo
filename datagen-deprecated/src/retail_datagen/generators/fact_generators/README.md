# fact_generators Package

This package contains the modularized fact data generation engine, refactored from the original monolithic `fact_generator.py` (4430 lines).

## Package Structure

```
fact_generators/
├── __init__.py                    # Public API exports
├── README.md                      # This file
├── core.py                        # Main FactDataGenerator class (594 lines)
├── models.py                      # Dataclasses (34 lines)
├── progress.py                    # HourlyProgressTracker (206 lines)
├── convenience.py                 # Module-level functions (31 lines)
│
├── Mixin modules (functional groupings):
├── data_loading_mixin.py          # Master data loading (151 lines)
├── inventory_mixin.py             # Inventory management (930 lines)
├── logistics_mixin.py             # Truck logistics (270 lines)
├── marketing_mixin.py             # Marketing campaigns (261 lines)
├── online_orders_mixin.py         # Online orders (50 lines)
├── persistence_mixin.py           # Database persistence (883 lines)
├── progress_reporting_mixin.py    # Progress tracking (455 lines)
├── receipts_mixin.py              # Receipt generation (294 lines)
├── seasonal_mixin.py              # Seasonal patterns (148 lines)
├── sensors_mixin.py               # Sensor data (216 lines)
└── utils_mixin.py                 # Utility helpers (509 lines)
```

## Module Responsibilities

### Core Modules

**core.py**
- Main `FactDataGenerator` class
- Inherits from all mixin classes
- Contains initialization, configuration, and orchestration logic
- Main entry point: `generate_historical_data()` method

**models.py**
- `FactGenerationSummary`: Results dataclass
- `MasterTableSpec`: Master table specification (deprecated, CSV-based)

**progress.py**
- `HourlyProgressTracker`: Thread-safe hourly progress tracking

**convenience.py**
- `generate_historical_facts()`: Convenience function for direct usage

### Mixin Modules

**data_loading_mixin.py** - Master Data Loading
- Methods: `load_master_data_from_duckdb`, `load_master_data`, `_normalize_*`
- Loads and normalizes dimension data from DuckDB/CSV

**inventory_mixin.py** - Inventory Management
- Methods: `_generate_dc_inventory_transactions`, `_build_store_customer_pools`
- Manages DC and store inventory transactions
- Customer-store assignment and sampling

**logistics_mixin.py** - Truck Logistics
- Methods: `_generate_truck_movements`, `_process_truck_lifecycle`, `_process_truck_deliveries`
- Simulates truck movements from DC to stores
- Handles delivery schedules and inventory transfers

**marketing_mixin.py** - Marketing Campaigns
- Methods: `_generate_marketing_activity`, `_compute_marketing_multiplier`
- Generates multi-channel marketing campaigns
- Computes marketing effectiveness multipliers

**online_orders_mixin.py** - Online Orders
- Methods: `_generate_online_orders`
- Generates online order lifecycle events (created, picked, shipped)

**persistence_mixin.py** - Database Persistence
- Methods: `_get_model_for_table`, `_build_outbox_rows_from_df`, `_map_field_names_for_db`
- Maps fact tables to SQLAlchemy models (DuckDB mode bypasses this)
- Builds streaming outbox rows for event publishing

**progress_reporting_mixin.py** - Progress Reporting
- Methods: `_send_throttled_progress_update`, `_emit_table_progress`, `set_progress_callback`
- Manages progress callbacks with throttling
- ETA calculation and table state tracking

**receipts_mixin.py** - Receipt Generation
- Methods: `_generate_store_hour_activity`, `_create_receipt`
- Generates customer receipts and receipt lines
- Handles basket composition and payment tenders

**seasonal_mixin.py** - Seasonal Patterns
- Methods: `_thanksgiving_date`, `_get_product_multiplier`, `_apply_holiday_overlay_to_basket`
- Implements holiday-specific logic
- Product demand multipliers for seasonal events

**sensors_mixin.py** - Sensor Data
- Methods: `_generate_foot_traffic`, `_generate_ble_pings`
- Generates foot traffic counts
- Simulates BLE beacon pings for customer tracking

**utils_mixin.py** - Utility Helpers
- Methods: `_get_available_products_for_date`, `_is_food_product`, `_generate_trace_id`, etc.
- Date/time randomization
- Product filtering and classification

## Backward Compatibility

The parent `fact_generator.py` now acts as a compatibility shim, re-exporting all public symbols:

```python
# Old code - still works
from retail_datagen.generators.fact_generator import FactDataGenerator

# New code - equivalent
from retail_datagen.generators.fact_generators import FactDataGenerator
```

## Usage

```python
from retail_datagen.config.models import RetailConfig
from retail_datagen.generators.fact_generators import FactDataGenerator
from datetime import datetime

# Initialize
config = RetailConfig.from_file("config.json")
generator = FactDataGenerator(config)

# Load master data
generator.load_master_data_from_duckdb()

# Generate historical facts
summary = await generator.generate_historical_data(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 31),
)

print(f"Generated {summary.total_records} fact records")
```

## Design Rationale

### Why Mixins?

The mixin pattern was chosen over alternatives (e.g., composition, separate classes) for several reasons:

1. **Preserves backward compatibility** - Public API remains identical
2. **Logical grouping** - Related methods are co-located
3. **Shared state access** - All mixins can access `self.config`, `self.stores`, etc.
4. **No method duplication** - Single inheritance hierarchy
5. **Manageable file sizes** - Largest module is 930 lines (inventory_mixin), down from 4430

### Module Size Goals

- Target: ~500 lines per module
- Maximum: 800 lines (with some exceptions for complex logic)
- Achieved: All modules under 950 lines

### Testing Strategy

Tests were updated to use the new import paths but remain functionally identical. No business logic was changed during refactoring.

## Field Naming Conventions

### Generator Layer (PascalCase)
The fact generation mixins use **PascalCase** for field names in dictionary records:
- `EventTS`, `TraceId`, `StoreID`, `CustomerID`, `ProductID`
- `ReceiptId`, `OrderId`, `PromoCode`, `DiscountAmount`

### Streaming Layer (snake_case)
The streaming schemas (`streaming/schemas.py`) use **snake_case** for payload fields:
- `event_ts`, `trace_id`, `store_id`, `customer_id`, `product_id`
- `receipt_id`, `order_id`, `promo_code`, `discount_amount`

### Database Layer (snake_case)
The database models and KQL tables use **snake_case** for column names.

### Automatic Field Mapping
The `persistence_mixin.py` module handles automatic transformation between naming conventions:

- **`_map_field_names_for_db()`**: Maps PascalCase (generator) → snake_case (database)
- **`_build_outbox_rows_from_df()`**: Converts fact records to streaming event payloads

The mapping is table-specific and documented in the `table_specific_mappings` dictionary (lines 233-383).

### Example Field Mappings

**Receipts Table:**
```python
"StoreID" → "store_id"
"CustomerID" → "customer_id"
"ReceiptId" → "receipt_id_ext"  # External linking key
"TenderType" → "payment_method"
"Tax" → "tax_amount"
```

**Promotions Table:**
```python
"ReceiptId" → "receipt_id_ext"
"PromoCode" → "promo_code"
"DiscountAmount" → "discount_amount"
"DiscountType" → "discount_type"  # Values: "PERCENTAGE", "FIXED_AMOUNT", "BOGO"
"ProductIds" → "product_ids"
```

### Important Notes
- **TraceId** field is excluded during database persistence (not stored in fact tables)
- External IDs (e.g., `receipt_id_ext`, `order_id_ext`) are used for cross-table linking
- The mapping layer ensures data integrity between generation and persistence

## Migration Notes

### For Contributors

When adding new fact generation logic:

1. Identify the appropriate mixin based on functionality
2. Add methods to that mixin following existing patterns
3. Methods should use `self.*` for accessing shared state
4. Update tests if adding new public methods
5. **Use PascalCase for generator field names**, snake_case mapping will be handled automatically

### For External Code

No changes needed. The compatibility shim ensures all existing imports continue to work.

## Maintenance

When modifying this package:

- **DO NOT** move methods between mixins without updating tests
- **DO NOT** change public method signatures (breaks backward compat)
- **DO** add new mixins if a functional area grows beyond 800 lines
- **DO** keep the compatibility shim in sync with `__init__.py`

## Original File

The original `fact_generator.py` has been preserved as `fact_generator.py.bak` for reference.
