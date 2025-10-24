# Retail Data Generator — AGENTS.md

Authoritative spec for data contracts, invariants, safety rules, and contributor runbooks.

## Goals
- Generate realistic but synthetic retail datasets for POCs and analytics.
- Support master data, historical facts, and real-time streaming to Azure Event Hubs.
- Enforce safety: no real PII, no real brands or companies.

## Non-goals
- Not a production OLTP/OLAP store. Output format is CSV.
- Not a full simulation of all retail processes; focuses on common analytics scenarios.

## Data Contracts

### Dictionaries (inputs)
- `geographies.csv`: ~578 rows `(City, State, Zip, District, Region)`
- `first_names.csv`: ~312 synthetic names
- `last_names.csv`: ~363 synthetic names
- `product_companies.csv`: 112 companies (current repo)
- `product_brands.csv`: 629 brand-company-category mappings (current repo)
- `products.csv`: 599 base products (current repo)

### Master Dimensions (outputs)
- `geographies_master.csv`: `ID, City, State, ZipCode, District, Region`
- `stores.csv`: `ID, StoreNumber, Address, GeographyID`
- `distribution_centers.csv`: `ID, DCNumber, Address, GeographyID`
- `trucks.csv`: `ID, LicensePlate, Refrigeration, DCID`
- `customers.csv`: `ID, FirstName, LastName, Address, GeographyID, LoyaltyCard, Phone, BLEId, AdId`
- `products_master.csv`: `ID, ProductName, Brand, Company, Department, Category, Subcategory, Cost, MSRP, SalePrice, RequiresRefrigeration, LaunchDate`
- Snapshots: `dc_inventory_snapshots.csv`, `store_inventory_snapshots.csv`

Pricing invariants: Cost < SalePrice ≤ MSRP; Cost is 50–85% of SalePrice.

### Fact Tables (historical)
- `dc_inventory_txn`: `TraceId, EventTS, DCID, ProductID, QtyDelta, Reason`
- `truck_moves`: `TraceId, EventTS, TruckId, DCID, StoreID, ShipmentId, Status, ETA, ETD`
- `store_inventory_txn`: `TraceId, EventTS, StoreID, ProductID, QtyDelta, Reason, Source`
- `receipts`: `TraceId, EventTS, StoreID, CustomerID, ReceiptId, Subtotal, Tax, Total, TenderType`
- `receipt_lines`: `TraceId, EventTS, ReceiptId, Line, ProductID, Qty, UnitPrice, ExtPrice, PromoCode`
- `foot_traffic`: `TraceId, EventTS, StoreID, SensorId, Zone, Dwell, Count`
- `ble_pings`: `TraceId, EventTS, StoreID, BeaconId, CustomerBLEId, RSSI, Zone`
- `marketing`: `TraceId, EventTS, Channel, CampaignId, CreativeId, CustomerAdId, ImpressionId, Cost, Device`
- `online_orders`: `TraceId, EventTS, OrderId, CustomerID, FulfillmentMode, FulfillmentNodeType, FulfillmentNodeID, Subtotal, Tax, Total, TenderType`

### Real-Time Event Envelope
```
{
  "event_type": "receipt_created",
  "payload": { /* event-specific */ },
  "trace_id": "string",
  "ingest_timestamp": "ISO-8601",
  "schema_version": "1.0",
  "source": "retail-datagen",
  "correlation_id": "string|null",
  "partition_key": "string|null"
}
```

Event types are defined in `retail_datagen.streaming.schemas.EventType`.
Key types include:
- `receipt_created`, `receipt_line_added`, `payment_processed`
- `inventory_updated`, `stockout_detected`, `reorder_triggered`
- `customer_entered`, `customer_zone_changed`, `ble_ping_detected`
- `truck_arrived`, `truck_departed`, `store_opened`, `store_closed`
- `ad_impression`, `promotion_applied`
- `online_order_created`, `online_order_picked`, `online_order_shipped`

## Configuration
`RetailConfig` in `retail_datagen.config.models`.
- Paths: prefer `paths.dictionaries`, `paths.master`, `paths.facts` (alias `dict` accepted).
- Historical: `historical.start_date`.
- Realtime: burst, interval, batch, retry, circuit breaker, buffer.
- Volume: `online_orders_per_day` controls the scale of online orders (historical and streaming pacing).

## Safety Rules
- No real names or brands; see `SyntheticDataValidator` blocklist.
- Pricing relationships enforced by `ProductMaster` model validator.
- FK integrity validated before export.

## Runbook
- Install: `pip install -r requirements.txt -r requirements-dev.txt` in a Python 3.11 venv.
- Generate master data via API or `MasterDataGenerator`.
- Generate historical: `/api/generate/historical` or `FactDataGenerator`.
- Stream real-time: `/api/stream/start` (uses `EventStreamer` and Azure client).
- Validate: run tests with Python 3.11 and plugin autoload disabled: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest -q`.

## Test Plan
- Unit: models, pricing, validators, config.
- Integration: end-to-end master generation on small dictionaries; inventory snapshots present; trucks present.

## API Contracts

### Generation Status Response

The `/api/tasks/{task_id}/status` endpoint returns enhanced progress information:

**Base Fields** (required):
- `status: GenerationStatus` - Current status (pending/running/completed/failed/cancelled)
- `progress: float` - Progress value between 0.0 and 1.0
- `message: str` - Human-readable status message
- `tables_completed: list[str]` - List of completed tables
- `tables_remaining: list[str]` - List of remaining tables

**Enhanced Progress Fields** (optional, added 2025-10-21):
- `tables_in_progress: list[str] | None` - Currently active tables
- `estimated_seconds_remaining: float | None` - Approximate seconds until completion (≥ 0.0)
- `progress_rate: float | None` - Progress velocity as rolling average (≥ 0.0)
- `last_update_timestamp: str | None` - ISO-8601 timestamp of last update

**Example Response**:
```json
{
  "status": "running",
  "progress": 0.45,
  "message": "Processing receipts (3/8 tables complete)",
  "tables_completed": ["dc_inventory_txn", "truck_moves", "store_inventory_txn"],
  "tables_remaining": ["receipt_lines", "foot_traffic", "ble_pings", "marketing"],
  "tables_in_progress": ["receipts"],
  "estimated_seconds_remaining": 45.2,
  "progress_rate": 0.01,
  "last_update_timestamp": "2025-10-21T14:28:15.123Z"
}
```

All enhanced fields are backward compatible (optional with `None` defaults).

## Contributor Notes
- Use `ruff` and `mypy` (strict) locally.
- Keep README high-level; deep spec stays here.
- See `CHANGELOG.md` for release notes and version history.
- Progress reporting tests: `tests/unit/test_progress_reporting.py`, `tests/integration/test_progress_integration.py`

### ETA Calculation Algorithm

The progress ETA calculation (`fact_generator.py:238-266`) uses a rolling history approach:

1. **History Tracking**: Maintains `_progress_history` list of `(timestamp, progress)` tuples
2. **Rate Calculation**: `progress_rate = (newest_progress - oldest_progress) / time_elapsed`
3. **ETA Formula**: `estimated_seconds = (1.0 - current_progress) / progress_rate`
4. **Safety**: Returns `None` if insufficient history (<2 entries) or invalid rates (≤0)

This provides a simple but effective linear projection based on recent progress velocity. For production, consider exponential smoothing for more stable estimates with variable workloads.
