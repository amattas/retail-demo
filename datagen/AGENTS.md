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
- `tax_jurisdictions.csv`: Tax rates by state/county/city `(StateCode, County, City, CombinedRate)`

### Master Dimensions (outputs)
- `geographies_master.csv`: `ID, City, State, ZipCode, District, Region`
- `stores.csv`: `ID, StoreNumber, Address, GeographyID, tax_rate, volume_class, store_format, operating_hours, daily_traffic_multiplier`
- `distribution_centers.csv`: `ID, DCNumber, Address, GeographyID`
- `trucks.csv`: `ID, LicensePlate, Refrigeration, DCID`
- `customers.csv`: `ID, FirstName, LastName, Address, GeographyID, LoyaltyCard, Phone, BLEId, AdId`
- `products_master.csv`: `ID, ProductName, Brand, Company, Department, Category, Subcategory, Cost, MSRP, SalePrice, RequiresRefrigeration, LaunchDate, taxability`
- Snapshots: `dc_inventory_snapshots.csv`, `store_inventory_snapshots.csv`

**Pricing invariants**: Cost < SalePrice ≤ MSRP; Cost is 50–85% of SalePrice.

**Tax system**:
- Stores have jurisdiction-based `tax_rate` (0% to 10.25%) based on geography
- Products have `taxability` field (TAXABLE, NON_TAXABLE, REDUCED_RATE)
- Tax calculation: `tax = subtotal * store.tax_rate * product.taxability_multiplier`
- Default tax rate: 7.407% if jurisdiction not found in tax_jurisdictions.csv

**Truck assignment**:
- Trucks can be assigned to DCs (DCID set) or pool/rental (DCID = NULL)
- Default assignment rate: 85% of DC-to-Store trucks assigned to DCs, 15% pool
- All supplier-to-DC trucks are pool trucks (DCID = NULL)
- Configurable via `volume.truck_dc_assignment_rate` or `volume.trucks_per_dc`

**Store profiles** (for realistic variability):
- Stores are classified into volume tiers: FLAGSHIP (5%), HIGH_VOLUME (15%), MEDIUM_VOLUME (50%), LOW_VOLUME (25%), KIOSK (5%)
- Each store has a `daily_traffic_multiplier` (0.25x - 3.0x) that varies transaction volume
- Store format influences basket size: HYPERMARKET (12-15 items), EXPRESS (1.5-3 items)
- Operating hours vary: ALWAYS_OPEN (24/7), EXTENDED (6am-midnight), STANDARD (8am-10pm)
- Geographic bias: urban stores more likely to be high-volume, rural more likely low-volume
- Final hourly customer count = base_rate × temporal_multiplier × store_multiplier
- See `docs/STORE_PROFILES.md` for complete specification

### Fact Tables (historical)
- `dc_inventory_txn`: `TraceId, EventTS, DCID, ProductID, QtyDelta, Reason`
- `truck_moves`: `TraceId, EventTS, TruckId, DCID, StoreID, ShipmentId, Status, ETA, ETD`
- `store_inventory_txn`: `TraceId, EventTS, StoreID, ProductID, QtyDelta, Reason, Source`
  - `Reason` (optional): InventoryReason enum (INBOUND_SHIPMENT, SALE, RETURN, ADJUSTMENT, etc.)
  - `Source` (optional): Source identifier (truck ID, receipt ID, adjustment ID)
- `receipts`: `TraceId, EventTS, StoreID, CustomerID, ReceiptId, Subtotal, Tax, Total, TenderType`
  - `Tax` calculated using store-specific tax rate and product taxability
- `receipt_lines`: `TraceId, EventTS, ReceiptId, Line, ProductID, Qty, UnitPrice, ExtPrice, PromoCode`
- `foot_traffic`: `TraceId, EventTS, StoreID, SensorId, Zone, Dwell, Count`
- `ble_pings`: `TraceId, EventTS, StoreID, BeaconId, CustomerBLEId, RSSI, Zone`
- `marketing`: `TraceId, EventTS, Channel, CampaignId, CreativeId, CustomerAdId, ImpressionId, Cost, Device`
  - `Cost` varies by channel ($0.10-$5.25) and device multiplier (1.0x mobile, 1.2x tablet, 1.5x desktop)
- `online_orders`: `TraceId, EventTS, OrderId, CustomerID, ProductID, Qty, Subtotal, Tax, Total, TenderType, FulfillmentStatus, FulfillmentMode, NodeType, NodeID`
  - `FulfillmentMode` (optional): SHIP_FROM_DC (60%), SHIP_FROM_STORE (30%), BOPIS (10%)
  - `NodeType` (optional): STORE or DC (indicates fulfillment location type)
  - `NodeID` (optional): Store ID or DC ID performing fulfillment

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
- Truck assignment: `volume.truck_dc_assignment_rate` (default 0.85) or `volume.trucks_per_dc` (fixed per-DC count).
- Marketing costs: `marketing_cost.*` configures per-channel cost ranges and device multipliers.

## Data Examples

### Tax Calculation
Tax is calculated based on store location and product taxability:

**Example 1: Standard taxable item in California**
```python
store.tax_rate = Decimal("0.0725")  # 7.25% California base rate
product.taxability = ProductTaxability.TAXABLE  # Standard goods
subtotal = Decimal("100.00")

# Tax calculation
tax = subtotal * store.tax_rate  # $7.25
total = subtotal + tax  # $107.25
```

**Example 2: Non-taxable groceries**
```python
store.tax_rate = Decimal("0.0825")  # 8.25% rate
product.taxability = ProductTaxability.NON_TAXABLE  # Groceries
subtotal = Decimal("50.00")

# Tax calculation
tax = Decimal("0.00")  # No tax on non-taxable items
total = subtotal  # $50.00
```

**Example 3: Reduced-rate items**
```python
store.tax_rate = Decimal("0.1025")  # 10.25% rate (high jurisdiction)
product.taxability = ProductTaxability.REDUCED_RATE  # Special category
subtotal = Decimal("75.00")

# Tax calculation (reduced rate implementation varies, typically 50% of standard)
tax = subtotal * store.tax_rate * Decimal("0.50")  # $3.84
total = subtotal + tax  # $78.84
```

### Marketing Costs
Marketing costs vary by channel and device type:

**Channel-based cost ranges** (per impression):
- Email: $0.10 - $0.25
- Display: $0.50 - $2.00
- Social (Facebook/Instagram): $0.75 - $3.00
- Search (Google): $1.00 - $5.00
- Video (YouTube): $1.50 - $5.25

**Device multipliers**:
- Mobile: 1.0x (baseline)
- Tablet: 1.2x (higher engagement)
- Desktop: 1.5x (highest engagement)

**Example calculation**:
```python
channel = MarketingChannel.GOOGLE  # Search ads
device = DeviceType.DESKTOP

# Base cost randomly selected from range
base_cost = random.uniform(1.00, 5.00)  # e.g., $2.50

# Apply device multiplier
final_cost = base_cost * 1.5  # $3.75 for desktop
```

### Inventory Transaction Source Tracking
Store inventory transactions include reason and source for audit trail:

**Example 1: Inbound shipment from truck**
```python
StoreInventoryTransaction(
    StoreID=42,
    ProductID=1523,
    QtyDelta=100,  # Received 100 units
    Reason=InventoryReason.INBOUND_SHIPMENT,
    Source="TRUCK-8734"  # Truck license plate
)
```

**Example 2: Sale from receipt**
```python
StoreInventoryTransaction(
    StoreID=42,
    ProductID=1523,
    QtyDelta=-5,  # Sold 5 units
    Reason=InventoryReason.SALE,
    Source="RCP-2024-01-15-00001"  # Receipt ID
)
```

**Example 3: Inventory adjustment**
```python
StoreInventoryTransaction(
    StoreID=42,
    ProductID=1523,
    QtyDelta=-3,  # Shrinkage
    Reason=InventoryReason.DAMAGED,
    Source="ADJ-20240115-001"  # Adjustment ID
)
```

### Online Order Fulfillment
Online orders route through different fulfillment modes:

**Example 1: Ship from DC (60% probability)**
```python
OnlineOrder(
    OrderId="ORD-2024-001",
    CustomerID=5432,
    FulfillmentMode="SHIP_FROM_DC",
    NodeType="DC",
    NodeID=3,  # DC ID
    FulfillmentStatus="created"
)
```

**Example 2: Ship from Store (30% probability)**
```python
OnlineOrder(
    OrderId="ORD-2024-002",
    CustomerID=7821,
    FulfillmentMode="SHIP_FROM_STORE",
    NodeType="STORE",
    NodeID=42,  # Store ID
    FulfillmentStatus="created"
)
```

**Example 3: Buy Online Pickup In Store (10% probability)**
```python
OnlineOrder(
    OrderId="ORD-2024-003",
    CustomerID=1234,
    FulfillmentMode="BOPIS",
    NodeType="STORE",
    NodeID=15,  # Store ID for pickup
    FulfillmentStatus="created"
)
```

### Truck Assignment Strategies
Trucks can be assigned to DCs or remain in a pool:

**Strategy 1: Percentage-based (default 85%)**
```python
total_trucks = 100
assigned_trucks = int(100 * 0.85)  # 85 trucks assigned to DCs
pool_trucks = 15  # 15 pool/rental trucks (DCID = NULL)

# Round-robin assignment across DCs
for i in range(assigned_trucks):
    dc_id = dcs[i % len(dcs)].ID
    truck.DCID = dc_id
```

**Strategy 2: Fixed per-DC count**
```python
trucks_per_dc = 10
dc_count = 8
assigned_trucks = 10 * 8  # 80 trucks (10 per DC)
remaining_pool = total_trucks - 80  # Remainder are pool

# Each DC gets exactly 10 trucks
for dc in dcs:
    for _ in range(trucks_per_dc):
        truck.DCID = dc.ID
```

## Field Constraints and Enums

### ProductTaxability Enum
Product tax classification affects tax calculation:
- `TAXABLE`: Standard taxable goods (full tax rate applied)
- `NON_TAXABLE`: Exempt items like groceries in some jurisdictions (0% tax)
- `REDUCED_RATE`: Items with reduced tax rate (implementation-specific, typically 50% of standard rate)

### InventoryReason Enum
Tracks why inventory changed:
- `INBOUND_SHIPMENT`: Goods received from truck/supplier
- `OUTBOUND_SHIPMENT`: Goods shipped to another location
- `SALE`: Sold to customer (negative QtyDelta)
- `RETURN`: Customer return (positive QtyDelta)
- `ADJUSTMENT`: Manual inventory correction
- `DAMAGED`: Damaged goods write-off (negative QtyDelta)
- `LOST`: Lost/stolen goods (negative QtyDelta)

### FulfillmentMode Values
How online orders are fulfilled:
- `SHIP_FROM_DC`: Order ships from distribution center (60% of orders)
- `SHIP_FROM_STORE`: Order ships from retail store (30% of orders)
- `BOPIS`: Buy Online Pickup In Store (10% of orders)

### NodeType Values
Type of fulfillment location:
- `DC`: Distribution center
- `STORE`: Retail store

### MarketingChannel Enum
Digital marketing channels with varying costs:
- `EMAIL`: $0.10-$0.25 per impression
- `DISPLAY`: $0.50-$2.00 per impression
- `SOCIAL`: $0.75-$3.00 per impression (Facebook, Instagram)
- `SEARCH`: $1.00-$5.00 per impression (Google)
- `VIDEO`: $1.50-$5.25 per impression (YouTube)
- `FACEBOOK`, `GOOGLE`, `INSTAGRAM`, `YOUTUBE`: Platform-specific channels

### DeviceType Enum
Device type affects marketing cost via multiplier:
- `MOBILE`: 1.0x multiplier (baseline)
- `TABLET`: 1.2x multiplier (20% premium for higher engagement)
- `DESKTOP`: 1.5x multiplier (50% premium for highest engagement)

### TenderType Enum
Payment methods:
- `CASH`: Physical currency
- `CREDIT_CARD`: Credit card payment
- `DEBIT_CARD`: Debit card payment
- `CHECK`: Paper check
- `MOBILE_PAY`: Mobile payment (Apple Pay, Google Pay, etc.)

### TruckStatus Enum
Truck movement lifecycle:
- `SCHEDULED`: Shipment scheduled but not started
- `LOADING`: Currently being loaded at DC
- `IN_TRANSIT`: En route to destination
- `ARRIVED`: Arrived at destination
- `UNLOADING`: Being unloaded at store
- `COMPLETED`: Delivery complete
- `DELAYED`: Experiencing delay

### Field Nullability
**Optional fields** (can be NULL/None):
- `Store.tax_rate`: Defaults to 7.407% if not found in tax jurisdiction mapping
- `Truck.DCID`: NULL for pool/rental trucks (15% of fleet by default)
- `StoreInventoryTransaction.Reason`: Optional reason code for inventory changes
- `StoreInventoryTransaction.Source`: Optional source identifier (truck ID, receipt ID, etc.)
- `OnlineOrder.FulfillmentMode`: Optional fulfillment method
- `OnlineOrder.NodeType`: Optional node type (STORE or DC)
- `OnlineOrder.NodeID`: Optional node ID (Store ID or DC ID)
- `ProductMaster.taxability`: Defaults to TAXABLE if not specified

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
