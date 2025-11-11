# Retail Big Data Generator

A comprehensive synthetic retail data generator that produces **realistic but entirely synthetic** retail data for analytics POCs, supporting Parquet exports and real-time Azure Event Hub streaming.

## Purpose

Generate synthetic but realistic retail data that simulates real-world retail behaviors including seasonality, promotions, inventory flows, and customer shopping patterns. **Never outputs actual real names or addresses** — only synthetic/fictitious values.

## Generation Modes

1. **Dimension Data**: Build dimension tables from dictionary CSV inputs → persisted in DuckDB; optional Parquet export under `data/export/<table>/<table>.parquet`
2. **Fact Data**: Generate fact tables between configurable timestamps → monthly Parquet files `data/export/<table>/<table>_YYYY-MM.parquet`  
3. **Real-Time Data**: Stream incremental events to Azure Event Hubs with intelligent state tracking

## Key Features

### 🏗️ **Dimension Data Generation**
- **Geographic Distribution**: Realistic store/DC placement across 100 selected geographies
- **Product Hierarchy**: Department → Category → Subcategory classification
- **Pricing Intelligence**: MSRP = Base ±15%, SalePrice with 40% discount probability, Cost = 50-85% of SalePrice
- **Synthetic Safety**: Generated names, addresses, phone numbers, loyalty cards, BLE IDs, Ad IDs

### 📊 **Historical Fact Generation**
- **Intelligent Date Ranges**: First run uses config start → current, subsequent runs use last generated → current
- **Retail Transaction Flows**: Receipts → Lines → Inventory Updates → Truck/DC Flows
- **Multi-Fact Tables**: 9 interconnected fact tables with full referential integrity
- **Seasonal Patterns**: Realistic daily/seasonal variation in customer traffic and purchases

### ⚡ **Real-Time Streaming** ⭐ **ENHANCED**
- **Customer Session Orchestrator**: Maintains consistency across foot traffic, BLE pings, and receipts
- **Marketing Attribution**: Industry-standard conversion tracking from ads to in-store purchases
- **Truck Logistics**: Full supply chain simulation with refrigerated transport capabilities
- **State-Aware**: Only starts after fact data exists, continues from last generated timestamp
- **Event Bursts**: Configurable mixed event streams to Azure Event Hub
- **Circuit Breaker**: Built-in resilience with retry logic and dead letter queuing
- **Data Consistency**: Enforces foot traffic ≥ BLE customers ≥ receipt customers

### 🌐 **FastAPI Web Interface**
- **Interactive UI**: Complete web interface for all operations
- **REST API**: Full programmatic access with OpenAPI documentation
- **Data Management**: Clear all data functionality with triple confirmation safety
- **Real-time Status**: Generation state monitoring and progress tracking
- **Enhanced Progress Display**: Table completion counter, ETA estimation, and smooth throttled updates

### 🔒 **Data Integrity & Safety**
- **FK Validation**: Complete foreign key relationship validation across all tables
- **Reproducibility**: RNG seed ensures deterministic outputs for testing
- **Synthetic Only**: No real personal data - all generated names, addresses, identifiers
- **Pricing Constraints**: Enforced Cost < SalePrice ≤ MSRP relationships

## 🆕 Latest Enhancements

### 🔧 **Comprehensive Data Quality Initiative** ⭐ **MAJOR UPDATE**

A comprehensive code review and fix initiative has addressed all data quality and realism issues:

**Receipt & Financial Integrity:**
- ✅ 100% of receipts now have ≥1 line items (fixed 2.6% empty receipts)
- ✅ All receipt totals accurate within $0.01 (fixed 0.06% mismatches)
- ✅ Proper formula: `Total = Subtotal - Discount + Tax`
- ✅ All financial calculations use Decimal precision (no floating point errors)

**Tax System Overhaul:**
- ✅ Store-specific tax rates loaded from 164 tax jurisdictions (state/county/city)
- ✅ Product taxability system (TAXABLE, NON_TAXABLE, REDUCED_RATE)
- ✅ Tax calculated on post-discount amounts (industry standard)
- ✅ Reduced-rate products taxed at 50% of normal rate

**Promotion System:**
- ✅ 13 promotion types with seasonal patterns (SAVE10, BFRIDAY30, SUMMER25, etc.)
- ✅ 10-20% of receipts now have promotions with proper discounts
- ✅ PromoCode tracking in receipt_lines
- ✅ Customer segment-based promotion usage (Budget: 30%, Premium: 5%)

**Store & Customer Variability:**
- ✅ Store profiles create realistic variability (FLAGSHIP/HIGH/MEDIUM/LOW/KIOSK)
- ✅ Coefficient of variation > 0.6 (strong variability in store volumes)
- ✅ Customer home geographies with distance-based store affinity
- ✅ 70% of customer visits at top 2 home stores
- ✅ 80% of trips under 10 miles from customer home

**Complete Lifecycle Management:**
- ✅ Truck 6-status lifecycle (SCHEDULED → LOADING → IN_TRANSIT → ARRIVED → UNLOADING → COMPLETED)
- ✅ DC OUTBOUND and Store INBOUND inventory transactions linked via shipment_id
- ✅ Online orders with multi-line support (1-5 items per order)
- ✅ Online order 4-status progression (created → picked → shipped → delivered)
- ✅ Location-aware tax calculation for online orders

**Sensor & Marketing Data:**
- ✅ Foot traffic with aggregate counts and 10-30% conversion rates
- ✅ BLE 30% customer_id match rate (was 0%)
- ✅ Marketing 5% customer_id resolution (was 0%)
- ✅ Marketing costs match documented ranges per channel

**New Files Added:**
- `src/retail_datagen/shared/tax_utils.py` - Tax calculation engine
- `src/retail_datagen/shared/promotion_utils.py` - Promotion system
- `src/retail_datagen/shared/customer_geography.py` - Geographic assignment
- `src/retail_datagen/generators/online_order_generator.py` - Order lifecycle
- 18+ validation scripts for quality assurance

**For complete implementation details, see CHANGELOG.md and the comprehensive fix documentation.**

### 🛍️ Online Orders Integration — Unified
- Online orders are now a first-class fact table (`online_orders`) integrated into the core generator and streamer.
- Inventory impacts from online orders are applied to existing tables: `store_inventory_txn` and `dc_inventory_txn` with `Source=ONLINE`.
- Seasonal/holiday patterns affect online order rates in both historical and streaming modes.
- Uses the same synthetic customers and products as the rest of the system.

**Note on Legacy Code**: The `retail_datagen/omnichannel` module represents an earlier implementation approach and is deprecated. Online orders are now fully integrated into the core `fact_generator.py` and `event_streamer.py` modules. All new development should use the unified generator and streaming paths.

### 📊 **Enhanced Progress Display for Historical Generation** ⭐ **NEW**
- **Table Completion Counter**: Real-time display showing "X/8 tables complete" during generation
- **ETA Estimation**: Intelligent time-remaining calculation based on progress history (e.g., "~2 minutes")
- **Progress Throttling**: 100ms minimum interval between updates prevents API flooding for smooth UX
- **Enhanced API Fields**: New optional fields in generation status responses:
  - `tables_in_progress`: Currently active tables
  - `estimated_seconds_remaining`: Approximate completion time in seconds
  - `progress_rate`: Progress velocity (rolling average)
  - `last_update_timestamp`: ISO-8601 timestamp of last update
- **Responsive Design**: Mobile-friendly progress display with optimized styling
- **Backward Compatible**: All new API fields are optional; existing integrations unaffected

### 🛒 **Enhanced Product Catalog & Brand Combinations** ⭐ **NEW**
- **Expanded Product Categories**: 598 base products spanning 10+ major retail categories:
  - **Grocery**: Traditional food items, beverages, produce, meat, dairy
  - **Electronics**: Headphones, charging cables, fitness trackers, computer accessories
  - **Clothing**: Men's, women's, and children's apparel, shoes, accessories
  - **Health & Beauty**: Vitamins, skincare, oral care, personal care products
  - **Home & Garden**: Kitchen items, cleaning supplies, tools, lighting, bedding
  - **Baby & Kids**: Formula, diapers, toys, books, feeding supplies
  - **Pet Supplies**: Dog/cat food, toys, leashes, litter, health products
  - **Automotive**: Motor oil, accessories, maintenance supplies
  - **Office Supplies**: Paper, ink, furniture, organizational tools
  - **Seasonal & Sports**: Holiday decorations, sporting goods, outdoor recreation

- **Synthetic Brand Combinations**: Smart product generation creates multiple synthetic brand/company combinations:
  - Same product offered by different synthetic retailers with varying synthetic brand names
  - Completely synthetic retail private labels and consumer brands - NO REAL BRANDS
  - All brand names are generated synthetically to avoid trademark issues
  - Price variation per brand: ±15% realistic market pricing differences

- **Smart Product Generation**: Creates up to 10,000 unique product SKUs from base products through brand combinations
- **Enhanced Refrigeration Logic**: Automatic classification of temperature-sensitive products for supply chain routing

### 🚛 **Supply Chain & Logistics** ⭐ **ENHANCED**
- **Advanced Truck Fleet Management**: Auto-generated truck fleet with realistic license plates and refrigeration capabilities
- **Multi-Tier Supply Chain**: Full Supplier → DC → Truck → Store flow simulation
- **Refrigerated Transport**: Specialized routing for temperature-sensitive products with RequiresRefrigeration logic
- **Geographic Supply Chain Constraints**: Stores only generated in states that have distribution centers
- **Supplier Truck Integration**: Dedicated supplier trucks (DCID=0) for supplier-to-DC transportation
- **Enhanced Inventory Snapshots**: Realistic starting inventory levels (DCs: 500-5000 units, Stores: 20-200 units)
- **Supply Chain Disruption Controls**: Real-time streaming disruption simulation integrated into web UI
- **Logistics UI Integration**: Comprehensive truck and supply chain management in web interface

### 🎯 **Marketing Attribution & Conversion Tracking**
- **Industry-Standard Conversion Rates**: Channel-specific rates (Search: 3.5%, Email: 2.5%, Social: 1.2%, Display: 0.8%)
- **Real Conversion Funnel**: Ad impressions → Store visits → Purchases with realistic 1-48 hour delay windows
- **Enhanced Purchase Behavior**: Marketing-driven customers show 80% purchase rate vs 40% baseline
- **Higher Spend Patterns**: Marketing customers spend 30% more with 25% larger baskets
- **Full Attribution Chain**: Links impression IDs to actual store visits and purchase transactions

### 👥 **Customer Session Orchestrator**
- **Data Consistency Engine**: Enforces foot traffic ≥ BLE unique customers ≥ receipt customers
- **Customer Journey Tracking**: Entry → Zone movement → Purchase → Exit with realistic timing
- **Session Lifecycle Management**: Automatic cleanup of expired sessions and store occupancy tracking
- **Realistic Behavior Patterns**: Marketing customers stay 20% longer with higher purchase intent

### 🎛️ **Enhanced User Interface**
- **Truck Management**: Trucks table added to Dimension Data tab with preview functionality
- **Unified Data Controls**: Clear All Data button added to Dimension Data tab for better UX
- **Real-time Status**: Improved tracking of customer sessions and marketing conversions
- **Data Preview**: Enhanced table preview system for all dimension data including trucks

For implementation details and full data contracts, see `AGENTS.md`.

### Online Orders (Unified)
- Configure daily volume via `volume.online_orders_per_day` in `config.json` or the UI.
- Fact (historical): exported monthly to `data/export/fact_online_orders/fact_online_orders_YYYY-MM.parquet` and inventory effects applied to `store_inventory_txn`/`dc_inventory_txn` with `Source=ONLINE`.
- Streaming: emits `online_order_created` followed by `online_order_picked` and `online_order_shipped`, plus an `inventory_updated` SALE for the fulfillment node.
- Seasonality/Holidays: applied to online orders in both historical and streaming modes.

## Architecture

### Project Structure
```
src/retail_datagen/
├── api/              # FastAPI models and schemas  
├── config/           # Configuration management
├── generators/       # Core generation engines
│   ├── master_generator.py    # Dimension table generation
│   ├── fact_generator.py      # Fact generation  
│   ├── generation_state.py    # State tracking for incremental generation
│   ├── retail_patterns.py     # Business logic patterns
│   └── seasonal_patterns.py   # Seasonality and temporal patterns
├── shared/           # Common models, utilities, validators
├── streaming/        # Real-time Azure Event Hub integration
└── main.py          # FastAPI application entry point (spec: AGENTS.md)
```

### Dictionary Files (CSV Inputs)
- **geographies.csv**: ~578 synthetic locations `(City, State, Zip, District, Region)`
- **first_names.csv**: ~312 synthetic first names  
- **last_names.csv**: ~363 synthetic last names
- **product_companies.csv**: 112 synthetic retail companies (current repo)
- **product_brands.csv**: 629 synthetic brand-company mappings (current repo)
- **products.csv**: 599 base products with `(ProductName, BasePrice, Department, Category, Subcategory)` spanning 10+ categories

### Generated Dimensions (Parquet Exports)
Dimension tables are persisted in DuckDB (`dim_*`) and can be exported to Parquet under `data/export/<table>/<table>.parquet`.

### Fact Tables (Historical & Real-Time)
- **dc_inventory_txn**: `TraceId, EventTS, DCID, ProductID, QtyDelta, Reason`
- **truck_moves**: `TraceId, EventTS, TruckId, DCID, StoreID, ShipmentId, Status, ETA, ETD`  
- **store_inventory_txn**: `TraceId, EventTS, StoreID, ProductID, QtyDelta, Reason, Source`
- **receipts**: `TraceId, EventTS, StoreID, CustomerID, ReceiptId, Subtotal, Tax, Total, TenderType`
- **receipt_lines**: `TraceId, EventTS, ReceiptId, Line, ProductID, Qty, UnitPrice, ExtPrice, PromoCode`
- **foot_traffic**: `TraceId, EventTS, StoreID, SensorId, Zone, Dwell, Count`
- **ble_pings**: `TraceId, EventTS, StoreID, BeaconId, CustomerBLEId, RSSI, Zone`  
- **marketing**: `TraceId, EventTS, Channel, CampaignId, CreativeId, CustomerAdId, ImpressionId, Cost, Device`

## Installation & Setup

### Prerequisites
- **Python 3.11**
- **Miniconda or Miniforge** (required)

### Installation Steps
1. **Create conda environment:**
```bash
conda create -n azure-retail python=3.11
conda activate azure-retail
```

2. **Install the package:**
```bash
pip install -e .
```

3. **Configure Azure Event Hub** (optional, for streaming):
   - Update `azure_connection_string` in `config.json`
   - Or set via environment variable: `AZURE_EVENTHUB_CONNECTION_STRING`

## Usage

### 🚀 Start the Web Interface
```bash
# Option 1: Direct module execution
python -m retail_datagen.main

# Option 2: Using uvicorn directly  
python -m uvicorn src.retail_datagen.main:app --host 0.0.0.0 --port 8000 --reload
```

**Access the application:**
- **Web UI**: http://localhost:8000 
- **API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

### 🎯 Programmatic Usage

#### Generate Dimension Data
```python
from retail_datagen.config.models import RetailConfig
from retail_datagen.generators.master_generator import MasterDataGenerator

config = RetailConfig.from_file("config.json")
generator = MasterDataGenerator(config)
generator.generate_all_master_data()
```

#### Generate Fact Data (Intelligent Date Ranges)
```python
from retail_datagen.generators.fact_generator import FactDataGenerator

# First run: uses config start_date → current datetime
generator = FactDataGenerator(config)
generator.generate_historical_data()

# Subsequent runs: automatically uses last_generated → current datetime
generator.generate_historical_data()  # Picks up where it left off
```

#### Stream Real-Time Events
```python
import asyncio
from retail_datagen.streaming.event_streamer import EventStreamer

async def main():
    # Requires fact data to exist first
    streamer = EventStreamer(config)
    await streamer.start()

asyncio.run(main())
```

### 📡 Real-Time Streaming

Stream synthetic retail events to Azure Event Hub or Microsoft Fabric Real-Time Intelligence.

#### Quick Start

1. **Set connection string:**
   ```bash
   export AZURE_EVENTHUB_CONNECTION_STRING="Endpoint=sb://your-namespace.servicebus.windows.net/;..."
   ```

2. **Start streaming:**
   ```bash
   curl -X POST http://localhost:8000/api/stream/start \
     -H "Content-Type: application/json" \
     -d '{"emit_interval_ms": 1000, "burst": 50}'
   ```

3. **Monitor status:**
   ```bash
   curl http://localhost:8000/api/stream/status
   ```

4. **Stop streaming:**
   ```bash
   curl -X POST http://localhost:8000/api/stream/stop
   ```

#### Event Types

Generates 15 types of realistic retail events:

**Transaction Events:**
- `receipt_created` - New purchase transaction
- `receipt_line_added` - Line items on receipt
- `payment_processed` - Payment authorization

**Inventory Events:**
- `inventory_updated` - Stock level changes
- `stockout_detected` - Out of stock alerts
- `reorder_triggered` - Automatic reorder points

**Customer Events:**
- `customer_entered` - Store entry via sensors
- `customer_zone_changed` - Movement between zones
- `ble_ping_detected` - Bluetooth beacon detection

**Operational Events:**
- `truck_arrived` - Delivery truck arrival
- `truck_departed` - Delivery completion
- `store_opened` / `store_closed` - Store operations

**Marketing Events:**
- `ad_impression` - Digital ad displayed
- `promotion_applied` - Discount/promo used

#### Comprehensive Documentation

- **[Setup Guide](docs/STREAMING_SETUP.md)** - Complete configuration and prerequisites
- **[API Reference](docs/STREAMING_API.md)** - All endpoints and event schemas
- **[Operations Guide](docs/STREAMING_OPERATIONS.md)** - Monitoring, troubleshooting, production deployment
- **[Credential Management](docs/CREDENTIALS.md)** - Security best practices for Azure credentials

#### Features

- **State-Aware Streaming**: Automatically continues from last generated timestamp
- **Circuit Breaker**: Built-in resilience with automatic failure handling
- **Dead Letter Queue**: Failed event tracking and analysis
- **Supply Chain Disruptions**: Simulate real-world operational issues
- **Event Filtering**: Stream specific event types only
- **Configurable Throughput**: Adjust burst size and emit intervals
- **Azure & Fabric Support**: Works with Event Hubs and Microsoft Fabric RTI

#### Clear All Data
```python
from retail_datagen.generators.generation_state import GenerationStateManager

state_manager = GenerationStateManager()
result = state_manager.clear_all_data({
    "master": "data/master", 
    "facts": "data/facts"
})
```

## Configuration

### Main Configuration (`config.json`)
```json
{
  "seed": 42,
  "volume": {
    "stores": 42,
    "dcs": 5,
    "total_geographies": 100,
    "total_customers": 50000,
    "total_products": 10000,
    "customers_per_day": 2000,
    "items_per_ticket_mean": 8.2
  },
  "realtime": {
    "emit_interval_ms": 500,
    "burst": 100,
    "azure_connection_string": "",
    "max_batch_size": 256,
    "batch_timeout_ms": 1000,
    "retry_attempts": 3,
    "backoff_multiplier": 2.0,
    "circuit_breaker_enabled": true,
    "monitoring_interval": 30,
    "max_buffer_size": 10000,
    "enable_dead_letter_queue": true
  },
  "paths": {
    "dictionaries": "data/dictionaries",
    "master": "data/master", 
    "facts": "data/facts"
  },
  "stream": {
    "hub": "retail-events"
  },
  "historical": {
    "start_date": "2024-01-01"
  }
}
```

### Key Configuration Parameters

**Volume Controls:**
- `stores`: Number of retail locations to generate
- `total_geographies`: Geographic locations to use (max 1000)
- `total_customers`: Customer base size
- `total_products`: Product catalog size (max 10000)
- `customers_per_day`: Daily transaction volume
- `items_per_ticket_mean`: Average items per transaction

**Streaming Configuration:**
- `emit_interval_ms`: Time between event bursts
- `burst`: Number of events per burst
- `circuit_breaker_enabled`: Automatic failure handling
- `max_buffer_size`: Event buffer before backpressure

**Date Management:**
- `historical.start_date`: Initial historical data start (ISO format)
- System automatically tracks last generated timestamp for incremental generation

### 🔐 Secure Credential Management

The system supports **three methods** for managing Azure Event Hub connection strings:

**1. Environment Variables (Recommended for Local Development):**
```bash
# Copy the example file
cp .env.example .env

# Edit .env and add your connection string
AZURE_EVENTHUB_CONNECTION_STRING="Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=..."

# Start the application (automatically loads .env)
./launch.sh
```

**2. Azure Key Vault (Recommended for Production):**
```bash
# Install Key Vault dependencies
pip install azure-keyvault-secrets azure-identity

# Configure in config.json
{
  "realtime": {
    "use_keyvault": true,
    "keyvault_url": "https://your-vault.vault.azure.net/",
    "keyvault_secret_name": "eventhub-connection-string"
  }
}
```

**3. Configuration File (Not Recommended):**
Store connection string directly in `config.json` (only for testing with temporary credentials)

**Connection String Priority:** Azure Key Vault → Environment Variable → Configuration File

**Security Features:**
- ✅ Automatic credential sanitization in logs (keys redacted)
- ✅ Connection string validation API endpoint
- ✅ Support for Azure Event Hub and Microsoft Fabric RTI
- ✅ Fabric RTI format auto-detection
- ✅ `.env` files automatically ignored by git

**See [CREDENTIALS.md](CREDENTIALS.md) for complete credential management documentation.**

## Testing & Validation

### Run Tests
Recommended: use Python 3.11 and disable external pytest plugin autoloading to avoid environment interference.
```bash
# Create and activate Python 3.11 virtualenv (example)
python3.11 -m venv .venv
source .venv/bin/activate

# Install deps
pip install -r requirements.txt -r requirements-dev.txt

# Run all tests (disable external plugins to avoid autoload issues)
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q

# Or use the bundled runner (auto-skips on <3.11, disables plugin autoload)
python tests/test_runner.py --all

# Run specific suites
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/unit/          # Unit tests
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/integration/   # Integration tests
```

### Validate Data Generation
```bash
# Test master data generation
python test_master_generation.py

# Test fact data generation  
python verify_fact_generation.py

# Test streaming functionality
python test_streaming_implementation.py
```

### Examples and Demo Scripts

The `examples/` directory contains runnable examples and demo scripts to explore different parts of the system.

#### Streaming Examples
- `examples/example_streaming_usage.py` - Minimal example showing how to initialize `EventStreamer`, produce events, and report statistics

#### Dictionary and Generation Demos
- Use `retail_datagen.shared.dictionary_loader.DictionaryLoader` for validated dictionary ingestion with caching
- `examples/demo_fact_generation.py` - Shows fact generation usage
- `examples/verify_fact_generation.py` - Verifies fact generation outputs

#### Ad-hoc Runners and Tests
- `examples/run_basic_test.py` - Basic end-to-end generation runner
- `examples/run_streaming_test.py` - Simple streaming runner
- `examples/quick_test.py` - Quick checks for local sanity
- `examples/simple_test.py` - Minimal run smoke script
- `examples/validate_implementation.py` - Validation helper for implementation checks

**Usage tip:** Run examples from an activated virtual environment with the repo's dependencies installed. See "Testing & Validation" section above for environment setup.

### Data Validation Features
- **Foreign Key Integrity**: Automatic validation across all table relationships
- **Real-Time Data Consistency**: Enforces foot traffic ≥ BLE customers ≥ receipt customers ⭐ **NEW**
- **Marketing Attribution Validation**: Links ad impressions to actual store visits and purchases ⭐ **NEW**
- **Customer Session Tracking**: Validates customer journey from entry to exit ⭐ **NEW**
- **Pricing Constraints**: Enforced Cost < SalePrice ≤ MSRP validation
- **Synthetic Data Safety**: Validation that no real personal data is generated
- **Reproducibility**: Seeded random generation for consistent test results

### Developer Tooling
- Install dev deps: `pip install -r requirements-dev.txt`
- Lint/format: `ruff check .`
- Type-check: `mypy src`

## Advanced Features

### State Management
The system tracks generation state in `data/generation_state.json`:
```json
{
  "last_generated_timestamp": "2024-01-15T10:30:00",
  "historical_start_date": "2024-01-01T00:00:00",
  "has_fact_data": true,
  "last_historical_run": "2024-01-15T10:30:15",
  "last_realtime_run": "2024-01-15T11:45:22"
}
```

### Retail Pattern Simulation ⭐ **ENHANCED**
- **Seasonal Trends**: Holiday shopping spikes, back-to-school patterns
- **Daypart Effects**: Morning coffee, lunch rushes, evening shopping
- **Marketing-Driven Behavior**: Ad impressions drive store visits with channel-specific conversion rates
- **Customer Journey Analytics**: Entry → Zone movement → Purchase → Exit with session tracking
- **Supply Chain Logistics**: DC → Truck → Store flows with refrigerated transport simulation
- **Geographic Weighting**: Population-based store placement and customer distribution
- **Inventory Flows**: Realistic multi-tier supply chain with truck logistics

### Event Stream Format
Real-time events use consistent envelope format (see AGENTS.md for full spec):
```json
{
  "event_type": "receipt_created", 
  "payload": { /* event-specific data */ },
  "trace_id": "uuid-v4",
  "ingest_timestamp": "2024-01-15T10:30:00.123Z",
  "schema_version": "1.0",
  "source": "retail-datagen"
}
```

## Technology Stack

- **Python 3.11**: Core runtime
- **FastAPI**: Web framework and API server
- **Pydantic**: Data validation and configuration management
- **Pandas/NumPy**: Data manipulation and export
- **Azure Event Hubs**: Real-time event streaming
- **Uvicorn**: ASGI server for production deployment

## Output Structure

```
data/
├── dictionaries/                 # Input CSV files (dictionaries)
├── export/                       # Parquet exports
│   ├── dim_stores/dim_stores.parquet
│   ├── dim_customers/dim_customers.parquet
│   ├── fact_receipts/fact_receipts_2024-01.parquet
│   ├── fact_receipt_lines/fact_receipt_lines_2024-01.parquet
│   ├── fact_store_inventory_txn/fact_store_inventory_txn_2024-01.parquet
│   └── ...
└── retail.duckdb                 # DuckDB with dimension/fact tables
```

## Important Notes

### Data Safety
- **100% Synthetic**: No real personal information is ever used or generated
- **Fictitious Only**: All names, addresses, phone numbers are algorithmically generated
- **Safe for Development**: Designed for POCs, demos, and development environments

### Performance Considerations
- **Memory Usage**: Large customer bases (>100k) require sufficient RAM
- **Disk Space**: Fact generation can produce significant Parquet output
- **Generation Time**: 10,000 products + 50,000 customers takes ~2-5 minutes

### Prerequisites for Streaming
- Real-time streaming requires fact data to be generated first
- Azure Event Hub connection string must be configured (see [Secure Credential Management](#-secure-credential-management))
- Supports Azure Event Hub and Microsoft Fabric Real-Time Intelligence
- Streaming picks up from the last generated timestamp automatically
- Connection string validation available at `/api/stream/validate-connection`

## License

This project generates synthetic data only for development and analytics purposes. No real personal information is used, generated, or exposed.
