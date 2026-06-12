# Fact Data Generation System

## Overview

The Historical Fact Data Generation System creates realistic retail transaction data for analytics and testing purposes. It generates 18 fact tables with proper temporal patterns, business logic coordination, and partitioned output, including unified online orders.

## 🎯 Key Features

### ✅ **Complete Fact Table Generation (18 tables)**

| Logical name | DuckDB table | Description |
|--------------|--------------|-------------|
| `dc_inventory_txn` | `fact_dc_inventory_txn` | Supplier deliveries, adjustments, outbound shipments |
| `truck_moves` | `fact_truck_moves` | Logistics with status progression and timing |
| `truck_inventory` | `fact_truck_inventory` | Per-shipment truck inventory contents |
| `store_inventory_txn` | `fact_store_inventory_txn` | Receiving, sales, returns, adjustments |
| `receipts` | `fact_receipts` | Customer transactions with realistic baskets |
| `receipt_lines` | `fact_receipt_lines` | Line items with pricing and promo codes |
| `foot_traffic` | `fact_foot_traffic` | Sensor data with zone-based movement |
| `ble_pings` | `fact_ble_pings` | Beacon interactions with realistic RSSI values |
| `customer_zone_changes` | `fact_customer_zone_changes` | In-store zone-to-zone movement |
| `marketing` | `fact_marketing` | Multi-channel impressions with cost tracking |
| `online_orders` | `fact_online_order_headers` | Online order headers (ship-from-store/DC, BOPIS) |
| `online_order_lines` | `fact_online_order_lines` | Online order line items |
| `fact_payments` | `fact_payments` | In-store and online payments (approved/declined) |
| `store_ops` | `fact_store_ops` | Store open/close operational events |
| `stockouts` | `fact_stockouts` | Out-of-stock detections at stores and DCs |
| `promotions` | `fact_promotions` | Applied promotions per receipt |
| `promo_lines` | `fact_promo_lines` | Per-product promotion details |
| `reorders` | `fact_reorders` | Reorder triggers from inventory thresholds |

All column names use `snake_case` (e.g., `event_ts`, `receipt_id_ext`, `store_id`).

### ✅ **Realistic Temporal Patterns**
- **Seasonal Effects**: Holiday spikes, back-to-school, weather impacts
- **Daily Patterns**: Peak hours, lunch rushes, weekend vs weekday differences
- **Store Hours**: Realistic operating schedules with closed-time handling
- **Event-Driven**: Promotional periods, flash sales, special events

### ✅ **Advanced Retail Behavior Simulation**
- **Customer Segments**: Budget-conscious, convenience-focused, quality-seekers, brand-loyal
- **Shopping Behaviors**: Quick trips, grocery runs, family shopping, bulk purchases
- **Realistic Baskets**: Category-based product combinations with segment preferences
- **Geographic Logic**: Customer-store proximity affects shopping patterns

### ✅ **Supply Chain Coordination**
- **Inventory Flow**: DC → Truck → Store → Customer with realistic timing
- **Demand-Driven Reordering**: Automatic shipment triggers based on sales
- **Capacity Constraints**: Truck limits, store capacity, DC throughput
- **Business Rules**: No negative inventory, proper pricing validation

### ✅ **Data Quality & Validation**
- **Referential Integrity**: All foreign keys validated across tables
- **Business Rule Compliance**: Receipt totals, pricing constraints, timing logic
- **Synthetic Data Safety**: No real personal information generated
- **Reproducible Results**: Seed-based deterministic generation

### ✅ **Production-Ready Output**
- **DuckDB Storage**: Generation writes directly to `data/retail.duckdb`
- **Partitioned Export**: Monthly Parquet files under `data/export/<table>` (via the export API)
- **Scalable Generation**: Handle millions of transactions efficiently
- **Progress Reporting**: Real-time feedback with table completion counter, ETA estimation, and throttled updates
- **Configurable Volumes**: Adjust store count, customer traffic, basket sizes

## 🏗️ System Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────────────┐
│                    FactDataGenerator                            │
│  ┌─────────────────┬──────────────────┬──────────────────────┐  │
│  │ SeasonalPatterns│ CustomerJourney  │  InventoryFlow       │  │
│  │ TemporalPatterns│ Simulator        │  Simulator           │  │
│  │ EventPatterns   │                  │                      │  │
│  └─────────────────┴──────────────────┴──────────────────────┘  │
│  ┌─────────────────┬──────────────────┬──────────────────────┐  │
│  │ MarketingCampaign│ BusinessRules   │  Cross-Fact          │  │
│  │ Simulator        │ Engine          │  Coordination        │  │
│  └─────────────────┴──────────────────┴──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Daily Generation Loop:
1. Calculate temporal multipliers (seasonal, daily, hourly)
2. Generate DC inventory transactions (supplier deliveries)
3. Generate marketing campaigns and impressions
4. For each hour of day:
   - Generate customer transactions (receipts + lines)
   - Generate foot traffic and BLE pings
   - Process inventory deductions
5. Analyze inventory needs → Generate truck movements
6. Process truck deliveries → Update store inventory
7. Validate business rules and export to monthly Parquet files
```

## 🚀 Quick Start

### 1. Via the API (recommended)

```bash
# Generate dimensions first
curl -X POST http://localhost:8000/api/generate/dimensions \
  -H "Content-Type: application/json" -d '{}'

# Generate facts with intelligent date range logic
curl -X POST http://localhost:8000/api/generate/fact \
  -H "Content-Type: application/json" -d '{}'

# Optional body: {"start_date": "...", "end_date": "...", "tables": ["receipts", "receipt_lines"]}

# Monitor progress
curl http://localhost:8000/api/generate/fact/status

# Generate a single table
curl -X POST http://localhost:8000/api/generate/fact/receipts
```

### 2. Basic Usage (Python)

```python
import asyncio
from datetime import datetime
from retail_datagen.config.models import RetailConfig
from retail_datagen.generators.fact_generator import FactDataGenerator

# Load configuration
config = RetailConfig.from_file("config.json")

# Initialize generator (loads dimension data automatically)
generator = FactDataGenerator(config)

# Generate 30 days of fact data
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 1, 30)

summary = asyncio.run(generator.generate_historical_data(start_date, end_date))

print(f"Generated {summary.total_records} records in {summary.generation_time_seconds:.1f}s")
```

### 3. Configuration Example

```json
{
  "seed": 42,
  "volume": {
    "stores": 250,
    "dcs": 12,
    "customers_per_day": 20000,
    "items_per_ticket_mean": 4.2,
    "online_orders_per_day": 2500
  },
  "realtime": {
    "emit_interval_ms": 500,
    "burst": 100
  },
  "paths": {
    "dictionaries": "data/dictionaries",
    "master": "data/master",
    "facts": "data/facts"
  },
  "stream": {
    "hub": "retail-events"
  }
}
```

### 4. Output Structure

Fact data is written to DuckDB (`data/retail.duckdb`). Exporting via `POST /api/export/facts` produces monthly-partitioned Parquet files:

```
data/export/
├── fact_receipts/
│   ├── fact_receipts_2024-01.parquet
│   ├── fact_receipts_2024-02.parquet
├── fact_receipt_lines/
│   ├── fact_receipt_lines_2024-01.parquet
├── fact_store_inventory_txn/
├── fact_truck_moves/
├── fact_dc_inventory_txn/
├── fact_foot_traffic/
├── fact_ble_pings/
├── fact_marketing/
├── ... (one directory per fact table)
└── fact_online_order_lines/
    └── fact_online_order_lines_2024-01.parquet
```

## 📊 Example Schemas

Columns follow the repo-wide `snake_case` convention. Two representative tables:

### fact_receipt_lines
```
receipt_id_ext VARCHAR, event_ts TIMESTAMP, product_id BIGINT, line_num INTEGER,
quantity INTEGER, unit_price VARCHAR, ext_price VARCHAR, unit_cents BIGINT,
ext_cents BIGINT, promo_code VARCHAR
```

### fact_payments
```
event_ts TIMESTAMP, receipt_id_ext VARCHAR, order_id_ext VARCHAR,
payment_method VARCHAR, amount_cents BIGINT, amount VARCHAR, transaction_id VARCHAR,
processing_time_ms BIGINT, status VARCHAR, decline_reason VARCHAR,
store_id BIGINT, customer_id BIGINT
```

Use `GET /api/facts/{table_name}` or `GET /api/data/{table_name}/summary` to inspect any table's data and schema, or query DuckDB directly.

## 🎯 Realistic Behavior Examples

### Seasonal Patterns
- **Black Friday**: 3.5x normal traffic with extended hours
- **Christmas**: 2x normal traffic with gift-focused purchases
- **Back-to-School**: Electronics and clothing surge in August
- **Summer**: Outdoor and BBQ products increase

### Daily Patterns
- **Monday**: 70% of baseline traffic (slow start)
- **Friday**: 120% of baseline (weekend prep)
- **Saturday**: 140% of baseline (peak shopping)
- **Lunch Rush**: 11:30 AM - 1:30 PM traffic spike
- **After Work**: 5:00 PM - 8:00 PM peak period

### Customer Behavior
- **Budget-Conscious**: Bulk shopping, promotional focus, longer baskets
- **Convenience-Focused**: Quick trips, premium for convenience
- **Quality-Seekers**: Higher-priced items, selective shopping
- **Brand-Loyal**: Consistent brand preferences, moderate baskets

### Supply Chain Logic
1. **Sales reduce store inventory**
2. **Inventory hits reorder point → Triggers truck shipment**
3. **Truck travels realistic time → Delivers to store**
4. **Store receives inventory → Ready for more sales**

## 🔧 Advanced Configuration

### Temporal Pattern Customization
```python
from retail_datagen.generators.seasonal_patterns import CompositeTemporalPatterns

# Override seasonal multipliers
patterns = CompositeTemporalPatterns(seed=42)
custom_multiplier = patterns.get_overall_multiplier(datetime(2024, 12, 25))
```

### Customer Journey Customization
```python
from retail_datagen.generators.retail_patterns import CustomerJourneySimulator

# Generate specific shopping behavior
simulator = CustomerJourneySimulator(customers, products, stores)
basket = simulator.generate_shopping_basket(
    customer_id=123, 
    behavior_type=ShoppingBehaviorType.FAMILY_SHOPPING
)
```

### Business Rules Validation
```python
from retail_datagen.generators.retail_patterns import BusinessRulesEngine

rules = BusinessRulesEngine()
is_valid = rules.validate_receipt_totals(receipt_lines, total_amount)
validation_summary = rules.get_validation_summary()
```

## 🚀 Performance & Scaling

### Recommended Specifications

| Store Count | Daily Customers | Generation Time | Memory Usage | Storage/Day |
|-------------|----------------|-----------------|--------------|-------------|
| 10 stores   | 1,000          | 30 seconds      | 100 MB       | 50 MB       |
| 100 stores  | 10,000         | 5 minutes       | 500 MB       | 500 MB      |
| 1,000 stores| 100,000        | 45 minutes      | 2 GB         | 5 GB        |

### Optimization Tips

1. **Use appropriate date ranges**: Start with small ranges for testing
2. **Monitor memory usage**: Large customer bases require more memory
3. **Configure customer density**: Adjust `customers_per_day` for your needs

## 🧪 Testing & Validation

```bash
cd datagen

# Full test suite
python -m pytest tests/

# Targeted fact-generation tests
python -m pytest tests/unit/test_payments_generation.py \
  tests/unit/test_promotions_generation.py \
  tests/unit/test_stockouts.py tests/unit/test_reorders.py \
  tests/unit/test_store_ops.py tests/unit/test_customer_zone_changes.py

# Integration flow
python -m pytest tests/integration/test_historical_generation_flow.py
```

See also the [Validation Guide](validation.md) for data-quality sanity checks.

## 🔍 Troubleshooting

### Common Issues

**1. "Dimension data not found"**
- Ensure dimension data exists in DuckDB (dim_* tables) or export files under `data/export/<table>`
- Run dimension data generation first

**2. "Negative inventory"**
- Check inventory reorder logic
- Verify business rules are properly applied

**3. "Large memory usage"**
- Reduce `customers_per_day` in config
- Generate smaller date ranges
- Use incremental generation

**4. "Slow generation"**
- Check disk I/O performance  
- Reduce fact table complexity
- Use SSD storage for output

### Debug Mode
```python
generator = FactDataGenerator(config)
generator.business_rules.clear_validation_results()  # Clear previous validations

# Enable detailed logging
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 🎯 Next Steps

1. **Integration**: Incorporate into your data pipeline
2. **Customization**: Modify patterns for your specific retail scenario
3. **Real-time**: Extend to real-time event streaming
4. **Analytics**: Use generated data for BI/ML model training
5. **Scale**: Generate production-volume datasets

## 📚 Related Files

- `src/retail_datagen/generators/fact_generators/` - Generation engine (core + per-domain mixins)
- `src/retail_datagen/generators/fact_generator.py` - Backward-compatibility shim re-exporting `FactDataGenerator`
- `src/retail_datagen/generators/seasonal_patterns.py` - Temporal modeling
- `src/retail_datagen/generators/retail_patterns/` - Behavior simulation (customer journey, business rules, inventory flow, truck operations, marketing campaigns, disruptions)
- `src/retail_datagen/shared/models.py` - Data models and validation
- `tests/unit/` and `tests/integration/` - Test suites

The historical fact data generation system provides a comprehensive, realistic, and scalable solution for generating retail transaction data that follows real-world patterns and business rules.
