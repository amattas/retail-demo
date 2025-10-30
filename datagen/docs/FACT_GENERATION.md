# Historical Fact Data Generation System

## Overview

The Historical Fact Data Generation System creates realistic retail transaction data for analytics and testing purposes. It generates all fact tables specified in AGENTS.md with proper temporal patterns, business logic coordination, and partitioned output, including unified online orders.

## 🎯 Key Features

### ✅ **Complete Fact Table Generation**
- **DC Inventory Transactions**: Supplier deliveries, adjustments, outbound shipments
- **Truck Movements**: Realistic logistics with status progression and timing
- **Store Inventory Transactions**: Receiving, sales, adjustments with proper sourcing
- **Receipts & Receipt Lines**: Customer transactions with realistic basket composition
- **Foot Traffic**: Sensor data with zone-based movement patterns
- **BLE Pings**: Beacon interactions with realistic RSSI values
- **Marketing**: Multi-channel campaigns with targeting and cost tracking
- **Online Orders**: Unified online order facts with inventory impacts on stores/DCs

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
- **Partitioned Storage**: Date-based partitions (`dt=YYYY-MM-DD`)
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
7. Validate business rules and export to partitioned CSV
```

## 🚀 Quick Start

### 1. Basic Usage

```python
from datetime import datetime, timedelta
from retail_datagen.config.models import RetailConfig
from retail_datagen.generators.fact_generator import FactDataGenerator

# Load configuration
config = RetailConfig.from_file("config.json")

# Initialize generator (loads master data automatically)
generator = FactDataGenerator(config)

# Generate 30 days of historical data
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 1, 30)

summary = generator.generate_historical_data(start_date, end_date)

print(f"Generated {summary.total_records} records in {summary.generation_time_seconds:.1f}s")
```

### 2. Configuration Example

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
    "dict": "data/dictionaries",
    "master": "data/master",
    "facts": "data/facts"
  },
  "stream": {
    "hub": "retail-events"
  }
}
```

### 3. Output Structure

```
data/facts/
├── receipts/
│   ├── dt=2024-01-01/
│   │   └── receipts_20240101.csv
│   ├── dt=2024-01-02/
│   │   └── receipts_20240102.csv
├── receipt_lines/
│   ├── dt=2024-01-01/
│   │   └── receipt_lines_20240101.csv
├── store_inventory_txn/
│   ├── dt=2024-01-01/
│   │   └── store_inventory_txn_20240101.csv
├── truck_moves/
├── dc_inventory_txn/
├── foot_traffic/
├── ble_pings/
├── marketing/
└── online_orders/
```

## 📊 Generated Fact Tables

### 1. DC Inventory Transactions
```csv
TraceId,EventTS,DCID,ProductID,QtyDelta,Reason
TRC0000000001,2024-01-01 08:30:45,1,1001,500,INBOUND_SHIPMENT
TRC0000000002,2024-01-01 09:15:22,1,1002,-250,OUTBOUND_SHIPMENT
```

### 2. Truck Movements
```csv
TraceId,EventTS,TruckId,DCID,StoreID,ShipmentId,Status,ETA,ETD
TRC0000000003,2024-01-01 06:00:00,TRK1001,1,15,SHIP20240101,SCHEDULED,2024-01-01 14:00:00,2024-01-01 15:00:00
```

### 3. Store Inventory Transactions
```csv
TraceId,EventTS,StoreID,ProductID,QtyDelta,Reason,Source
TRC0000000004,2024-01-01 14:30:15,15,1001,100,INBOUND_SHIPMENT,TRK1001
TRC0000000005,2024-01-01 15:45:30,15,1001,-2,SALE,CUSTOMER_PURCHASE
```

### 4. Receipts
```csv
TraceId,EventTS,StoreID,CustomerID,ReceiptId,Subtotal,Tax,Total,TenderType
TRC0000000006,2024-01-01 15:45:30,15,5001,RCP202401011545015,25.98,2.08,28.06,CREDIT_CARD
```

### 5. Receipt Lines
```csv
TraceId,EventTS,ReceiptId,Line,ProductID,Qty,UnitPrice,ExtPrice,PromoCode
TRC0000000006,2024-01-01 15:45:30,RCP202401011545015,1,1001,2,12.99,25.98,
```

### 6. Foot Traffic
```csv
TraceId,EventTS,StoreID,SensorId,Zone,Dwell,Count
TRC0000000007,2024-01-01 15:42:18,15,SENSOR_015_ENTRANCE,ENTRANCE,45,1
```

### 7. BLE Pings
```csv
TraceId,EventTS,StoreID,BeaconId,CustomerBLEId,RSSI,Zone
TRC0000000008,2024-01-01 15:43:25,15,BEACON_015_GROCERY,BLE500123,-65,GROCERY
```

### 8. Marketing
```csv
TraceId,EventTS,Channel,CampaignId,CreativeId,CustomerAdId,ImpressionId,Cost,Device
TRC0000000009,2024-01-01 10:15:30,FACEBOOK,CAMP20240101,CREAT001FB01,AD500123,IMP20240101234567,0.15,MOBILE
```

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

### Run Verification Script
```bash
python verify_fact_generation.py
```

### Run Demonstration
```bash
python demo_fact_generation.py
```

### Unit Tests
```bash
python -m pytest tests/unit/test_fact_generation.py
```

## 🔍 Troubleshooting

### Common Issues

**1. "Master data not found"**
- Ensure master data CSV files exist in configured path
- Run master data generation first

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

- `/src/retail_datagen/generators/fact_generator.py` - Main generation engine
- `/src/retail_datagen/generators/seasonal_patterns.py` - Temporal modeling
- `/src/retail_datagen/generators/retail_patterns.py` - Behavior simulation
- `/src/retail_datagen/shared/models.py` - Data models and validation
- `/tests/unit/test_fact_generation.py` - Unit tests
- `/demo_fact_generation.py` - Complete demonstration
- `/verify_fact_generation.py` - System verification

The historical fact data generation system provides a comprehensive, realistic, and scalable solution for generating retail transaction data that follows real-world patterns and business rules.
