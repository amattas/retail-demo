# Real-Time Rules

Real-time rules/alerts driving actions for operations and customer experience.

## Important Note

This folder contains **KQL query definitions only**. Full Fabric Rules integration (delivery actions, Teams alerts, SMS, etc.) requires **manual configuration** in your Fabric workspace.

## Rule Definitions

The following rules are defined in `definitions.kql`:

### 1. Stockout Alert
**Trigger**: Stockout detected within last 5 minutes
**Action**: Immediate alert to inventory team

```kql
stockout_detected
| where ingest_timestamp > ago(5m)
| project store_id, product_id, last_known_quantity, ingest_timestamp
```

### 2. Reorder URGENT
**Trigger**: Reorder with priority='URGENT' within 10 minutes
**Action**: Escalate to on-call + SMS

```kql
reorder_triggered
| where ingest_timestamp > ago(10m)
| where priority == "URGENT"
| project store_id, product_id, reorder_quantity, priority
```

### 3. Truck Dwell Breach
**Trigger**: Truck arrived > 90 minutes without departure
**Action**: Alert logistics channel

```kql
truck_arrived
| where ingest_timestamp > ago(90m)
| join kind=leftanti (truck_departed | where ingest_timestamp > ago(90m)) on truck_id
| project truck_id, store_id, arrival_time = ingest_timestamp
```

### 4. Zone Overcrowding
**Trigger**: Customers > 50 OR dwell > 600 seconds in 5-minute window
**Action**: Queue management alert

```kql
customer_entered
| where ingest_timestamp > ago(5m)
| summarize customer_count = sum(customer_count), avg_dwell = avg(dwell_time) by store_id, zone
| where customer_count > 50 or avg_dwell > 600
```

### 5. High-Value Receipt
**Trigger**: Receipt total > $200 within 5 minutes
**Action**: Concierge/priority service notification

```kql
receipt_created
| where ingest_timestamp > ago(5m)
| where total > 200
| project store_id, customer_id, total, receipt_id
```

## Manual Integration Steps

To enable these rules in Fabric:

1. Navigate to your Fabric workspace
2. Create a new **Reflex** item
3. Connect to your KQL database
4. Add each rule query
5. Configure actions:
   - **Teams**: Add incoming webhook connector
   - **Email**: Configure notification recipients
   - **SMS**: Integrate via Logic Apps or third-party service
6. Set evaluation frequency (e.g., every 1 minute)
7. Enable and test

## Rule Categories

| Category | Rules | Priority |
|----------|-------|----------|
| Inventory | Stockout Alert, Reorder URGENT | Critical |
| Logistics | Truck Dwell Breach | High |
| Operations | Zone Overcrowding | Medium |
| Customer Experience | High-Value Receipt | Medium |
