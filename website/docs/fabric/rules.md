# Real-Time Rules

Real-time rules/alerts driving actions for operations and customer experience.

## Important Note

`fabric/rules/definitions.kql` contains **reference KQL queries only**. Wiring them to delivery actions (Teams alerts, email, SMS) requires **manual configuration** of a Fabric Activator item in your workspace.

## Rule Definitions

The following rules are defined in `definitions.kql`:

### 1. Stockout Alert
**Trigger**: Stockout detected within last 5 minutes
**Action**: Immediate alert to inventory team

```kql
stockout_detected
| where ingest_timestamp > ago(5m)
| project store_id, dc_id, product_id, last_known_quantity, detection_time, trace_id
```

### 2. Reorder URGENT
**Trigger**: Reorder with priority='URGENT' within 10 minutes
**Action**: Escalate to on-call + SMS

```kql
reorder_triggered
| where priority == 'URGENT' and ingest_timestamp > ago(10m)
| project store_id, dc_id, product_id, reorder_quantity, reorder_point, current_quantity, trace_id
```

### 3. Truck Dwell Breach
**Trigger**: Truck arrived more than 90 minutes ago with no matching departure
**Action**: Alert logistics channel

```kql
let thresholdMinutes = 90m;
truck_arrived
| join kind=leftanti (truck_departed) on truck_id, dc_id, store_id, shipment_id
| where datetime_diff('minute', now(), arrival_time) > thresholdMinutes
| project truck_id, dc_id, store_id, shipment_id, arrival_time
```

### 4. Zone Overcrowding
**Trigger**: Customers > 50 OR average dwell > 600 seconds in 5-minute window
**Action**: Queue management alert

```kql
customer_entered
| where ingest_timestamp > ago(5m)
| summarize customers=sum(customer_count), avg_dwell=avg(dwell_time) by store_id, zone
| where customers > 50 or avg_dwell > 600
```

### 5. High-Value Receipt
**Trigger**: Receipt total > $200 within 5 minutes
**Action**: Concierge/priority service notification

```kql
receipt_created
| where ingest_timestamp > ago(5m)
| where total > 200.0
| project store_id, receipt_id, total, trace_id
```

## Manual Integration Steps

To enable these rules in Fabric:

1. Navigate to your Fabric workspace
2. Create a new **Activator** item (formerly Reflex / Data Activator)
3. Connect to your Eventhouse KQL database
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
