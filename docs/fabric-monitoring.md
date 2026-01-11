# Microsoft Fabric Monitoring & Alerting Guide

This guide covers monitoring pipeline execution, data quality metrics, and alerting configuration for the Retail Demo medallion architecture.

---

## Table of Contents

1. [Built-in Fabric Monitoring](#built-in-fabric-monitoring)
2. [Pipeline Monitoring](#pipeline-monitoring)
3. [Data Quality Monitoring](#data-quality-monitoring)
4. [Performance Monitoring](#performance-monitoring)
5. [Alerting Configuration](#alerting-configuration)
6. [Azure Monitor Integration](#azure-monitor-integration)
7. [Custom Monitoring Notebook](#custom-monitoring-notebook)

---

## Built-in Fabric Monitoring

### Monitoring Hub

Access the Fabric Monitoring Hub to view all pipeline runs, notebook executions, and item activities:

1. Navigate to Fabric workspace
2. Click **Monitoring** in left navigation
3. View recent activities, failures, and performance metrics

**Key Metrics:**
- Pipeline run status (succeeded/failed/in progress)
- Execution duration
- Data refresh status
- Item access patterns

### Pipeline Run History

View detailed execution history for each pipeline:

1. Navigate to pipeline item
2. Click **Monitor** tab
3. View:
   - Run start/end times
   - Activity status
   - Error messages
   - Output logs

**Access Logs:**
```
Workspace → Pipeline → Monitor → Select run → View details → Logs
```

---

## Pipeline Monitoring

### Bronze → Silver Pipeline

**Key Metrics to Track:**
- Execution frequency: Every 5 minutes
- Expected duration: 5-10 minutes
- Success rate: Target >99%
- Row counts: Silver tables should match Bronze sources

**Monitor via Notebook Output:**
```python
# In 02-onelake-to-silver.ipynb
# Check final metrics cell for:
print(f"Silver tables created: {len(silver_tables)}")
print(f"Schema mismatches: {schema_mismatch_count}")
print(f"Total rows processed: {total_row_count}")
```

**Warning Signs:**
- ⚠️ Schema mismatch count > 0 → Investigate streaming source schema changes
- ⚠️ Execution time > 15 minutes → Check data volume or optimize queries
- ⚠️ Row count drops > 20% → Potential data source issue

### Silver → Gold Pipeline

**Key Metrics to Track:**
- Execution frequency: Every 15 minutes
- Expected duration: 3-5 minutes
- Success rate: Target >99%
- Gold table freshness: Max event_ts should be within 20 minutes

**Monitor via Notebook Output:**
```python
# In 03-silver-to-gold.ipynb
# Check summary cell for:
print(f"Gold tables created: {gold_success}")
print(f"Failed tables: {gold_failed}")
```

**Warning Signs:**
- ⚠️ Failed table count > 0 → Check Silver table availability
- ⚠️ Execution time > 10 minutes → Optimize aggregation queries
- ⚠️ Data freshness lag > 30 minutes → Upstream pipeline delays

---

## Data Quality Monitoring

### Automated Data Quality Checks

Create a monitoring notebook that runs after Silver layer refresh to validate data quality.

**Example: Data Quality Notebook**
```python
# data-quality-monitor.ipynb
from pyspark.sql import functions as F
from datetime import datetime, timedelta

SILVER_DB = "ag"

def check_data_freshness(table_name, max_age_minutes=30):
    """Check if table has recent data."""
    from datetime import timezone
    
    df = spark.table(f"{SILVER_DB}.{table_name}")
    max_ts = df.agg(F.max("event_ts")).collect()[0][0]
    
    if max_ts is None:
        return False, "No data in table"
    
    # Use timezone-aware datetime for consistent comparisons
    now = datetime.now(timezone.utc)
    if max_ts.tzinfo is None:
        # Assume UTC if timestamp is naive
        max_ts = max_ts.replace(tzinfo=timezone.utc)
    
    age_minutes = (now - max_ts).total_seconds() / 60
    
    if age_minutes > max_age_minutes:
        return False, f"Data is {age_minutes:.1f} minutes old (max: {max_age_minutes})"
    
    return True, f"Fresh (age: {age_minutes:.1f} minutes)"

def check_row_count(table_name, min_expected=1):
    """Check if table has minimum expected rows."""
    df = spark.table(f"{SILVER_DB}.{table_name}")
    count = df.count()
    
    if count < min_expected:
        return False, f"Only {count} rows (expected: {min_expected})"
    
    return True, f"{count:,} rows"

def check_null_keys(table_name, key_columns):
    """Check for null values in key columns."""
    df = spark.table(f"{SILVER_DB}.{table_name}")
    
    for col in key_columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            return False, f"{null_count} nulls in {col}"
    
    return True, "No null keys"

# Run checks
checks = [
    ("fact_receipts", "freshness", lambda: check_data_freshness("fact_receipts", 30)),
    ("fact_receipts", "row_count", lambda: check_row_count("fact_receipts", 100)),
    ("fact_receipts", "null_keys", lambda: check_null_keys("fact_receipts", ["receipt_id_ext", "store_id"])),
    ("fact_receipt_lines", "freshness", lambda: check_data_freshness("fact_receipt_lines", 30)),
    ("fact_receipt_lines", "row_count", lambda: check_row_count("fact_receipt_lines", 500)),
]

failures = []
for table, check_type, check_fn in checks:
    passed, message = check_fn()
    status = "✓" if passed else "✗"
    print(f"{status} {table} - {check_type}: {message}")
    
    if not passed:
        failures.append(f"{table} - {check_type}: {message}")

# Alert if failures
if failures:
    print(f"\n⚠️  {len(failures)} data quality issues detected!")
    for failure in failures:
        print(f"  - {failure}")
    
    # Send notification (integrate with Teams/email)
    # send_alert(failures)
else:
    print(f"\n✓ All data quality checks passed")
```

### Metrics Dashboard

Create a Gold layer table for monitoring metrics:

```python
# In 03-silver-to-gold.ipynb - add this aggregation
def create_monitoring_metrics():
    """Create monitoring metrics table."""
    df_receipts = read_silver("fact_receipts")
    
    return (
        df_receipts
        .withColumn("hour", F.date_trunc("hour", F.col("event_ts")))
        .groupBy("hour", "store_id")
        .agg(
            F.count("*").alias("receipt_count"),
            F.sum("total").alias("total_sales"),
            F.max("event_ts").alias("max_event_ts")
        )
        .withColumn("computed_at", F.current_timestamp())
        .orderBy(F.desc("hour"))
    )

process_gold_table("monitoring_metrics_hourly", create_monitoring_metrics)
```

Use this table in Power BI to create a monitoring dashboard showing:
- Receipts per hour trend
- Sales volume trend
- Data freshness by store
- Pipeline execution status

---

## Performance Monitoring

### Execution Time Tracking

Track pipeline execution times to identify performance degradation:

**Create Performance Log Table:**
```python
# At end of each notebook
execution_time = (datetime.now() - start_time).total_seconds()

log_df = spark.createDataFrame([{
    "notebook_name": "02-onelake-to-silver",
    "execution_time_seconds": execution_time,
    "rows_processed": total_row_count,
    "success": True,
    "timestamp": datetime.now()
}])

log_df.write.format("delta").mode("append").saveAsTable("monitoring.pipeline_execution_log")
```

**Query Performance Trends:**
```sql
SELECT 
    DATE_TRUNC('day', timestamp) as day,
    notebook_name,
    AVG(execution_time_seconds) as avg_time,
    MAX(execution_time_seconds) as max_time,
    COUNT(*) as run_count
FROM monitoring.pipeline_execution_log
WHERE timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY day, notebook_name
ORDER BY day DESC
```

### Query Performance Analysis

Use Fabric's built-in query insights:

1. Navigate to Lakehouse → **SQL analytics endpoint**
2. Click **Monitoring** → **Query insights**
3. View:
   - Long-running queries
   - Resource consumption
   - Execution plans

**Optimize Slow Queries:**
- Add Z-ordering on filtered columns
- Partition large fact tables by date
- Use Delta table statistics
- Review execution plans for full table scans

---

## Alerting Configuration

### 1. Fabric Built-in Alerts

Configure alerts in Fabric Monitoring Hub:

**Pipeline Failure Alerts:**
1. Navigate to pipeline → **Settings**
2. Enable **Alerts**
3. Configure:
   - Alert on: Failure
   - Frequency: Immediate
   - Recipients: Email addresses or Teams channel
4. Save

**Data Refresh Alerts:**
1. Navigate to semantic model → **Settings**
2. Enable **Refresh failure notifications**
3. Add recipients
4. Save

### 2. Microsoft Teams Integration

Send alerts to Teams channel via webhook:

**Setup Teams Webhook:**
1. Open Teams channel
2. Click **•••** → **Connectors** → **Incoming Webhook**
3. Configure webhook and copy URL
4. Store URL in notebook environment variable

**Send Alert from Notebook:**
```python
import requests
import json

def send_teams_alert(title, message, color="ff0000"):
    """Send alert to Teams channel."""
    webhook_url = os.environ.get("TEAMS_WEBHOOK_URL")
    
    if not webhook_url:
        print("⚠️  Teams webhook not configured")
        return
    
    payload = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "summary": title,
        "themeColor": color,
        "title": title,
        "text": message
    }
    
    response = requests.post(webhook_url, json=payload)
    
    if response.status_code == 200:
        print("✓ Teams alert sent")
    else:
        print(f"✗ Failed to send Teams alert: {response.status_code}")

# Use in notebook
if schema_mismatch_count > 0:
    send_teams_alert(
        title="⚠️ Silver Layer Schema Mismatch Detected",
        message=f"Schema mismatches: {schema_mismatch_count}\\nCheck Bronze source schemas for changes.",
        color="ffa500"  # Orange
    )
```

### 3. Email Alerts

Send email alerts using Azure Logic Apps or SendGrid:

**Azure Logic App Example:**
1. Create Logic App in Azure Portal
2. Add trigger: **HTTP Request**
3. Add action: **Send an email (V2)**
4. Configure:
   - To: monitoring@company.com
   - Subject: From HTTP body
   - Body: From HTTP body
5. Save and copy HTTP POST URL

**Call from Notebook:**
```python
import requests

def send_email_alert(subject, body):
    """Send email via Logic App."""
    logic_app_url = os.environ.get("LOGIC_APP_EMAIL_URL")
    
    if not logic_app_url:
        print("⚠️  Email Logic App not configured")
        return
    
    payload = {
        "subject": subject,
        "body": body
    }
    
    response = requests.post(logic_app_url, json=payload)
    
    if response.status_code == 200:
        print("✓ Email alert sent")
    else:
        print(f"✗ Failed to send email: {response.status_code}")
```

### 4. Azure Monitor Integration

Forward Fabric metrics to Azure Monitor for centralized monitoring:

**Enable Diagnostic Logs:**
1. Navigate to Fabric workspace settings
2. Enable **Diagnostic settings**
3. Send to:
   - Log Analytics workspace
   - Storage account (archival)
   - Event Hub (streaming)

**Create Azure Monitor Alerts:**
1. Navigate to Azure Monitor → **Alerts**
2. Create alert rule:
   - Resource: Fabric workspace
   - Condition: Pipeline run failed
   - Action group: Email/SMS/Teams
3. Save

**Query Logs in Log Analytics:**
```kql
FabricActivityLogs
| where OperationName == "PipelineRunFailed"
| where TimeGenerated > ago(24h)
| summarize FailureCount = count() by PipelineName, bin(TimeGenerated, 1h)
| order by TimeGenerated desc
```

---

## Custom Monitoring Notebook

Create a comprehensive monitoring notebook that runs on a schedule:

```python
# monitoring-dashboard.ipynb
from pyspark.sql import functions as F
from datetime import datetime, timedelta

print("="*80)
print("FABRIC MONITORING DASHBOARD")
print("="*80)
print(f"Generated: {datetime.now().isoformat()}")
print()

# 1. Data Freshness
print("1. DATA FRESHNESS")
print("-"*80)
fact_tables = ["fact_receipts", "fact_receipt_lines", "fact_payments"]
for table in fact_tables:
    try:
        df = spark.table(f"ag.{table}")
        max_ts = df.agg(F.max("event_ts")).collect()[0][0]
        age_min = (datetime.now() - max_ts).total_seconds() / 60
        status = "✓" if age_min < 30 else "⚠️"
        print(f"  {status} {table:30s} {age_min:6.1f} min ago")
    except:
        print(f"  ✗ {table:30s} ERROR")
print()

# 2. Row Counts
print("2. ROW COUNTS (Last 24 Hours)")
print("-"*80)
cutoff = datetime.now() - timedelta(days=1)
for table in fact_tables:
    try:
        df = spark.table(f"ag.{table}")
        count = df.filter(F.col("event_ts") > cutoff).count()
        print(f"  {table:30s} {count:>10,} rows")
    except:
        print(f"  {table:30s} ERROR")
print()

# 3. Pipeline Status
print("3. PIPELINE EXECUTION STATUS")
print("-"*80)
try:
    log_df = spark.table("monitoring.pipeline_execution_log")
    recent = log_df.filter(F.col("timestamp") > cutoff)
    
    summary = recent.groupBy("notebook_name").agg(
        F.count("*").alias("runs"),
        F.sum(F.when(F.col("success"), 1).otherwise(0)).alias("successes"),
        F.avg("execution_time_seconds").alias("avg_time")
    ).collect()
    
    for row in summary:
        success_rate = (row.successes / row.runs) * 100
        status = "✓" if success_rate >= 95 else "⚠️"
        print(f"  {status} {row.notebook_name:30s} {row.runs} runs, {success_rate:.1f}% success, {row.avg_time:.1f}s avg")
except:
    print("  ⚠️  No pipeline execution log found")
print()

# 4. Storage Metrics
print("4. STORAGE METRICS")
print("-"*80)
for schema in ["ag", "au"]:
    try:
        tables = spark.sql(f"SHOW TABLES IN {schema}").collect()
        total_tables = len(tables)
        print(f"  {schema:4s} schema: {total_tables} tables")
    except:
        print(f"  {schema:4s} schema: ERROR")
print()

print("="*80)
print("Dashboard complete")
```

**Schedule this notebook to run hourly** and publish results to a monitoring dashboard.

---

## Alert Priority Matrix

| Metric | Warning Threshold | Critical Threshold | Action |
|--------|------------------|-------------------|--------|
| **Pipeline failure rate** | >1% | >5% | Investigate logs, check dependencies |
| **Data freshness lag** | >30 min | >60 min | Check upstream sources, pipeline execution |
| **Row count drop** | >20% vs avg | >50% vs avg | Verify data source connectivity |
| **Execution time increase** | >2x baseline | >5x baseline | Optimize queries, check capacity |
| **Schema mismatch** | Any | N/A | Update transformation mappings |
| **Null key values** | >0 | N/A | Fix data quality at source |
| **Storage growth** | >20% per week | >50% per week | Run VACUUM, optimize Delta tables |

---

## Best Practices

1. **Monitor Continuously**: Set up automated monitoring that runs every hour
2. **Alert Proactively**: Configure alerts before issues become critical
3. **Trending Analysis**: Track metrics over time to identify degradation patterns
4. **Runbook Documentation**: Document response procedures for each alert type
5. **Regular Reviews**: Weekly review of monitoring metrics and alert trends
6. **Capacity Planning**: Monitor resource usage to anticipate scaling needs
7. **Incident Postmortems**: Document failures and implement preventive measures

---

## Related Documentation

- [Fabric Monitoring Hub](https://learn.microsoft.com/fabric/admin/monitoring-hub)
- [Azure Monitor Integration](https://learn.microsoft.com/azure/azure-monitor/overview)
- [Pipeline Monitoring](https://learn.microsoft.com/fabric/data-factory/monitor-pipeline-runs)
- [Microsoft Teams Webhooks](https://learn.microsoft.com/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook)
