import json
import math
import re
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

if not re.fullmatch(r"[0-9a-fA-F-]{36}", CASE_ID):
    raise ValueError("CASE_ID must identify one of the monitored replenishment cases.")
case_id = str(uuid.UUID(CASE_ID))
if not LAKEHOUSE_NAME.isidentifier():
    raise ValueError("LAKEHOUSE_NAME must be a valid Lakehouse identifier.")

kusto_format = "com.microsoft.kusto.spark.synapse.datasource"
token = notebookutils.credentials.getToken(KUSTO_URI)
source = (
    spark.read.format(kusto_format)
    .option("kustoCluster", KUSTO_URI)
    .option("kustoDatabase", KQL_DATABASE)
    .option("accessToken", token)
    .option(
        "kustoQuery",
        "ops_availability_signals "
        f"| where case_id == {json.dumps(case_id)} "
        "| top 1 by event_ts desc",
    )
    .load()
)
rows = source.collect()
if len(rows) != 1:
    raise RuntimeError("The selected case has no unique signal; refresh the scenario first.")
row = rows[0]
if not 1 <= row.store_id <= 5 or not 1 <= row.product_id <= 20:
    raise RuntimeError("The selected case is outside the fixed Plan cohort.")
if (
    row.source_as_of is None or row.shortage_units is None or row.shortage_units < 0
    or row.forecast_units_7d is None or not math.isfinite(row.forecast_units_7d)
    or row.forecast_units_7d < 0
):
    raise RuntimeError("The case lacks valid source time or shortage evidence.")
source_time = row.source_as_of
if source_time.tzinfo is None:
    source_time = source_time.replace(tzinfo=timezone.utc)
if not 0 <= (datetime.now(timezone.utc) - source_time).total_seconds() <= 7 * 86400:
    raise RuntimeError("The case source is stale or future-dated; refresh the forecasts first.")
forecast_time = row.forecast_as_of
if forecast_time is None:
    raise RuntimeError("The case lacks forecast source time.")
if forecast_time.tzinfo is None:
    forecast_time = forecast_time.replace(tzinfo=timezone.utc)
if not 0 <= (datetime.now(timezone.utc) - forecast_time).total_seconds() <= 7 * 86400:
    raise RuntimeError("The forecast source is stale or future-dated; refresh the forecasts first.")

target = f"{LAKEHOUSE_NAME}.ops.replenishment_cases"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LAKEHOUSE_NAME}.ops")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target} (
    case_id STRING, store_id BIGINT, product_id BIGINT,
    source_as_of TIMESTAMP, forecast_as_of TIMESTAMP, forecast_units_7d DOUBLE,
    shortage_units BIGINT, status STRING, prepared_at TIMESTAMP,
    notes STRING
) USING DELTA
""")
draft = (
    spark.createDataFrame(rows, schema=source.schema).select(
        "case_id", "store_id", "product_id", "source_as_of", "forecast_as_of",
        F.col("forecast_units_7d").cast("double"),
        "shortage_units",
    )
    .withColumn("status", F.lit("DRAFT"))
    .withColumn("prepared_at", F.current_timestamp())
    .withColumn("notes", F.lit(
        "Synthetic decision draft only. Verify reservations, confirmed inbound, "
        f"lead times, supply and budget in {PLANNING_REVIEW_TARGET} before approval."
    ))
)
draft.createOrReplaceTempView("_ops_case_draft")
try:
    spark.sql(f"""
    MERGE INTO {target} AS target
    USING _ops_case_draft AS source ON target.case_id = source.case_id
    WHEN NOT MATCHED THEN INSERT *
    """)
finally:
    spark.catalog.dropTempView("_ops_case_draft")

persisted = spark.table(target).where(F.col("case_id") == case_id).select("status").collect()
if len(persisted) != 1 or persisted[0].status not in {"DRAFT", "APPROVED", "RESOLVED"}:
    raise RuntimeError("The persisted case has an invalid or ambiguous lifecycle state.")
if persisted[0].status != "DRAFT":
    raise RuntimeError(
        f"Case is already {persisted[0].status}; no draft action or audit was recorded."
    )

action_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"retail-ops:{case_id}:draft"))
tag = f"retail-ops:{action_id}"
audit = spark.createDataFrame(
    [(datetime.now(timezone.utc), case_id, action_id, "DRAFT",
      "Prepared a planning case; no purchasing or stock movement was executed.")],
    "event_ts timestamp, case_id string, action_id string, status string, details string",
)
(
    audit.write.format(kusto_format)
    .option("kustoCluster", KUSTO_URI)
    .option("kustoDatabase", KQL_DATABASE)
    .option("kustoTable", "ops_case_actions")
    .option("accessToken", token)
    .option("tableCreateOptions", "FailIfNotExist")
    .option("writeMode", "Transactional")
    .option("requestId", action_id)
    .option("ensureNoDuplicatedBlobs", "true")
    .option("sparkIngestionPropertiesJson", json.dumps({
        "flushImmediately": True, "ingestByTags": [tag], "ingestIfNotExists": [tag],
    }))
    .mode("Append").save()
)
print(f"Planning case {case_id} is DRAFT. No order or transfer has been executed.")
