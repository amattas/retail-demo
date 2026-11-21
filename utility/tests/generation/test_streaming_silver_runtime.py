"""Run notebook 03's live-event transforms (store ops, promotions, touches) with Spark."""

import ast
import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

pytestmark = pytest.mark.spark_no_filesystem

NOTEBOOK = (
    Path(__file__).resolve().parents[3] / "fabric" / "lakehouse" / "03-streaming-to-silver.ipynb"
)
FUNCTIONS = {
    "_event_time_expr",
    "_event_time_columns",
    "_normalized_attribution_journey_id",
    "_legacy_index",
    "_promotion_product_ids",
    "transform_store_operation",
    "transform_promotion_applied",
    "transform_ad_impression",
    "transform_stockout_detected",
}
CONSTANTS = {"EVENT_TIME_FIELDS", "STORE_OPERATION_TYPES"}


def _formatted(frame):
    """Format event_ts in Spark (UTC session), not in the local Python time zone."""
    return frame.withColumn("event_time", F.date_format("event_ts", "yyyy-MM-dd HH:mm:ss"))


@pytest.fixture(scope="module")
def nb03():
    notebook = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    source = "\n".join(
        "".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"
    )
    nodes = [
        node
        for node in ast.parse(source).body
        if (isinstance(node, ast.FunctionDef) and node.name in FUNCTIONS)
        or (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id in CONSTANTS for t in node.targets)
        )
    ]
    namespace = {"F": F}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(NOTEBOOK), "exec"), namespace)
    return namespace


def test_store_operations_use_the_historical_lower_case(spark, nb03):
    rows = spark.sql("""
        SELECT timestamp'2026-03-02 11:00:00' AS operation_time,
               timestamp'2026-03-02 11:00:05' AS ingest_timestamp,
               7L AS store_id, 'TRC-1' AS trace_id
    """)
    opened = _formatted(nb03["transform_store_operation"](rows, "store_opened")).first()
    closed = nb03["transform_store_operation"](rows, "store_closed").first()
    assert (opened.operation_type, closed.operation_type) == ("opened", "closed")
    assert opened.event_time == "2026-03-02 11:00:00"


def test_promotion_product_ids_are_sorted_comma_separated(spark, nb03):
    # The Eventhouse shortcut exposes the KQL dynamic array as JSON text.
    rows = spark.sql("""
        SELECT timestamp'2026-03-02 11:00:00' AS ingest_timestamp, 'SAVE10' AS promo_code,
               3L AS store_id, '[105,12,7]' AS product_ids, 1.25 AS discount_amount,
               125L AS discount_cents, 'PERCENTAGE' AS discount_type, 3L AS product_count,
               9L AS customer_id, 'RCP-1' AS receipt_id, 'TRC-2' AS trace_id,
               CAST(NULL AS STRING) AS correlation_id
    """)
    assert nb03["transform_promotion_applied"](rows).first().product_ids == "7,12,105"
    arrays = rows.withColumn("product_ids", F.array(F.lit(40), F.lit(3)))
    assert nb03["transform_promotion_applied"](arrays).first().product_ids == "3,40"


def test_journey_touches_are_dated_by_impression_time(spark, nb03):
    rows = spark.sql("""
        SELECT timestamp'2026-03-09 15:00:00' AS ingest_timestamp,
               timestamp'2026-03-05 08:30:00' AS impression_time,
               'SOCIAL' AS channel, 'CAMP2026030103' AS campaign_id, 'CREATJRN1-OLDER' AS creative_id,
               'AD00000009' AS customer_ad_id, 'IMPJRN1O' AS impression_id, 0.12 AS cost,
               'MOBILE' AS device_type, 9L AS customer_id, 'JRN1' AS correlation_id, 'TRC-3' AS trace_id
        UNION ALL
        SELECT timestamp'2026-03-09 15:01:00', CAST(NULL AS TIMESTAMP), 'EMAIL', 'CAMP2026030101',
               'CREAT1', 'AD00000010', 'IMP1', 0.01, 'DESKTOP', CAST(NULL AS BIGINT),
               CAST(NULL AS STRING), 'TRC-4'
    """)
    out = {row.trace_id: row for row in _formatted(nb03["transform_ad_impression"](rows)).collect()}
    assert out["TRC-3"].event_time == "2026-03-05 08:30:00"
    assert str(out["TRC-3"].event_date) == "2026-03-05"
    assert out["TRC-4"].event_time == "2026-03-09 15:01:00"


def test_store_stockouts_keep_dcid_null_like_history(spark, nb03):
    rows = spark.sql("""
        SELECT timestamp'2026-03-02 11:00:00' AS detection_time,
               timestamp'2026-03-02 11:00:00' AS ingest_timestamp,
               4L AS store_id, 2L AS dc_id, 501L AS product_id, 3L AS last_known_quantity,
               'TRC-5' AS trace_id
        UNION ALL
        SELECT timestamp'2026-03-02 12:00:00', timestamp'2026-03-02 12:00:00',
               CAST(NULL AS BIGINT), 2L, 502L, 9L, 'TRC-6'
    """)
    out = {row.trace_id: row for row in nb03["transform_stockout_detected"](rows).collect()}
    assert (out["TRC-5"].StoreID, out["TRC-5"].DCID) == (4.0, None)
    assert (out["TRC-6"].StoreID, out["TRC-6"].DCID) == (None, 2.0)
