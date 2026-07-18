"""Promotions fact group, derived purely from receipt lines (no draws).

Semantics (datagen promotions_mixin):

- ``fact_promo_lines``: one row per SALE receipt line that has a non-NULL
  ``promo_code`` and a positive line discount
  (``discount_cents = unit_cents * quantity - ext_cents > 0``). Event fields
  come from the line; ``trace_id = 'TRC-PRM-' + receipt_id_ext + '-' +
  promo_code + '-' + line_num``.
- ``fact_promotions``: one row per (receipt_id_ext, promo_code) aggregating
  those lines — discount sums, distinct ``product_count``, ``product_ids`` as
  a comma-joined sorted id list. ``discount_type`` is 'BOGO' for BOGO codes and
  'PERCENTAGE' for all other generated promo codes.
  Store/customer/event fields are joined from the receipt header;
  ``trace_id = 'TRC-PRM-' + receipt_id_ext + '-' + promo_code``.

TMDL contract note: ``fact_promo_lines`` carries dual columns — the
snake_case plan names plus TMDL-bound PascalCase mirrors (ReceiptId,
PromoCode, LineNumber, ProductID, Qty, DiscountAmount, DiscountCents) and the
legacy pandas-index column ``__index_level_0__`` (hash-derived via
legacy_index(receipt_id_ext, promo_code, line_number)). The PascalCase columns
are exact copies of their snake_case twins.

Pure joins/groupBys over the sales group — deterministic, no randomness.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_setup.generation.receipts import _fmt
from retail_setup.generation.runtime import legacy_index
from retail_setup.generation.schemas import column_names


def generate_promotions(
    spark: SparkSession,
    sales_group: dict[str, DataFrame],
) -> tuple[DataFrame, DataFrame]:
    """Derive (fact_promotions, fact_promo_lines) from the sales group."""

    sale_ids = (
        sales_group["fact_receipts"]
        .filter(F.col("receipt_type") == "SALE")
        .select("receipt_id_ext")
    )

    # --- promo lines: discounted SALE lines with a promo code
    lines = (
        sales_group["fact_receipt_lines"]
        .join(sale_ids, "receipt_id_ext")
        .filter(F.col("promo_code").isNotNull())
        .withColumn(
            "discount_cents",
            F.col("unit_cents") * F.col("quantity") - F.col("ext_cents"),
        )
        .filter(F.col("discount_cents") > 0)
        .withColumn("line_number", F.col("line_num").cast("long"))
        .withColumn("quantity", F.col("quantity").cast("long"))
        .withColumn("discount_amount", _fmt(F.col("discount_cents")))
        .withColumn(
            "trace_id",
            F.concat(
                F.lit("TRC-PRM-"), F.col("receipt_id_ext"), F.lit("-"),
                F.col("promo_code"), F.lit("-"),
                F.col("line_number").cast("string"),
            ),
        )
    )

    promo_lines = (
        lines
        # TMDL-bound PascalCase mirrors of the snake_case columns
        .withColumn("ReceiptId", F.col("receipt_id_ext"))
        .withColumn("PromoCode", F.col("promo_code"))
        .withColumn("LineNumber", F.col("line_number"))
        .withColumn("ProductID", F.col("product_id"))
        .withColumn("Qty", F.col("quantity"))
        .withColumn("DiscountAmount", F.col("discount_amount"))
        .withColumn("DiscountCents", F.col("discount_cents"))
        # Legacy pandas-index column bound by the semantic model
        .withColumn("__index_level_0__",
                    legacy_index("receipt_id_ext", "promo_code", "line_number"))
        .select(*column_names("fact_promo_lines"))
    )

    # --- promotions: aggregate per (receipt_id_ext, promo_code)
    agg = lines.groupBy("receipt_id_ext", "promo_code").agg(
        F.sum("discount_cents").alias("discount_cents"),
        F.countDistinct("product_id").alias("product_count"),
        F.concat_ws(",", F.sort_array(F.collect_set("product_id"))).alias("product_ids"),
    )

    headers = sales_group["fact_receipts"].select(
        "receipt_id_ext", "event_ts", "event_date", "store_id", "customer_id"
    )
    promotions = (
        agg
        .join(headers, "receipt_id_ext")
        .withColumn("discount_amount", _fmt(F.col("discount_cents")))
        # BOGO codes are buy-one-get-one; everything else is a percentage off.
        .withColumn("discount_type",
                    F.when(F.col("promo_code").startswith("BOGO"), F.lit("BOGO"))
                    .otherwise(F.lit("PERCENTAGE")))
        .withColumn(
            "trace_id",
            F.concat(F.lit("TRC-PRM-"), F.col("receipt_id_ext"), F.lit("-"),
                     F.col("promo_code")),
        )
        # IMP-007: NULL unless the underlying receipt was selected for
        # attribution; enriched in attribution.py.
        .withColumn("attribution_journey_id", F.lit(None).cast("string"))
        .select(*column_names("fact_promotions"))
    )

    return promotions, promo_lines
