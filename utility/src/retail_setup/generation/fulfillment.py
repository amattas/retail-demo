"""Spark adapter for the causal SKU policy and fulfilled receipt finances."""

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from functools import partial
from itertools import groupby
from typing import Iterator

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.inventory_policy import Demand, simulate_sku
from retail_setup.generation.receipts import _fmt
from retail_setup.generation.returns import build_return_headers
from retail_setup.generation.runtime import seeded_draws
from retail_setup.generation.schemas import column_names

_EVENT_SCHEMA = """
    store_id long, product_id long, event_ts timestamp, trace_id string,
    kind string, quantity long, source string, balance long,
    receipt_id string, line_num long, requested_quantity long,
    unfulfilled_quantity long, reorder_point long, priority string,
    shipment_id string, leg long, scheduled_ts timestamp, loading_ts timestamp,
    depart_ts timestamp, eta timestamp, unloading_ts timestamp, etd timestamp
"""
_FINANCE_COLUMNS = [
    "subtotal_cents", "discount_cents", "gross_subtotal_cents", "tax_cents", "total_cents",
    "subtotal_amount", "discount_amount", "tax_amount", "total_amount", "Subtotal",
]


@dataclass
class FulfillmentResult:
    sales: dict[str, DataFrame]
    return_headers: DataFrame
    events: DataFrame


def _simulate_partition(
    rows: Iterator[Row], *, start_date: date, end_date: date, seed: int,
    capacity: int, daily_prior: float,
) -> Iterator[dict]:
    for (store_id, product_id), sku_rows in groupby(
        rows, key=lambda row: (row.store_id, row.product_id),
    ):
        demand = (
            Demand(
                datetime.fromisoformat(row.event_ts), row.receipt_id_ext,
                row.line_num, row.quantity,
                datetime.fromisoformat(row.return_ts) if row.return_ts else None,
                row.return_id,
            )
            for row in sku_rows if row.receipt_id_ext is not None
        )
        for event in simulate_sku(
            demand, store_id=store_id, product_id=product_id,
            start_date=start_date, end_date=end_date, seed=seed,
            capacity=capacity, daily_prior=daily_prior,
        ):
            # PySpark converts naive datetimes in the worker's OS timezone,
            # which can differ from the UTC Spark session (notably on Windows).
            yield {
                name: value.replace(tzinfo=timezone.utc) if isinstance(value, datetime) else value
                for name, value in asdict(event).items()
            }


def _fulfilled_finances(
    sales: dict[str, DataFrame], events: DataFrame, dims: dict[str, DataFrame],
) -> dict[str, DataFrame]:
    accepted = events.filter(F.col("kind") == "SALE").select(
        F.col("receipt_id").alias("receipt_id_ext"), "line_num",
    )
    lines = sales["fact_receipt_lines"].join(
        accepted, ["receipt_id_ext", "line_num"], "left_semi",
    )
    finance_lines = (
        lines.join(
            sales["fact_receipts"].select("receipt_id_ext", "store_id"), "receipt_id_ext",
        )
        .join(dims["dim_stores"].select(F.col("ID").alias("store_id"), "tax_rate"), "store_id")
        .join(
            dims["dim_products"].select(F.col("ID").alias("product_id"), "taxability"),
            "product_id",
        )
        .withColumn("_gross", F.col("unit_cents") * F.col("quantity"))
        .withColumn("_bps", F.round(F.col("tax_rate") * 10000).cast("long"))
        .withColumn(
            "_tax_mult", F.when(F.col("taxability") == "TAXABLE", 100)
            .when(F.col("taxability") == "REDUCED_RATE", 50).otherwise(0),
        )
        .withColumn(
            "_tax", F.floor(
                (F.col("ext_cents").cast("decimal(24,0)") * F.col("_bps")
                 * F.col("_tax_mult") + F.lit(500_000)) / F.lit(1_000_000),
            ).cast("long"),
        )
    )
    totals = finance_lines.groupBy("receipt_id_ext").agg(
        F.sum("ext_cents").alias("subtotal_cents"),
        F.sum("_gross").alias("gross_subtotal_cents"),
        F.sum("_tax").alias("tax_cents"),
    )
    header = (
        sales["fact_receipts"].drop(*_FINANCE_COLUMNS).join(totals, "receipt_id_ext")
        .withColumn("discount_cents", F.col("gross_subtotal_cents") - F.col("subtotal_cents"))
        .withColumn("total_cents", F.col("subtotal_cents") + F.col("tax_cents"))
    )
    for name in ("subtotal", "discount", "tax", "total"):
        header = header.withColumn(f"{name}_amount", _fmt(F.col(f"{name}_cents")))
    header = header.withColumn("Subtotal", F.col("subtotal_amount")).select(
        *column_names("fact_receipts"),
    )
    payments = (
        sales["fact_payments"].drop("amount_cents", "amount")
        .join(header.select("receipt_id_ext", F.col("total_cents").alias("amount_cents")),
              "receipt_id_ext")
        .withColumn("amount", _fmt(F.col("amount_cents")))
        .select(*column_names("fact_payments"))
    )
    return {
        "fact_receipts": header,
        "fact_receipt_lines": lines.select(*column_names("fact_receipt_lines")),
        "fact_payments": payments,
    }


def resolve_fulfillment(
    sales_group: dict[str, DataFrame], dims: dict[str, DataFrame], cfg: GenerationConfig,
    *, return_headers: DataFrame | None = None,
) -> FulfillmentResult:
    """Resolve candidate demand before any receipt-dependent fact generation.

    Only small dimension metadata is counted on the driver. Sales are shuffled
    by (store, SKU), sorted, and streamed on workers, never collected/grouped
    into Python lists. ``events`` is cached because all output ledgers and the
    fulfilled receipt filter must consume the exact same policy execution.
    """
    headers = (
        build_return_headers(sales_group, cfg) if return_headers is None else return_headers
    ).cache()
    returns = headers.select(
        F.col("orig_receipt_id_ext").alias("receipt_id_ext"),
        F.col("receipt_id_ext").alias("return_id"), F.col("event_ts").alias("return_ts"),
    )
    demand = (
        sales_group["fact_receipt_lines"]
        .join(sales_group["fact_receipts"].select("receipt_id_ext", "store_id"), "receipt_id_ext")
        .join(returns, "receipt_id_ext", "left")
    )
    restock = seeded_draws(cfg.seed).u(
        [F.col("return_id"), F.col("line_num").cast("string")], "restock",
    ) < 0.5
    demand = demand.select(
        "store_id", "product_id", "event_ts", "receipt_id_ext", "line_num", "quantity",
        F.when(restock, F.col("return_ts")).alias("return_ts"), "return_id",
        F.concat(F.lit("TRC-INV-"), "receipt_id_ext", F.lit("-"), "line_num").alias("_sort"),
    )
    # Seed every catalog SKU, not only products that happen to sell in this window.
    seeds = (
        dims["dim_stores"].select(F.col("ID").alias("store_id"))
        .crossJoin(dims["dim_products"].select(F.col("ID").alias("product_id")))
        .select(
            "store_id", "product_id",
            F.to_timestamp(F.lit(f"{cfg.start_date} 00:00:00")).alias("event_ts"),
            F.lit(None).cast("string").alias("receipt_id_ext"),
            F.lit(0).alias("line_num"), F.lit(0).alias("quantity"),
            F.lit(None).cast("timestamp").alias("return_ts"),
            F.lit(None).cast("string").alias("return_id"), F.lit("").alias("_sort"),
        )
    )
    n_products = dims["dim_products"].count()
    if n_products < 1:
        raise ValueError("fulfillment requires a nonempty product catalog")
    ordered = demand.unionByName(seeds).repartition("store_id", "product_id").sortWithinPartitions(
        "store_id", "product_id", "event_ts", "_sort",
    )
    for name in ("event_ts", "return_ts"):
        ordered = ordered.withColumn(name, F.date_format(name, "yyyy-MM-dd HH:mm:ss.SSSSSS"))
    # A theoretical basket prior, not a look-ahead at generated demand.
    worker = partial(
        _simulate_partition, start_date=cfg.start_date, end_date=cfg.end_date,
        seed=cfg.seed, capacity=cfg.truck_capacity,
        daily_prior=cfg.transactions_per_store_day * 10.0 / n_products,
    )
    events = sales_group["fact_receipts"].sparkSession.createDataFrame(
        ordered.rdd.mapPartitions(worker), _EVENT_SCHEMA,
    ).withColumn("event_date", F.to_date("event_ts")).cache()
    return FulfillmentResult(_fulfilled_finances(sales_group, events, dims), headers, events)
