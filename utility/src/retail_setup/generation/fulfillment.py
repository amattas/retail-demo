"""Spark adapter for the causal SKU policy and fulfilled receipt finances."""

from dataclasses import dataclass, fields
from datetime import date
from functools import partial
from itertools import chain, groupby
from operator import attrgetter
from typing import Any, Iterator

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import load_dictionaries, load_tax_rules
from retail_setup.dictionaries.models import StoreTypeProfile
from retail_setup.generation.baskets import (
    COMPLEMENT_RATE,
    TaxRules,
    price_basket,
    receipt_offer,
    store_tax_rules,
    sum_lines,
    tax_rules_by_state,
)
from retail_setup.generation.behavior import repeat_purchase_probability
from retail_setup.generation.inventory_policy import Demand, InventoryEvent, simulate_sku
from retail_setup.generation.network import store_routes
from retail_setup.generation.pricing import volume_growth
from retail_setup.generation.receipts import SEASONAL_LIFT, _fmt
from retail_setup.generation.returns import build_return_headers, returned_quantity
from retail_setup.generation.runtime import case_when, seeded_draws
from retail_setup.generation.stream_calendar import monthly_factors
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
    "subtotal_cents",
    "discount_cents",
    "gross_subtotal_cents",
    "tax_cents",
    "total_cents",
    "subtotal_amount",
    "discount_amount",
    "tax_amount",
    "total_amount",
    "Subtotal",
]


@dataclass
class FulfillmentResult:
    sales: dict[str, DataFrame]
    return_headers: DataFrame
    events: DataFrame


_EVENT_FIELDS = tuple(field.name for field in fields(InventoryEvent))
_EVENT_TYPES = dict(
    (name, kind)
    for name, kind in (column.split() for column in _EVENT_SCHEMA.replace("\n", " ").split(","))
)
_DEMAND_COLUMNS = (
    "store_id",
    "product_id",
    "event_ts",
    "receipt_id_ext",
    "line_num",
    "quantity",
    "return_ts",
    "return_id",
    "return_quantity",
    "prior_rate",
)
# Bounded output batches keep Python worker memory flat for very busy SKUs.
_ARROW_BATCH_ROWS = 10_000


def _arrow_event_schema(time_zone: str) -> Any:
    import pyarrow as pa

    arrow_types = {
        "long": pa.int64(),
        "string": pa.string(),
        "timestamp": pa.timestamp("us", tz=time_zone),
    }
    return pa.schema([(name, arrow_types[_EVENT_TYPES[name]]) for name in _EVENT_FIELDS])


def _simulate_arrow_partition(
    batches: Iterator[Any],
    *,
    start_date: date,
    end_date: date,
    seed: int,
    daily_prior: float,
    time_zone: str,
    routes: dict[int, tuple[int, int]] | None = None,
) -> Iterator[Any]:
    """Stream sorted (store, SKU) demand through the policy for ``mapInArrow``.

    Arrow moves rows between the JVM and Python in column batches instead of
    pickling one Row/dict per demand line and inventory event, which dominated
    this stage. Timestamps cross the boundary as UTC instants and are handled
    as naive UTC datetimes, exactly as the policy expects.

    Each SKU's opening row carries its demand prior (``prior_rate``, else
    ``daily_prior``); ``routes`` maps a store to its (delivery slot, lane minutes).
    """
    import pyarrow as pa

    schema = _arrow_event_schema(time_zone)
    naive = pa.timestamp("us")
    values = attrgetter(*_EVENT_FIELDS)

    def demand_rows() -> Iterator[tuple]:
        for batch in batches:
            columns = []
            for name in _DEMAND_COLUMNS:
                index = batch.schema.get_field_index(name)
                if index < 0:
                    columns.append([None] * batch.num_rows)
                    continue
                column = batch.column(index)
                if pa.types.is_timestamp(column.type):
                    column = column.cast(naive)
                columns.append(column.to_pylist())
            yield from zip(*columns)

    def flush(rows: list[tuple]) -> Any:
        return pa.RecordBatch.from_arrays(
            [
                pa.array(column, type=schema.field(index).type)
                for index, column in enumerate(zip(*rows))
            ],
            schema=schema,
        )

    pending: list[tuple] = []
    routes = routes or {}
    for (store_id, product_id), sku_rows in groupby(
        demand_rows(),
        key=lambda row: (row[0], row[1]),
    ):
        first = next(sku_rows)
        prior = first[9] if first[9] is not None else daily_prior
        slot, transit = routes.get(store_id, (None, None))
        demand = (
            Demand(event_ts, receipt_id, line_num, quantity, return_ts, return_id, returned)
            for (
                _,
                _,
                event_ts,
                receipt_id,
                line_num,
                quantity,
                return_ts,
                return_id,
                returned,
                _,
            ) in chain([first], sku_rows)
            if receipt_id is not None
        )
        for event in simulate_sku(
            demand,
            store_id=store_id,
            product_id=product_id,
            start_date=start_date,
            end_date=end_date,
            seed=seed,
            daily_prior=prior,
            slot_minute=slot,
            transit_minutes=transit,
        ):
            pending.append(values(event))
            if len(pending) >= _ARROW_BATCH_ROWS:
                yield flush(pending)
                pending = []
    if pending:
        yield flush(pending)


def _fulfilled_finances(
    sales: dict[str, DataFrame],
    events: DataFrame,
    dims: dict[str, DataFrame],
    profile: StoreTypeProfile,
    seed: int,
    tax_rules: TaxRules | None = None,
) -> dict[str, DataFrame]:
    """Receipts, lines and payments of the lines the policy could fill.

    The basket is priced again over its accepted lines (``baskets.price_basket``,
    as for candidates and the live stream), so a promotion whose minimum the
    fulfilled basket no longer meets is replaced or dropped.
    """
    draws = seeded_draws(seed)
    accepted = events.filter(F.col("kind") == "SALE").select(
        F.col("receipt_id").alias("receipt_id_ext"),
        "line_num",
    )
    lines = (
        sales["fact_receipt_lines"]
        .join(accepted, ["receipt_id_ext", "line_num"], "left_semi")
        .join(
            dims["dim_products"].select(
                F.col("ID").alias("product_id"),
                "taxability",
                F.col("Department").alias("department"),
            ),
            "product_id",
        )
        .groupBy("receipt_id_ext")
        .agg(
            F.array_sort(
                F.collect_list(
                    F.struct(
                        "line_num",
                        "product_id",
                        "quantity",
                        "unit_cents",
                        "taxability",
                        "department",
                    )
                )
            ).alias("_lines")
        )
    )
    key = F.col("receipt_id_ext")
    priced = (
        sales["fact_receipts"]
        .select("receipt_id_ext", "store_id", "event_ts", "event_date")
        .join(lines, "receipt_id_ext")
        .join(dims["dim_stores"].select(F.col("ID").alias("store_id"), "tax_rate"), "store_id")
        .join(
            store_tax_rules(dims["dim_stores"], dims.get("dim_geographies"), tax_rules or {}),
            "store_id",
        )
        .withColumn(
            "_lines",
            price_basket(
                draws,
                key,
                F.col("_lines"),
                F.month("event_date"),
                receipt_offer(draws, key, profile.promo_rate),
                F.round(F.col("tax_rate") * 10000).cast("long"),
                profile,
                F.col("tax_rule"),
            ),
        )
    )
    fulfilled_lines = priced.select(
        "receipt_id_ext", "event_ts", "event_date", F.explode("_lines").alias("_l")
    ).select(
        "receipt_id_ext",
        "event_ts",
        "event_date",
        F.col("_l.line_num").alias("line_num"),
        F.col("_l.product_id").alias("product_id"),
        F.col("_l.quantity").alias("quantity"),
        _fmt(F.col("_l.unit_cents")).alias("unit_price"),
        F.col("_l.unit_cents").alias("unit_cents"),
        _fmt(F.col("_l.ext_cents")).alias("ext_price"),
        F.col("_l.ext_cents").alias("ext_cents"),
        F.col("_l.promo_code").alias("promo_code"),
    )
    totals = priced.select(
        "receipt_id_ext",
        sum_lines(F.col("_lines"), "ext_cents").alias("subtotal_cents"),
        sum_lines(F.col("_lines"), "discount_cents").alias("discount_cents"),
        sum_lines(F.col("_lines"), "tax_cents").alias("tax_cents"),
    )
    header = (
        sales["fact_receipts"]
        .drop(*_FINANCE_COLUMNS)
        .join(totals, "receipt_id_ext")
        .withColumn("gross_subtotal_cents", F.col("subtotal_cents") + F.col("discount_cents"))
        .withColumn("total_cents", F.col("subtotal_cents") + F.col("tax_cents"))
    )
    for name in ("subtotal", "discount", "tax", "total"):
        header = header.withColumn(f"{name}_amount", _fmt(F.col(f"{name}_cents")))
    header = header.withColumn("Subtotal", F.col("subtotal_amount")).select(
        *column_names("fact_receipts"),
    )
    payments = (
        sales["fact_payments"]
        .drop("amount_cents", "amount")
        .join(
            header.select("receipt_id_ext", F.col("total_cents").alias("amount_cents")),
            "receipt_id_ext",
        )
        .withColumn("amount", _fmt(F.col("amount_cents")))
        .select(*column_names("fact_payments"))
    )
    return {
        "fact_receipts": header,
        "fact_receipt_lines": fulfilled_lines.select(*column_names("fact_receipt_lines")),
        "fact_payments": payments,
    }


def sku_demand_priors(
    dims: dict[str, DataFrame], profile: StoreTypeProfile, cfg: GenerationConfig
) -> DataFrame:
    """``(store_id, product_id, prior_rate)``: expected daily units of every SKU.

    A theoretical prior from the demand model at the first history day, never a
    look-ahead at generated demand: the store's receipts a day (volume, store
    multiplier, month and growth) times the lines per basket (the ticket target
    over the expected line value, as ``behavior.calibrated_basket_mean``), the
    department's seasonal weight, the SKU's share of its launched department
    (``behavior.retail_catalog`` popularity at the list price) and the mean
    units per line. ``prior_rate`` is NULL where the catalog lacks the inputs.
    """
    draws = seeded_draws(cfg.seed)
    start = cfg.start_date
    weights = {}
    for name, weight in profile.department_weights.items():
        lifts = next((m for token, m in SEASONAL_LIFT.items() if token in name.lower()), None)
        weights[name] = weight * (lifts[start.month - 1] if lifts else 1.0)
    total_weight = sum(weights.values()) or 1.0
    department = F.col("department")
    products = (
        dims["dim_products"]
        .select(
            F.col("ID").cast("long").alias("product_id"),
            F.col("Department").alias("department"),
            "SalePrice",
            F.to_date("LaunchDate").alias("launch_date"),
        )
        .withColumn(
            "_launched",
            F.coalesce(F.col("launch_date") <= F.lit(start), F.lit(True)),
        )
        .withColumn(
            "_popularity",
            F.when(~F.col("_launched"), F.lit(0.0)).when(
                F.col("SalePrice") > 0,
                F.exp(0.8 * draws.gauss(["product_id"], "retail-popularity") - 0.32)
                / F.pow(F.col("SalePrice"), 1.15),
            ),
        )
        .withColumn(
            "_department_share",
            case_when(
                (department == name, F.lit(weight / total_weight))
                for name, weight in weights.items()
            ).otherwise(F.when(department.isNotNull(), F.lit(0.0))),
        )
        .withColumn("_units_per_line", 1.0 / (1.0 - repeat_purchase_probability(department)))
    )
    by_department = Window.partitionBy("department")
    products = products.withColumn(
        "_share",
        F.col("_department_share")
        * F.col("_popularity")
        / F.sum("_popularity").over(by_department),
    )
    line_value = products.agg(
        F.sum(F.col("_share") * F.col("SalePrice") * F.col("_units_per_line"))
    ).collect()[0][0]
    tax_rate = dims["dim_stores"].agg(F.avg("tax_rate")).collect()[0][0] or 0.0
    lines = None
    if line_value:
        affordable = profile.avg_ticket_target / (
            line_value
            * (1.0 + tax_rate)
            * (1.0 - profile.promo_rate * 0.20)
            * (1.0 + COMPLEMENT_RATE)
        )
        lines = max(1.0, min(affordable, profile.basket_lambda)) * (1.0 + COMPLEMENT_RATE)
    month = monthly_factors(profile)[start.month - 1]
    receipts_per_day = (
        cfg.transactions_per_store_day
        * month
        * volume_growth(cfg.annual_volume_growth, cfg.anchor_date, start)
    )
    units = (
        F.lit(lines) * F.col("_share") * F.col("_units_per_line")
        if lines is not None
        else F.lit(None).cast("double")
    )
    stores = dims["dim_stores"].select(
        F.col("ID").alias("store_id"),
        F.coalesce(F.col("daily_traffic_multiplier"), F.lit(1.0)).alias("_multiplier"),
    )
    result = stores.crossJoin(products.select("product_id", units.alias("_units"))).select(
        "store_id",
        "product_id",
        (F.lit(receipts_per_day) * F.col("_multiplier") * F.col("_units")).alias("prior_rate"),
    )
    return result


def resolve_fulfillment(
    sales_group: dict[str, DataFrame],
    dims: dict[str, DataFrame],
    cfg: GenerationConfig,
    *,
    return_headers: DataFrame | None = None,
    profile: StoreTypeProfile | None = None,
) -> FulfillmentResult:
    """Resolve candidate demand before any receipt-dependent fact generation.

    Only small dimension metadata is counted on the driver. Sales are shuffled
    by (store, SKU), sorted, and streamed on workers, never collected/grouped
    into Python lists. ``events`` is cached because all output ledgers and the
    fulfilled receipt filter must consume the exact same policy execution.
    """
    if profile is None:
        profile = load_dictionaries(cfg.resolved_dictionary_root, cfg.store_type).profile
    headers = (
        build_return_headers(sales_group, cfg, dims, profile)
        if return_headers is None
        else return_headers
    ).cache()
    # The lines (and units) each return takes back; a plan without
    # returned_lines returns the whole receipt.
    returned = headers.select(
        F.col("orig_receipt_id_ext").alias("receipt_id_ext"),
        F.col("receipt_id_ext").alias("return_id"),
        F.col("event_ts").alias("return_ts"),
        (
            F.col("returned_lines")
            if "returned_lines" in headers.columns
            else F.lit(None).cast("array<struct<line_num:int,quantity:int>>")
        ).alias("returned_lines"),
    )
    demand = (
        sales_group["fact_receipt_lines"]
        .join(sales_group["fact_receipts"].select("receipt_id_ext", "store_id"), "receipt_id_ext")
        .join(returned, "receipt_id_ext", "left")
        .withColumn(
            "return_quantity",
            F.when(
                F.col("return_id").isNotNull(),
                returned_quantity(F.col("returned_lines"), F.col("line_num"), F.col("quantity")),
            ),
        )
    )
    restock = (
        seeded_draws(cfg.seed).u(
            [F.col("return_id"), F.col("line_num").cast("string")],
            "restock",
        )
        < 0.5
    ) & (F.col("return_quantity") > 0)
    demand = demand.select(
        "store_id",
        "product_id",
        "event_ts",
        "receipt_id_ext",
        "line_num",
        "quantity",
        F.when(restock, F.col("return_ts")).alias("return_ts"),
        F.when(restock, F.col("return_id")).alias("return_id"),
        F.when(restock, F.col("return_quantity")).cast("long").alias("return_quantity"),
        F.lit(None).cast("double").alias("prior_rate"),
        F.concat(F.lit("TRC-INV-"), "receipt_id_ext", F.lit("-"), "line_num").alias("_sort"),
    )
    n_products = dims["dim_products"].count()
    if n_products < 1:
        raise ValueError("fulfillment requires a nonempty product catalog")
    # A uniform basket prior covers SKUs the demand model cannot price.
    uniform_prior = cfg.transactions_per_store_day * 10.0 / n_products
    # Seed every catalog SKU, not only products that happen to sell in this
    # window, with its expected daily demand (sizes opening stock).
    seeds = sku_demand_priors(dims, profile, cfg).select(
        "store_id",
        "product_id",
        F.to_timestamp(F.lit(f"{cfg.start_date} 00:00:00")).alias("event_ts"),
        F.lit(None).cast("string").alias("receipt_id_ext"),
        F.lit(0).alias("line_num"),
        F.lit(0).alias("quantity"),
        F.lit(None).cast("timestamp").alias("return_ts"),
        F.lit(None).cast("string").alias("return_id"),
        F.lit(None).cast("long").alias("return_quantity"),
        F.coalesce(F.col("prior_rate"), F.lit(uniform_prior)).alias("prior_rate"),
        F.lit("").alias("_sort"),
    )
    ordered = (
        demand.unionByName(seeds)
        .repartition("store_id", "product_id")
        .sortWithinPartitions(
            "store_id",
            "product_id",
            "event_ts",
            "_sort",
        )
    )
    spark = sales_group["fact_receipts"].sparkSession
    time_zone = spark.conf.get("spark.sql.session.timeZone")
    if time_zone is None:
        raise RuntimeError("spark.sql.session.timeZone is not set")
    worker = partial(
        _simulate_arrow_partition,
        start_date=cfg.start_date,
        end_date=cfg.end_date,
        seed=cfg.seed,
        daily_prior=uniform_prior,
        time_zone=time_zone,
        routes={
            store: (route.slot_minute, route.transit_minutes)
            for store, route in store_routes(
                cfg.seed,
                dims["dim_stores"],
                dims["dim_geographies"],
                dims["dim_distribution_centers"],
            ).items()
        },
    )
    events = (
        ordered.select(*_DEMAND_COLUMNS)
        .mapInArrow(worker, _EVENT_SCHEMA)
        .withColumn(
            "event_date",
            F.to_date("event_ts"),
        )
        .cache()
    )
    return FulfillmentResult(
        _fulfilled_finances(
            sales_group,
            events,
            dims,
            profile,
            cfg.seed,
            tax_rules_by_state(load_tax_rules(cfg.resolved_dictionary_root)),
        ),
        headers,
        events,
    )
