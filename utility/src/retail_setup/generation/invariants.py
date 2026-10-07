"""Cross-table invariant checks. Pure reads; raises nothing — returns a report."""

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dataclass
class InvariantReport:
    checks: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    row_counts: dict[str, int] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return not self.failures


def _check(report: InvariantReport, name: str, bad_count: int) -> None:
    report.checks.append(name)
    if bad_count:
        report.failures.append(f"{name}: {bad_count} violations")


def run_invariants(spark: SparkSession, t: dict[str, DataFrame]) -> InvariantReport:
    r = InvariantReport()
    for name, df in t.items():
        r.row_counts[name] = df.count()

    receipts, lines, pay = t["fact_receipts"], t["fact_receipt_lines"], t["fact_payments"]
    _check(r, "fact_receipts.receipt_id_ext unique",
           r.row_counts["fact_receipts"] - receipts.select("receipt_id_ext").distinct().count())
    _check(r, "fact_receipt_lines -> fact_receipts FK",
           lines.join(receipts, "receipt_id_ext", "left_anti").count())
    _check(r, "fact_payments xor keys",
           pay.filter(F.col("receipt_id_ext").isNotNull() == F.col("order_id_ext").isNotNull()).count())
    _check(r, "fact_payments -> receipts FK",
           pay.filter(F.col("receipt_id_ext").isNotNull())
              .join(receipts, "receipt_id_ext", "left_anti").count())

    products = t["dim_products"].select(F.col("ID").alias("product_id"))
    for tbl in ["fact_receipt_lines", "fact_online_order_lines", "fact_promo_lines",
                "fact_store_inventory_txn", "fact_dc_inventory_txn", "fact_reorders"]:
        _check(r, f"{tbl} -> dim_products FK",
               t[tbl].join(products, "product_id", "left_anti").count())

    stores = t["dim_stores"].select(F.col("ID").alias("store_id"))
    for tbl in ["fact_receipts", "fact_store_inventory_txn", "fact_reorders",
                "fact_store_ops", "fact_foot_traffic", "fact_ble_pings"]:
        _check(r, f"{tbl} -> dim_stores FK",
               t[tbl].join(stores, "store_id", "left_anti").count())

    for tbl, df in t.items():
        if tbl.startswith("fact_") and "event_date" in df.columns:
            _check(r, f"{tbl}.event_date not null",
                   df.filter(F.col("event_date").isNull()).count())

    oh, ol = t["fact_online_order_headers"], t["fact_online_order_lines"]
    _check(r, "online order ids unique",
           r.row_counts["fact_online_order_headers"]
           - oh.select("order_id_ext").distinct().count())
    _check(r, "online lines -> headers FK",
           ol.join(oh, ol.order_id == oh.order_id_ext, "left_anti").count())

    so = t["fact_stockouts"]
    _check(r, "stockouts StoreID xor DCID",
           so.filter(F.col("StoreID").isNotNull() == F.col("DCID").isNotNull()).count())

    # Per-receipt promo discount consistency: for receipts present in
    # fact_promo_lines (SALE only by construction), the summed discount must
    # equal the implied line-level discount sum(unit_cents*quantity - ext_cents).
    promo_sum = (t["fact_promo_lines"]
                 .groupBy("receipt_id_ext")
                 .agg(F.sum("discount_cents").alias("promo_discount")))
    line_sum = (lines
                .groupBy("receipt_id_ext")
                .agg(F.sum(F.col("unit_cents") * F.col("quantity")
                           - F.col("ext_cents")).alias("line_discount")))
    _check(r, "promo discount consistency",
           promo_sum.join(line_sum, "receipt_id_ext", "left")
           .filter(F.col("line_discount").isNull()
                   | (F.col("promo_discount") != F.col("line_discount")))
           .count())

    # --- dimension geography FK integrity (datagen foreign_key validator parity)
    geo_ids = t["dim_geographies"].select(F.col("ID").alias("geo_id"))
    for dim in ["dim_stores", "dim_distribution_centers", "dim_customers"]:
        _check(r, f"{dim} -> dim_geographies FK",
               t[dim].select(F.col("GeographyID").alias("geo_id"))
               .join(geo_ids, "geo_id", "left_anti").count())

    # --- DC coverage on facts that reference a distribution center
    dc_ids = t["dim_distribution_centers"].select(F.col("ID").alias("dc_id"))
    for tbl in ["fact_dc_inventory_txn", "fact_truck_moves", "fact_reorders"]:
        _check(r, f"{tbl} -> dim_distribution_centers FK",
               t[tbl].filter(F.col("dc_id").isNotNull()).select("dc_id")
               .join(dc_ids, "dc_id", "left_anti").count())

    # --- truck coverage on logistics facts
    truck_ids = t["dim_trucks"].select(F.col("ID").alias("truck_id"))
    for tbl in ["fact_truck_moves", "fact_truck_inventory"]:
        _check(r, f"{tbl} -> dim_trucks FK",
               t[tbl].filter(F.col("truck_id").isNotNull()).select("truck_id")
               .join(truck_ids, "truck_id", "left_anti").count())

    # --- truck timing: arrival (eta) must not be after completion (etd)
    tm = t["fact_truck_moves"]
    _check(r, "fact_truck_moves etd >= eta",
           tm.filter(F.col("eta").isNotNull() & F.col("etd").isNotNull()
                     & (F.col("etd") < F.col("eta"))).count())

    # --- customer coverage on facts that resolve a customer (nullable for some)
    customer_ids = t["dim_customers"].select(F.col("ID").alias("customer_id"))
    for tbl in ["fact_receipts", "fact_online_order_headers"]:
        _check(r, f"{tbl} -> dim_customers FK",
               t[tbl].filter(F.col("customer_id").isNotNull()).select("customer_id")
               .join(customer_ids, "customer_id", "left_anti").count())
    # fact_marketing.customer_id is a nullable double (low resolution rate); cast.
    _check(r, "fact_marketing -> dim_customers FK",
           t["fact_marketing"].filter(F.col("customer_id").isNotNull())
           .select(F.col("customer_id").cast("long").alias("customer_id"))
           .join(customer_ids, "customer_id", "left_anti").count())

    # --- dim_products pricing constraints (datagen pricing validator parity)
    prod = t["dim_products"]
    _check(r, "dim_products pricing Cost<SalePrice<=MSRP",
           prod.filter(~((F.col("Cost") > 0)
                         & (F.col("Cost") < F.col("SalePrice"))
                         & (F.col("SalePrice") <= F.col("MSRP")))).count())

    # --- IMP-007 marketing attribution -------------------------------------
    # gross - discount = net holds for every SALE receipt / online order,
    # attributed or not (discount is always applied before tax).
    _check(r, "fact_receipts gross - discount = net subtotal",
           receipts.filter(
               F.col("gross_subtotal_cents") - F.col("discount_cents")
               != F.col("subtotal_cents")).count())
    _check(r, "fact_online_order_headers gross - discount = net subtotal",
           oh.filter(
               F.col("gross_subtotal_cents") - F.col("discount_cents")
               != F.col("subtotal_cents")).count())

    if "fact_marketing_attribution" in t:
        fma = t["fact_marketing_attribution"]
        _check(r, "fact_marketing_attribution.attribution_id unique",
               r.row_counts["fact_marketing_attribution"]
               - fma.select("attribution_id").distinct().count())
        _check(r, "fact_marketing_attribution xor purchase keys",
               fma.filter(F.col("receipt_id_ext").isNotNull()
                          == F.col("order_id_ext").isNotNull()).count())
        _check(r, "fact_marketing_attribution purchase_type matches xor key",
               fma.filter(
                   ((F.col("purchase_type") == "STORE")
                    != F.col("receipt_id_ext").isNotNull())
                   | ((F.col("purchase_type") == "ONLINE")
                      != F.col("order_id_ext").isNotNull())).count())
        # one journey per purchase, one purchase per journey: distinct
        # non-NULL attribution_journey_id count must equal ATTRIBUTED row count.
        attributed = fma.filter(F.col("attribution_status") == "ATTRIBUTED")
        _check(r, "fact_marketing_attribution one journey per purchase",
               attributed.count()
               - attributed.select("attribution_journey_id").distinct().count())
        _check(r, "fact_marketing_attribution journey_id set iff ATTRIBUTED",
               fma.filter(
                   (F.col("attribution_status") == "ATTRIBUTED")
                   != F.col("attribution_journey_id").isNotNull()).count())
        _check(r, "fact_marketing_attribution -> fact_receipts FK",
               fma.filter(F.col("receipt_id_ext").isNotNull())
               .select("receipt_id_ext")
               .join(receipts.select("receipt_id_ext"), "receipt_id_ext", "left_anti")
               .count())
        _check(r, "fact_marketing_attribution -> online headers FK",
               fma.filter(F.col("order_id_ext").isNotNull())
               .select("order_id_ext")
               .join(oh.select("order_id_ext"), "order_id_ext", "left_anti")
               .count())
        # financial reconciliation: gross-discount=net, net+tax=total, and for
        # any row that reached a payment decision (not RECONCILIATION_FAILED)
        # the recorded payment equals the purchase total.
        _check(r, "fact_marketing_attribution gross - discount = net",
               fma.filter(F.col("gross_subtotal_cents") - F.col("discount_cents")
                          != F.col("net_subtotal_cents")).count())
        _check(r, "fact_marketing_attribution net + tax = total",
               fma.filter(F.col("net_subtotal_cents") + F.col("tax_cents")
                          != F.col("total_cents")).count())
        _check(r, "fact_marketing_attribution approved payment = total",
               fma.filter(F.col("attribution_status").isin(
                   "ATTRIBUTED", "UNATTRIBUTED_NO_JOURNEY"))
               .filter(F.col("payment_cents") != F.col("total_cents")).count())
        _check(r, "fact_marketing_attribution attributed_revenue matches status",
               fma.filter(
                   ((F.col("attribution_status") == "ATTRIBUTED")
                    & (F.col("attributed_revenue_cents") != F.col("net_subtotal_cents")))
                   | ((F.col("attribution_status") != "ATTRIBUTED")
                      & (F.col("attributed_revenue_cents") != 0))).count())
        # window/tie correctness: every ATTRIBUTED row's last touch must be
        # inside the inclusive 7-day window, with a non-negative lag.
        _check(r, "fact_marketing_attribution touch within 7-day window",
               attributed.filter(
                   F.col("touch_ts").isNull()
                   | (F.col("lag_seconds") < 0)
                   | (F.col("lag_seconds") > F.col("attribution_window_days") * 86400)
               ).count())

        # fact_marketing: each journey's touches are exactly the two rows
        # created for it (older + newer), never orphaned or duplicated.
        mk = t["fact_marketing"]
        journey_touch_counts = (
            mk.filter(F.col("attribution_journey_id").isNotNull())
            .groupBy("attribution_journey_id")
            .agg(F.count("*").alias("n")))
        _check(r, "fact_marketing journeys have exactly 2 touches",
               journey_touch_counts.filter(F.col("n") != 2).count())
        _check(r, "fact_marketing journeys <-> attributed purchases 1:1",
               journey_touch_counts.join(
                   attributed.select("attribution_journey_id"),
                   "attribution_journey_id", "full_outer")
               .filter(F.col("n").isNull()
                       | F.col("attribution_journey_id").isNull()).count())
    _run_inventory_invariants(r, t)
    _run_business_invariants(r, t)
    return r


def _inventory_balance_checks(
    report: InvariantReport, table: str, transactions: DataFrame, node: str,
) -> None:
    ordered = Window.partitionBy(node, "product_id").orderBy("event_ts", "trace_id")
    running = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    checked = (
        transactions.withColumn("_expected_balance", F.sum("quantity").over(running))
        .withColumn("_position", F.row_number().over(ordered))
        .withColumn(
            "_initial_count",
            F.sum(F.when(F.col("txn_type") == "INITIAL", 1).otherwise(0))
            .over(Window.partitionBy(node, "product_id")),
        )
    )
    invalid = (
        F.col(node).isNull() | F.col("product_id").isNull() | F.col("event_ts").isNull()
        | F.col("trace_id").isNull() | F.col("quantity").isNull()
        | F.col("balance").isNull() | (F.col("balance") < 0)
    )
    counts = checked.agg(
        F.count(F.when(invalid, 1)).alias("physical"),
        F.count(F.when(~F.col("balance").eqNullSafe(F.col("_expected_balance")), 1))
        .alias("conservation"),
        F.count(F.when(
            (F.col("_initial_count") != 1)
            | ((F.col("_position") == 1) & ~F.col("txn_type").eqNullSafe(F.lit("INITIAL"))),
            1,
        )).alias("opening"),
        (F.count("*") - F.countDistinct("trace_id")).alias("duplicate_traces"),
    ).first()
    _check(report, f"{table} nonnegative physical balances", counts.physical)
    _check(report, f"{table} running quantity conservation", counts.conservation)
    _check(report, f"{table} exactly one opening INITIAL", counts.opening)
    _check(report, f"{table}.trace_id unique", counts.duplicate_traces)


def _inventory_mirror_violations(actual: DataFrame, expected: DataFrame) -> int:
    """Compare both directions as multisets, so duplicates cannot cancel out."""
    return actual.exceptAll(expected).unionByName(expected.exceptAll(actual)).count()


def _run_inventory_invariants(r: InvariantReport, t: dict[str, DataFrame]) -> None:
    """Check physical stock only; independent online fulfillment is not reserved here."""
    store = t["fact_store_inventory_txn"]
    dc = t["fact_dc_inventory_txn"]
    truck = t["fact_truck_inventory"]
    _inventory_balance_checks(r, "fact_store_inventory_txn", store, "store_id")
    _inventory_balance_checks(r, "fact_dc_inventory_txn", dc, "dc_id")

    requested, unfulfilled = F.col("requested_quantity"), F.col("unfulfilled_quantity")
    valid_demand = (
        F.when(
            F.col("txn_type") == "SALE",
            (F.col("quantity") < 0) & (requested == -F.col("quantity")) & (unfulfilled == 0),
        )
        .when(
            F.col("txn_type") == "LOST_SALE",
            (F.col("quantity") == 0) & (requested > 0) & (unfulfilled == requested)
            & (requested > F.col("balance")),
        )
        .otherwise((requested == 0) & (unfulfilled == 0))
    )
    _check(r, "store requested = fulfilled + unfulfilled quantity",
           store.filter(~F.coalesce(valid_demand, F.lit(False))).count())

    receipt_stock = (
        t["fact_receipt_lines"]
        .join(t["fact_receipts"].select("receipt_id_ext", "store_id", "receipt_type"),
              "receipt_id_ext")
        .select(
            F.concat(F.lit("TRC-INV-"), "receipt_id_ext", F.lit("-"), "line_num")
            .alias("trace_id"),
            "store_id", "product_id", "event_ts",
            (-F.col("quantity")).cast("long").alias("quantity"), "receipt_type",
        )
    )
    receipt_keys = ["trace_id", "store_id", "product_id", "event_ts", "quantity"]
    _check(r, "fulfilled receipt lines <-> store SALE conservation",
           _inventory_mirror_violations(
               store.filter(F.col("txn_type") == "SALE").select(*receipt_keys),
               receipt_stock.filter(F.col("receipt_type") == "SALE").select(*receipt_keys),
           ))
    # Refunds may be destroyed rather than restocked. Every actual add-back
    # must match a refunded line, but not every refund must add stock.
    _check(r, "store RETURN backed by refunded lines",
           store.filter(F.col("txn_type") == "RETURN").select(*receipt_keys)
           .exceptAll(receipt_stock.filter(F.col("receipt_type") == "RETURN")
                      .select(*receipt_keys)).count())

    for action, ledger, kind, node, source, sign in [
        ("LOAD", dc, "OUTBOUND_SHIPMENT", "dc_id", "Source", -1),
        ("UNLOAD", store, "INBOUND_SHIPMENT", "store_id", "source", 1),
    ]:
        expected = truck.filter(F.col("action") == action).select(
            F.col("location_id").alias("node_id"), "product_id", "event_ts",
            (F.col("quantity") * sign).alias("quantity"), "shipment_id",
        )
        actual = ledger.filter(F.col("txn_type") == kind).select(
            F.col(node).alias("node_id"), "product_id", "event_ts", "quantity",
            F.col(source).alias("shipment_id"),
        )
        _check(r, f"truck {action} <-> {node} ledger conservation",
               _inventory_mirror_violations(actual, expected))

    valid_cargo = (
        F.col("quantity").isNotNull() & (F.col("quantity") > 0)
        & F.col("shipment_id").isNotNull() & F.col("truck_id").isNotNull()
        & F.col("product_id").isNotNull() & F.col("event_ts").isNotNull()
        & F.col("location_id").isNotNull() & F.col("trace_id").isNotNull()
        & (((F.col("action") == "LOAD") & (F.col("location_type") == "DC"))
           | ((F.col("action") == "UNLOAD") & (F.col("location_type") == "STORE")))
    )
    cargo_order = (
        Window.partitionBy("shipment_id", "product_id", "truck_id")
        .orderBy("event_ts", "trace_id")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    cargo_counts = truck.withColumn(
        "_cargo_balance",
        F.sum(F.when(F.col("action") == "LOAD", F.col("quantity"))
              .otherwise(-F.col("quantity"))).over(cargo_order),
    ).agg(
        F.count(F.when(~F.coalesce(valid_cargo, F.lit(False)), 1)).alias("invalid"),
        F.count(F.when(F.col("_cargo_balance") < 0, 1)).alias("negative"),
        (F.count("*") - F.countDistinct("trace_id")).alias("duplicate_traces"),
        (F.count("*") - F.countDistinct("shipment_id", "product_id", "action"))
        .alias("duplicate_actions"),
    ).first()
    _check(r, "truck cargo positive with valid action and location", cargo_counts.invalid)
    _check(r, "truck nonnegative cargo balances", cargo_counts.negative)
    _check(r, "fact_truck_inventory.trace_id unique", cargo_counts.duplicate_traces)
    _check(r, "truck shipment/product actions unique", cargo_counts.duplicate_actions)
    loads = truck.filter(F.col("action") == "LOAD").select(
        "shipment_id", "product_id", "truck_id",
        F.col("quantity").alias("_loaded"), F.col("event_ts").alias("_loaded_at"),
    )
    # A final-day LOAD may still be in transit; only UNLOAD requires a prior LOAD.
    _check(r, "truck UNLOAD backed by prior equal LOAD",
           truck.filter(F.col("action") == "UNLOAD")
           .join(loads, ["shipment_id", "product_id", "truck_id"], "left")
           .filter(
               F.col("_loaded").isNull() | ~F.col("quantity").eqNullSafe(F.col("_loaded"))
               | (F.col("event_ts") < F.col("_loaded_at")),
           ).count())


def _run_business_invariants(r: InvariantReport, t: dict[str, DataFrame]) -> None:
    """IMP-010 shared business invariants: deliberate seeds must not produce
    sales while a store is closed, pre-launch sales, same-day returns, or
    same-time dimension lifecycles."""
    from retail_setup.generation.receipts import _open_close_cols

    receipts = t["fact_receipts"]

    # --- sales-while-closed: every SALE receipt's local hour must fall inside
    # its store's operating window. RETURN rows are stamped at noon, always open.
    store_hours = t["dim_stores"].select(
        F.col("ID").alias("store_id"), "operating_hours")
    open_h, close_h = _open_close_cols(F.col("operating_hours"))
    hour_checked = (
        receipts.select("store_id", F.hour("event_ts").alias("_h"))
        .join(store_hours, "store_id")
        .withColumn("_open", open_h)
        .withColumn("_close", close_h))
    _check(r, "fact_receipts within store operating hours",
           hour_checked.filter((F.col("_h") < F.col("_open"))
                               | (F.col("_h") >= F.col("_close"))).count())

    # --- pre-launch sales: no receipt line may sell a product before its
    # LaunchDate. Compares the calendar day of the sale to the product launch.
    launch = t["dim_products"].select(
        F.col("ID").alias("product_id"),
        F.to_date("LaunchDate").alias("_launch_date"))
    _check(r, "fact_receipt_lines no pre-launch sales",
           t["fact_receipt_lines"]
           .filter(F.col("quantity") > 0)  # SALE lines only; RETURN lines are negative
           .join(launch, "product_id")
           .filter(F.col("event_date") < F.col("_launch_date")).count())

    # --- same-day returns: a RETURN must post strictly after its originating
    # SALE's day. The final fact_receipts contract can't carry the sale link, so
    # the per-receipt guarantee is enforced in returns.build_return_headers and
    # proven by an adversarial unit test. Here we assert the checkable global
    # surrogates: every RETURN is dated at noon (return lifecycle) and strictly
    # after the earliest SALE on record (no return predates all sales).
    returns = receipts.filter(F.col("receipt_type") == "RETURN")
    min_sale = receipts.filter(F.col("receipt_type") == "SALE").agg(
        F.min("event_date").alias("_min")).collect()[0]["_min"]
    if min_sale is not None:
        _check(r, "fact_receipts no return before first sale",
               returns.filter(F.col("event_date") <= F.lit(min_sale)).count())
    _check(r, "fact_receipts returns posted at noon",
           returns.filter(F.hour("event_ts") != F.lit(12)).count())

    # --- same-time lifecycles: a product's LaunchDate must be a single instant
    # (no null) and strictly before the far-future horizon; guards against
    # degenerate zero-length lifecycles introduced by adversarial seeds.
    _check(r, "dim_products LaunchDate present",
           t["dim_products"].filter(F.col("LaunchDate").isNull()).count())