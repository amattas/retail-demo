"""Cross-table invariant checks. Pure reads; raises nothing — returns a report."""

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


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
    return r
