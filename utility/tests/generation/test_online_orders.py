from datetime import date, timedelta

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.invariants import (
    ONLINE_MAX_OPEN_SECONDS,
    InvariantReport,
    _online_order_checks,
)
from retail_setup.generation.network import store_dc_map
from retail_setup.generation.online_orders import generate_online_orders
from retail_setup.generation.schemas import column_names
from retail_setup.generation.vocabulary import (
    ONLINE_BACKORDER_SECONDS,
    ONLINE_HOURLY_WEIGHTS,
    ONLINE_SHIP_SECONDS,
)

START, END = date(2025, 4, 1), date(2025, 4, 21)


CODEGEN_OFF = {
    "spark.sql.codegen.wholeStage": "false",
    "spark.sql.codegen.factoryMode": "NO_CODEGEN",
}


@pytest.fixture(scope="module")
def setup(spark):
    # Generation's wide expressions spend longer in code generation than in
    # execution on these few rows; interpreting them keeps the fixture quick.
    previous = {key: spark.conf.get(key, None) for key in CODEGEN_OFF}
    for key, value in CODEGEN_OFF.items():
        spark.conf.set(key, value)
    try:
        yield _generate(spark)
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def _generate(spark):
    # Three weeks, so backordered orders (a 3-14 day wait) are delivered
    # within history as well as still open at its end.
    cfg = GenerationConfig(
        store_type="grocery",
        start_date=START,
        end_date=END,
        store_count=3,
        dc_count=2,
        customer_count=300,
        seed=13,
        online_orders_per_day=120,
    )
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = {
        name: frame.localCheckpoint()
        for name, frame in generate_dimensions(spark, dicts, cfg).items()
    }
    generated = {
        name: frame.localCheckpoint()
        for name, frame in generate_online_orders(spark, dims, dicts.profile, cfg).items()
    }
    return cfg, dims, generated, dicts.profile


def test_contract_columns(setup):
    _, _, g, _ = setup
    assert g["fact_online_order_headers"].columns == column_names("fact_online_order_headers")
    assert g["fact_online_order_lines"].columns == column_names("fact_online_order_lines")
    assert g["payments"].columns == column_names("fact_payments")


def test_volume_and_ids(setup):
    cfg, _, g, _ = setup
    h = g["fact_online_order_headers"]
    n = h.count()
    days = (END - START).days + 1
    assert 0.6 * days * cfg.online_orders_per_day < n < 1.4 * days * cfg.online_orders_per_day
    assert h.select("order_id_ext").distinct().count() == n
    assert h.filter(~F.col("order_id_ext").startswith("ONL")).count() == 0


def test_order_hours_follow_the_online_curve(setup):
    _, _, g, _ = setup
    counts = {
        r.hour: r["count"]
        for r in g["fact_online_order_headers"]
        .groupBy(F.hour("event_ts").alias("hour"))
        .count()
        .collect()
    }
    observed = [counts.get(hour, 0) for hour in range(24)]
    weights = ONLINE_HOURLY_WEIGHTS
    mean_o, mean_w = sum(observed) / 24, sum(weights) / 24
    cov = sum((o - mean_o) * (w - mean_w) for o, w in zip(observed, weights))
    spread = (
        sum((o - mean_o) ** 2 for o in observed) * sum((w - mean_w) ** 2 for w in weights)
    ) ** 0.5
    print(f"STAT online hourly correlation={cov / spread:.3f}")
    assert cov / spread >= 0.9


def test_lines_link_and_money(setup):
    _, _, g, _ = setup
    headers = g["fact_online_order_headers"]
    lines = g["fact_online_order_lines"]
    assert lines.join(headers, lines.order_id == headers.order_id_ext, "left_anti").count() == 0
    # Cancelled lines are zero, so every order's lines sum to its subtotal.
    sums = lines.groupBy("order_id").agg(F.sum("ext_cents").alias("s"))
    j = headers.join(sums, headers.order_id_ext == sums.order_id)
    assert j.filter(F.col("subtotal_cents") != F.col("s")).count() == 0
    assert (
        headers.filter(F.col("total_cents") != F.col("subtotal_cents") + F.col("tax_cents")).count()
        == 0
    )
    # Repeated SKUs merge into one line.
    assert lines.groupBy("order_id", "product_id").count().filter("count > 1").count() == 0


def test_baskets_meet_the_ticket_target(setup):
    _, _, g, profile = setup
    live = g["fact_online_order_headers"].filter(F.col("total_cents") > 0)
    row = live.agg(F.avg("total_cents").alias("ticket")).first()
    lines = g["fact_online_order_lines"].groupBy("order_id").count()
    mean_lines = lines.agg(F.avg("count")).first()[0]
    ticket = row["ticket"] / 100.0
    print(
        f"STAT online ticket={ticket:.2f} target={profile.avg_ticket_target} lines={mean_lines:.2f}"
    )
    assert 0.7 * profile.avg_ticket_target <= ticket <= 1.3 * profile.avg_ticket_target
    assert mean_lines > 2


def test_lines_only_use_launched_products(setup):
    _, dims, generated, _ = setup
    launches = dims["dim_products"].select(
        F.col("ID").alias("product_id"),
        F.to_date("LaunchDate").alias("launch_date"),
    )
    early = (
        generated["fact_online_order_lines"]
        .join(launches, "product_id")
        .filter(F.col("event_date") < F.col("launch_date"))
    )
    assert early.count() == 0


def test_one_local_fulfillment_node_per_order(setup):
    _, dims, g, _ = setup
    lines = g["fact_online_order_lines"]
    modes = {r.fulfillment_mode for r in lines.select("fulfillment_mode").distinct().collect()}
    assert modes == {"SHIP_FROM_DC", "SHIP_FROM_STORE", "BOPIS"}
    orders = lines.select("order_id", "fulfillment_mode", "node_type", "node_id").distinct()
    assert orders.count() == lines.select("order_id").distinct().count()
    # Store modes use the customer's home store, ship-from-DC its DC.
    home = (
        g["fact_online_order_headers"]
        .join(
            dims["dim_customers"].select(F.col("ID").alias("customer_id"), "HomeStoreID"),
            "customer_id",
        )
        .join(
            store_dc_map(
                dims["dim_stores"], dims["dim_geographies"], dims["dim_distribution_centers"]
            ).withColumnRenamed("store_id", "HomeStoreID"),
            "HomeStoreID",
        )
        .select(F.col("order_id_ext").alias("order_id"), "HomeStoreID", "dc_id")
    )
    checked = orders.join(home, "order_id")
    assert checked.count() == orders.count()
    wrong = checked.filter(
        F.when(F.col("node_type") == "DC", F.col("node_id") != F.col("dc_id")).otherwise(
            F.col("node_id") != F.col("HomeStoreID")
        )
    )
    assert wrong.count() == 0
    assert (
        checked.filter(
            (F.col("fulfillment_mode") == "SHIP_FROM_DC") != (F.col("node_type") == "DC")
        ).count()
        == 0
    )


def test_lifecycle_ordering(setup):
    _, _, g, _ = setup
    lines = g["fact_online_order_lines"].filter(F.col("fulfillment_status") == "DELIVERED")
    assert lines.count() > 0
    assert lines.filter(F.col("picked_ts") < F.col("event_ts")).count() == 0
    shipped = lines.filter(F.col("shipped_ts").isNotNull())
    assert shipped.filter(F.col("shipped_ts") < F.col("picked_ts")).count() == 0
    assert shipped.filter(F.col("delivered_ts") < F.col("shipped_ts")).count() == 0


def test_backorders_resolve_and_statuses_are_as_of_history_end(setup):
    _, _, g, _ = setup
    lines = g["fact_online_order_lines"]
    statuses = {
        r.fulfillment_status for r in lines.select("fulfillment_status").distinct().collect()
    }
    assert statuses <= {"DELIVERED", "SHIPPED", "PICKED", "PENDING", "CANCELLED"}
    end = F.lit(END + timedelta(days=1)).cast("timestamp")
    for stage in ("picked_ts", "shipped_ts", "delivered_ts"):
        assert lines.filter(F.col(stage) >= end).count() == 0
    # Backordered orders wait for stock, then ship and are delivered.
    wait = ONLINE_SHIP_SECONDS[1] + ONLINE_BACKORDER_SECONDS[0] - 1
    waited = lines.filter(F.unix_timestamp("shipped_ts") - F.unix_timestamp("picked_ts") > wait)
    assert waited.filter(F.col("fulfillment_status") == "DELIVERED").count() > 0
    # Nothing stays SHIPPED (or otherwise open) long before the end of history.
    open_lines = lines.filter(F.col("fulfillment_status").isin("PENDING", "PICKED", "SHIPPED"))
    assert open_lines.count() > 0
    cutoff = END + timedelta(days=1) - timedelta(seconds=ONLINE_MAX_OPEN_SECONDS)
    assert open_lines.filter(F.col("event_ts") < F.lit(cutoff).cast("timestamp")).count() == 0
    assert open_lines.filter(F.col("delivered_ts").isNotNull()).count() == 0
    assert (
        lines.filter("fulfillment_status = 'SHIPPED'")
        .filter(F.col("fulfillment_mode") == "BOPIS")
        .count()
        == 0
    )


def test_cancellations_zeroed_no_payment(setup):
    _, _, g, _ = setup
    h, pay = g["fact_online_order_headers"], g["payments"]
    cancelled_lines = g["fact_online_order_lines"].filter("fulfillment_status = 'CANCELLED'")
    assert cancelled_lines.count() > 0
    assert cancelled_lines.filter(F.col("ext_cents") != 0).count() == 0
    cancelled = h.join(
        cancelled_lines.select(F.col("order_id").alias("order_id_ext")).distinct(),
        "order_id_ext",
    )
    assert cancelled.filter(F.col("total_cents") != 0).count() == 0
    assert pay.join(cancelled.select("order_id_ext"), "order_id_ext").count() == 0
    # payments basics
    assert pay.filter(F.col("receipt_id_ext").isNotNull()).count() == 0
    assert pay.filter(F.col("store_id").isNotNull()).count() == 0
    # A declined order is retried on another tender (approved) or voided as a
    # cancellation: no fulfilled order has only a declined payment.
    live = h.join(cancelled.select("order_id_ext"), "order_id_ext", "left_anti")
    approved = pay.filter("status = 'APPROVED'").groupBy("order_id_ext").count()
    assert approved.filter(F.col("count") != 1).count() == 0
    assert live.join(approved, "order_id_ext", "left_anti").count() == 0
    assert (
        pay.filter("status = 'DECLINED'").join(approved, "order_id_ext", "left_anti").count() == 0
    )
    assert pay.select("transaction_id").distinct().count() == pay.count()


def test_online_invariants_pass_and_catch_violations(spark, setup):
    _, _, g, _ = setup
    report = InvariantReport()
    _online_order_checks(report, g["fact_online_order_lines"])
    assert report.passed, report.failures

    bad = spark.createDataFrame(
        [
            ("O1", 1, "BOPIS", "STORE", 1, "CANCELLED", 500, "2025-01-01 10:00:00", None),
            ("O1", 2, "SHIP_FROM_DC", "DC", 1, "CANCELLED", 0, "2025-01-01 10:00:00", None),
            ("O2", 1, "BOPIS", "STORE", 1, "SHIPPED", 100, "2025-01-01 10:00:00", None),
            ("O3", 1, "BOPIS", "STORE", 1, "DELIVERED", 100, "2025-03-01 10:00:00", None),
        ],
        "order_id string, line_num long, fulfillment_mode string, node_type string, "
        "node_id long, fulfillment_status string, ext_cents long, event_ts string, "
        "delivered_ts timestamp",
    ).withColumn("event_ts", F.to_timestamp("event_ts"))
    report = InvariantReport()
    _online_order_checks(report, bad)
    assert {failure.split(":")[0] for failure in report.failures} == {
        "online orders have one fulfillment mode and node",
        "cancelled online lines carry no amount",
        "online orders open only near the end of history",
    }
