"""Small executable Spark checks; no filesystem or full generation suite."""

import json
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation import engine
from retail_setup.generation.fulfillment import resolve_fulfillment
from retail_setup.generation.inventory import generate_inventory_chain
from retail_setup.generation.inventory_policy import opening_stock
from retail_setup.generation.returns import generate_returns
from retail_setup.generation.runtime import seeded_draws
from retail_setup.generation.schemas import column_names, spark_schema


def _native_frame(spark, rows, table):
    # Native literal inputs avoid launching workers merely to deserialize tiny
    # test fixtures; the fulfillment mapPartitions still runs on real workers.
    payload = json.dumps(rows, default=lambda value: value.isoformat())
    return spark.range(1).select(
        F.explode(F.from_json(F.lit(payload), ArrayType(spark_schema(table)))).alias("row"),
    ).select("row.*")


@pytest.fixture(scope="module")
def candidate_case(spark):
    cfg = GenerationConfig(
        store_type="grocery", start_date=date(2025, 1, 1), end_date=date(2025, 1, 12),
        store_count=1, dc_count=1, seed=31, truck_capacity=25,
        transactions_per_store_day=5, return_rate=0.1,
    )
    dims = {
        "dim_stores": _native_frame(
            spark, [{"ID": 1, "GeographyID": 1, "tax_rate": 0.0825}], "dim_stores",
        ),
        "dim_products": _native_frame(
            spark, [{"ID": product, "taxability": ["TAXABLE", "REDUCED_RATE", "EXEMPT"][(product - 1) % 3]}
                    for product in range(1, 11)], "dim_products",
        ),
        "dim_distribution_centers": _native_frame(
            spark, [{"ID": 1, "GeographyID": 1}], "dim_distribution_centers",
        ),
        "dim_geographies": _native_frame(
            spark, [{"ID": 1, "State": "WA", "Region": "WEST"}], "dim_geographies",
        ),
        "dim_trucks": _native_frame(
            spark, [{"ID": 1, "DCID": 1.0}, {"ID": 2, "DCID": None}], "dim_trucks",
        ),
    }
    headers, lines, payments = [], [], []

    def receipt(identifier, day, hour, products):
        ts = datetime(2025, 1, 1, hour, tzinfo=timezone.utc) + timedelta(days=day)
        gross, subtotal, tax = 0, 0, 0
        for number, (product, quantity) in enumerate(products, 1):
            unit = 101
            ext = unit * quantity - 1
            mult = [100, 50, 0][(product - 1) % 3]
            gross += unit * quantity
            subtotal += ext
            tax += (ext * 825 * mult + 500_000) // 1_000_000
            lines.append({
                "receipt_id_ext": identifier, "event_ts": ts, "event_date": ts.date(),
                "line_num": number, "product_id": product, "quantity": quantity,
                "unit_cents": unit, "unit_price": "1.01", "ext_cents": ext,
                "ext_price": f"{ext / 100:.2f}", "promo_code": "TEST",
            })
        header = {
            "receipt_id_ext": identifier, "trace_id": f"TRC-{identifier}",
            "event_ts": ts, "event_date": ts.date(), "store_id": 1, "customer_id": 1,
            "receipt_type": "SALE", "tender_type": "CREDIT_CARD", "payment_method": "CREDIT_CARD",
            "gross_subtotal_cents": gross, "subtotal_cents": subtotal,
            "discount_cents": gross - subtotal, "tax_cents": tax, "total_cents": subtotal + tax,
        }
        headers.append(header)
        payments.append({
            "receipt_id_ext": identifier, "event_ts": ts, "event_date": ts.date(),
            "payment_method": "CREDIT_CARD", "amount_cents": subtotal + tax,
            "transaction_id": f"PAY-{identifier}", "status": "APPROVED",
            "store_id": 1, "customer_id": 1, "processing_time_ms": 2000,
        })

    receipt("PARTIAL", 0, 0, [(1, opening_stock(cfg.seed, 1, 1)), (2, 1000), (3, 2), (5, 3)])
    receipt("EMPTY", 0, 1, [(1, 1000)])
    for day in range(12):
        for product in range(1, 11):
            receipt(f"R-{day:02d}-{product:02d}", day, 9, [(product, 15)])
    sales = {
        table: _native_frame(spark, rows, table)
        for table, rows in [
            ("fact_receipts", headers), ("fact_receipt_lines", lines), ("fact_payments", payments),
        ]
    }
    planned = (
        sales["fact_receipts"].filter(F.col("receipt_id_ext").isin("PARTIAL", "EMPTY"))
        .withColumnRenamed("receipt_id_ext", "orig_receipt_id_ext")
        .withColumnRenamed("event_date", "orig_event_date")
        .withColumn("receipt_id_ext", F.concat(F.lit("RET-"), "orig_receipt_id_ext"))
        .withColumn("trace_id", F.concat(F.lit("TRC-"), "receipt_id_ext"))
        .withColumn("event_ts", F.to_timestamp(F.lit("2025-01-03 12:00:00")))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("receipt_type", F.lit("RETURN"))
        .withColumn("customer_id", F.lit(None).cast("long"))
    )
    return cfg, dims, sales, planned


@pytest.fixture(scope="module")
def fulfilled_case(candidate_case):
    cfg, dims, sales, planned = candidate_case
    result = resolve_fulfillment(sales, dims, cfg, return_headers=planned)
    result.sales = {name: frame.cache() for name, frame in result.sales.items()}
    yield result
    for frame in result.sales.values():
        frame.unpersist()
    result.events.unpersist()
    result.return_headers.unpersist()


def test_spark_fulfillment_finances_returns_and_all_ledgers(spark, candidate_case, fulfilled_case):
    cfg, dims, candidate, _ = candidate_case
    result = fulfilled_case
    sales = {name: frame.collect() for name, frame in result.sales.items()}
    headers = {row.receipt_id_ext: row for row in sales["fact_receipts"]}
    assert "EMPTY" not in headers
    assert len(sales["fact_receipt_lines"]) < candidate["fact_receipt_lines"].count()
    partial = headers["PARTIAL"]
    retained = [row for row in sales["fact_receipt_lines"] if row.receipt_id_ext == "PARTIAL"]
    assert {row.product_id for row in retained} == {1, 3, 5}
    assert partial.gross_subtotal_cents == sum(row.quantity * row.unit_cents for row in retained)
    assert partial.subtotal_cents == sum(row.ext_cents for row in retained)
    assert partial.discount_cents == 3
    assert partial.tax_cents == sum(
        (row.ext_cents * 825 * {1: 100, 3: 0, 5: 50}[row.product_id] + 500_000) // 1_000_000
        for row in retained
    )
    assert partial.total_cents == partial.subtotal_cents + partial.tax_cents
    assert partial.Subtotal == partial.subtotal_amount == f"{partial.subtotal_cents / 100:.2f}"
    assert all(row.amount_cents == headers[row.receipt_id_ext].total_cents
               for row in sales["fact_payments"])
    refunds = generate_returns(spark, result.sales, dims, cfg, return_headers=result.return_headers)
    refund = refunds["fact_receipts"].collect()
    assert len(refund) == 1 and refund[0].receipt_id_ext == "RET-PARTIAL"
    assert refund[0].event_date == date(2025, 1, 3)
    assert refund[0].total_cents == -partial.total_cents
    assert refund[0].discount_cents == -partial.discount_cents
    returned_lines = refunds["fact_receipt_lines"].collect()
    assert {row.product_id for row in returned_lines} == {1, 3, 5}
    assert refunds["fact_payments"].first().amount_cents == -partial.total_cents
    inventory = generate_inventory_chain(
        spark, result.sales, refunds, dims, cfg, fulfillment=result,
    )
    rows = {}
    for table, frame in inventory.items():
        assert frame.columns == column_names(table)
        rows[table] = frame.collect()
        assert len({row.trace_id for row in rows[table]}) == len(rows[table]), table
    for table, node in [("fact_store_inventory_txn", "store_id"), ("fact_dc_inventory_txn", "dc_id")]:
        balances = {}
        for row in sorted(rows[table], key=lambda row: (row.event_ts, row.trace_id)):
            key = (row[node], row.product_id)
            balances[key] = balances.get(key, 0) + row.quantity
            assert row.balance == balances[key] >= 0
            assert row.event_date <= cfg.end_date
    store = rows["fact_store_inventory_txn"]
    assert sum(row.txn_type == "SALE" for row in store) == len(sales["fact_receipt_lines"])
    assert sum(row.requested_quantity for row in store) == sum(
        row.quantity for row in candidate["fact_receipt_lines"].collect()
    )
    assert any(row.txn_type == "LOST_SALE" and row.unfulfilled_quantity > 0 for row in store)
    assert all(row.quantity == 0 for row in store if row.txn_type == "LOST_SALE")
    restocked = {
        row.line_num for row in refunds["fact_receipt_lines"]
        .filter(seeded_draws(cfg.seed).u(
            [F.col("receipt_id_ext"), F.col("line_num").cast("string")], "restock",
        ) < 0.5).collect()
    }
    assert sum(row.quantity for row in store if row.txn_type == "RETURN") == sum(
        -row.quantity for row in returned_lines if row.line_num in restocked
    )
    truck = rows["fact_truck_inventory"]
    assert truck and all(row.event_date <= cfg.end_date for row in truck)
    loads = {(row.shipment_id, row.product_id): row for row in truck if row.action == "LOAD"}
    for row in truck:
        assert 0 < row.quantity <= cfg.truck_capacity
        if row.action == "UNLOAD":
            assert loads[row.shipment_id, row.product_id].quantity == row.quantity
            assert any(txn.source == row.shipment_id and txn.product_id == row.product_id
                       and txn.event_ts == row.event_ts and txn.quantity == row.quantity
                       for txn in store)
        else:
            assert any(txn.Source == row.shipment_id and txn.product_id == row.product_id
                       and txn.event_ts == row.event_ts and txn.quantity == -row.quantity
                       for txn in rows["fact_dc_inventory_txn"])
    assert sum(row.quantity for row in truck if row.action == "UNLOAD") == sum(
        row.quantity for row in store if row.txn_type == "INBOUND_SHIPMENT"
    )
    moves = {}
    for row in rows["fact_truck_moves"]:
        moves.setdefault(row.shipment_id, []).append(row)
    for shipment in moves.values():
        assert len(shipment) == 6
        assert [row.status for row in sorted(shipment, key=lambda row: row.event_ts)] == [
            "SCHEDULED", "LOADING", "IN_TRANSIT", "ARRIVED", "UNLOADING", "COMPLETED",
        ]
    assert {row.product_id for row in rows["fact_reorders"]} == set(range(1, 11))
    assert rows["fact_stockouts"]
    for row in rows["fact_stockouts"]:
        assert any(txn.balance == 0 and txn.event_ts == row.event_ts
                   and txn.product_id == row.ProductID for txn in store)


def test_spark_partition_reproducibility(spark, candidate_case, fulfilled_case):
    cfg, dims, sales, planned = candidate_case
    original = spark.conf.get("spark.sql.shuffle.partitions")
    try:
        expected = {row.trace_id: row.asDict() for row in fulfilled_case.events.collect()}
        spark.conf.set("spark.sql.shuffle.partitions", "2")
        other = resolve_fulfillment(
            {name: frame.repartition(3) for name, frame in sales.items()},
            dims, cfg, return_headers=planned.repartition(2),
        )
        actual = {row.trace_id: row.asDict() for row in other.events.collect()}
        assert expected == actual
        other.events.unpersist()
        other.return_headers.unpersist()
    finally:
        spark.conf.set("spark.sql.shuffle.partitions", original)


def test_direct_inventory_rejects_unresolved_candidate_sales(spark, candidate_case):
    cfg, dims, sales, _ = candidate_case
    with pytest.raises(ValueError, match="resolve_fulfillment"):
        generate_inventory_chain(spark, sales, {}, dims, cfg)


def test_engine_public_path_uses_fulfilled_sales(spark, monkeypatch, candidate_case):
    cfg, dims, sales, _ = candidate_case

    def empty(table):
        return spark.createDataFrame([], spark_schema(table))

    monkeypatch.setattr(engine.dims_mod, "generate_dimensions", lambda *_: dims.copy())
    monkeypatch.setattr(engine.dims_mod, "generate_dim_date", lambda *_: empty("dim_date"))
    monkeypatch.setattr(engine.receipts_mod, "generate_receipts_group", lambda *_: sales.copy())
    monkeypatch.setattr(engine.online_orders, "generate_online_orders", lambda *_: {
        "fact_online_order_headers": empty("fact_online_order_headers"),
        "fact_online_order_lines": empty("fact_online_order_lines"), "payments": empty("fact_payments"),
    })
    seen = {}

    # Stage intermediates are released when generate_all returns, so observe
    # what each builder received while generation is running.
    def promotions(_spark, accepted):
        seen["promotion_lines"] = accepted["fact_receipt_lines"].count()
        return empty("fact_promotions"), empty("fact_promo_lines")

    monkeypatch.setattr(engine.promotions, "generate_promotions", promotions)
    monkeypatch.setattr(engine.marketing, "generate_marketing", lambda *_: empty("fact_marketing"))
    monkeypatch.setattr(engine.attribution, "generate_attribution", lambda _s, tables, _c: {
        **tables, "fact_marketing_attribution": empty("fact_marketing_attribution"),
    })
    monkeypatch.setattr(engine.store_activity, "generate_store_ops", lambda *_: empty("fact_store_ops"))

    def footfall(_spark, accepted, *_):
        seen["footfall_unfulfilled"] = accepted.filter("receipt_id_ext = 'EMPTY'").count()
        return empty("fact_foot_traffic")

    monkeypatch.setattr(engine.store_activity, "generate_foot_traffic", footfall)
    monkeypatch.setattr(engine.sensors, "generate_ble", lambda *_: (
        empty("fact_ble_pings"), empty("fact_customer_zone_changes"),
    ))
    result = engine.generate_all(spark, SimpleNamespace(profile=None), cfg)
    accepted = result.tables["fact_receipts"].filter("receipt_type = 'SALE'")
    assert accepted.filter("receipt_id_ext = 'EMPTY'").count() == 0
    assert seen["footfall_unfulfilled"] == 0
    sale_txns = result.tables["fact_store_inventory_txn"].filter("txn_type = 'SALE'").count()
    assert sale_txns == seen["promotion_lines"]
    assert sale_txns < sales["fact_receipt_lines"].count()
