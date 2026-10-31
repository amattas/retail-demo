"""Physical-stock checks use native Spark literals, never Python-backed RDDs."""

import json

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType

from retail_setup.generation.invariants import InvariantReport, _run_inventory_invariants
from retail_setup.generation.schemas import spark_schema


def _stock_frame(spark, table, rows):
    return (
        spark.range(1)
        .select(
            F.explode(F.from_json(F.lit(json.dumps(rows)), ArrayType(spark_schema(table)))).alias(
                "row"
            ),
        )
        .select("row.*")
    )


def _stock_tables(spark):
    store = [
        ("INIT1", 1, "2025-01-01T00:00:00", "INITIAL", 10, 10, "SEED", 0, 0),
        ("INIT2", 2, "2025-01-01T00:00:00", "INITIAL", 7, 7, "SEED", 0, 0),
        ("TRC-INV-R1-1", 1, "2025-01-01T09:00:00", "SALE", -10, 0, "CUSTOMER_PURCHASE", 10, 0),
        ("LOST1", 1, "2025-01-01T10:00:00", "LOST_SALE", 0, 0, "CUSTOMER_PURCHASE", 4, 4),
        ("SI1", 1, "2025-01-01T14:00:00", "INBOUND_SHIPMENT", 6, 6, "SHIP1", 0, 0),
        ("TRC-INV-RET1-1", 1, "2025-01-02T12:00:00", "RETURN", 2, 8, "RET1", 0, 0),
        ("TRC-INV-R2-1", 1, "2025-01-02T13:00:00", "SALE", -3, 5, "CUSTOMER_PURCHASE", 3, 0),
    ]
    dc = [
        ("DCINIT1", 1, "2025-01-01T00:00:00", "INITIAL", 20, 20, "SEED"),
        ("DCINIT2", 2, "2025-01-01T00:00:00", "INITIAL", 20, 20, "SEED"),
        ("SUP1", 1, "2025-01-01T11:59:59", "INBOUND_SHIPMENT", 6, 26, "SUPPLIER-SHIP1"),
        ("DCOUT1", 1, "2025-01-01T12:00:00", "OUTBOUND_SHIPMENT", -6, 20, "SHIP1"),
        ("SUP2", 2, "2025-01-02T22:59:59", "INBOUND_SHIPMENT", 5, 25, "SUPPLIER-SHIP2"),
        ("DCOUT2", 2, "2025-01-02T23:00:00", "OUTBOUND_SHIPMENT", -5, 20, "SHIP2"),
    ]
    truck = [
        ("LOAD1", "SHIP1", 1, 1, 6, "2025-01-01T12:00:00", "LOAD", "DC"),
        ("UNLOAD1", "SHIP1", 1, 1, 6, "2025-01-01T14:00:00", "UNLOAD", "STORE"),
        ("LOAD2", "SHIP2", 2, 2, 5, "2025-01-02T23:00:00", "LOAD", "DC"),
    ]
    receipts = [
        ("R1", "SALE", 10, "2025-01-01T09:00:00"),
        ("R2", "SALE", 3, "2025-01-02T13:00:00"),
        ("RET1", "RETURN", -2, "2025-01-02T12:00:00"),
        ("RET2", "RETURN", -1, "2025-01-02T12:30:00"),
    ]
    rows = {
        "fact_store_inventory_txn": [
            dict(
                zip(
                    [
                        "trace_id",
                        "product_id",
                        "event_ts",
                        "txn_type",
                        "quantity",
                        "balance",
                        "source",
                        "requested_quantity",
                        "unfulfilled_quantity",
                    ],
                    row,
                ),
                store_id=1,
            )
            for row in store
        ],
        "fact_dc_inventory_txn": [
            dict(
                zip(
                    [
                        "trace_id",
                        "product_id",
                        "event_ts",
                        "txn_type",
                        "quantity",
                        "balance",
                        "Source",
                    ],
                    row,
                ),
                dc_id=1,
            )
            for row in dc
        ],
        "fact_truck_inventory": [
            dict(
                zip(
                    [
                        "trace_id",
                        "shipment_id",
                        "product_id",
                        "truck_id",
                        "quantity",
                        "event_ts",
                        "action",
                        "location_type",
                    ],
                    row,
                ),
                location_id=1,
            )
            for row in truck
        ],
        "fact_receipts": [
            {"receipt_id_ext": identifier, "receipt_type": kind, "store_id": 1, "event_ts": ts}
            for identifier, kind, _, ts in receipts
        ],
        "fact_receipt_lines": [
            {
                "receipt_id_ext": identifier,
                "line_num": 1,
                "product_id": 1,
                "quantity": quantity,
                "event_ts": ts,
            }
            for identifier, _, quantity, ts in receipts
        ],
    }
    return {table: _stock_frame(spark, table, values) for table, values in rows.items()}


def test_physical_invariants_accept_conserved_stock_returns_and_inflight_load(spark):
    report = InvariantReport()
    _run_inventory_invariants(report, _stock_tables(spark))
    assert report.passed, report.failures
    assert len(report.checks) == 18
    assert "truck nonnegative cargo balances" in report.checks
    assert "fulfilled receipt lines <-> store SALE conservation" in report.checks
    assert "store RETURN backed by refunded lines" in report.checks


def test_physical_invariants_reject_clamps_deficits_lost_demand_and_broken_transfers(spark):
    tables = _stock_tables(spark)
    store = tables["fact_store_inventory_txn"]
    store = (
        store.withColumn(
            "quantity",
            F.when(F.col("trace_id") == "TRC-INV-R1-1", -11)
            .when(F.col("trace_id") == "TRC-INV-RET1-1", 4)
            .when(F.col("trace_id") == "SI1", 7)
            .otherwise(F.col("quantity")),
        )
        .withColumn(
            "balance",
            F.when(F.col("trace_id") == "TRC-INV-R2-1", -1).otherwise(F.col("balance")),
        )
        .withColumn(
            "unfulfilled_quantity",
            F.when(F.col("txn_type") == "LOST_SALE", F.lit(None).cast("long")).otherwise(
                F.col("unfulfilled_quantity")
            ),
        )
    )
    tables["fact_store_inventory_txn"] = store.unionByName(store.filter("trace_id = 'INIT2'"))
    tables["fact_dc_inventory_txn"] = (
        tables["fact_dc_inventory_txn"]
        .filter("trace_id != 'DCINIT2'")
        .withColumn(
            "quantity",
            F.when(F.col("trace_id") == "DCOUT1", -7).otherwise(F.col("quantity")),
        )
        .withColumn(
            "balance",
            F.when(F.col("trace_id") == "DCOUT1", -1).otherwise(F.col("balance")),
        )
    )
    truck = (
        tables["fact_truck_inventory"]
        .withColumn(
            "event_ts",
            F.when(
                F.col("action") == "UNLOAD", F.to_timestamp(F.lit("2025-01-01 11:00:00"))
            ).otherwise(F.col("event_ts")),
        )
        .withColumn(
            "location_type",
            F.when(F.col("action") == "UNLOAD", "DC").otherwise(F.col("location_type")),
        )
        .withColumn(
            "quantity",
            F.when(F.col("action") == "UNLOAD", 8).otherwise(F.col("quantity")),
        )
    )
    tables["fact_truck_inventory"] = truck.unionByName(truck.filter("action = 'UNLOAD'"))
    report = InvariantReport()
    _run_inventory_invariants(report, tables)
    for name in [
        "fact_store_inventory_txn nonnegative physical balances",
        "fact_store_inventory_txn running quantity conservation",
        "fact_store_inventory_txn exactly one opening INITIAL",
        "fact_store_inventory_txn.trace_id unique",
        "fact_dc_inventory_txn nonnegative physical balances",
        "fact_dc_inventory_txn running quantity conservation",
        "fact_dc_inventory_txn exactly one opening INITIAL",
        "store requested = fulfilled + unfulfilled quantity",
        "fulfilled receipt lines <-> store SALE conservation",
        "store RETURN backed by refunded lines",
        "truck LOAD <-> dc_id ledger conservation",
        "truck UNLOAD <-> store_id ledger conservation",
        "truck cargo positive with valid action and location",
        "truck nonnegative cargo balances",
        "fact_truck_inventory.trace_id unique",
        "truck shipment/product actions unique",
        "truck UNLOAD backed by prior equal LOAD",
    ]:
        assert any(failure.startswith(f"{name}: ") for failure in report.failures), name
