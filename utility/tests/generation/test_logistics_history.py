"""History under logistics incidents: delays, DC stock and the dwell model (Spark).

One small history runs with injected incidents (a supplier that stops
shipping one DC for five weeks and a lane delay into one store of the
other DC) so the statistical checks do not depend on the random plan.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import numpy as np
import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation import calibration as cal
from retail_setup.generation import fulfillment as fulfillment_mod
from retail_setup.generation import inventory as inventory_mod
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.fulfillment import resolve_fulfillment
from retail_setup.generation.incidents import Incident
from retail_setup.generation.inventory import delivery_routes, generate_inventory_chain
from retail_setup.generation.receipts import generate_receipts_group

START, END = date(2025, 7, 7), date(2025, 8, 17)
SHORT_FROM, SHORT_TO = (
    datetime(2025, 7, 14, tzinfo=timezone.utc),
    datetime(2025, 8, 17, tzinfo=timezone.utc),
)
LANE_FROM, LANE_TO = (
    datetime(2025, 7, 14, tzinfo=timezone.utc),
    datetime(2025, 7, 25, tzinfo=timezone.utc),
)
# notebook 13's quality gate (TARGET_MAE minutes).
TARGET_MAE = 15.0


@pytest.fixture(scope="module")
def history(spark):
    cfg = GenerationConfig(
        store_type="grocery",
        start_date=START,
        end_date=END,
        store_count=6,
        dc_count=2,
        customer_count=300,
        seed=29,
        transactions_per_store_day=40,
    )
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    # Eager checkpoints, as the engine checkpoints stage outputs: chained
    # lineage would otherwise make every action re-plan the whole history.
    dims = {
        name: frame.localCheckpoint()
        for name, frame in generate_dimensions(spark, dicts, cfg).items()
    }
    routes = delivery_routes(dims, cfg)
    by_dc: dict[int, list[int]] = {}
    for store, route in sorted(routes.items()):
        by_dc.setdefault(route.dc_id, []).append(store)
    if len(by_dc) < 2:
        pytest.skip("the generated geography routes every store to one DC")
    short_dc, lane_dc = sorted(by_dc, key=lambda dc: (-len(by_dc[dc]), dc))[:2]
    lane_store = by_dc[lane_dc][0]
    incidents = [
        Incident(
            f"INC-SUP-TEST-{group}",
            "SUPPLIER_SHORT_SHIP",
            short_dc,
            None,
            SHORT_FROM,
            SHORT_TO,
            "HIGH",
            tuple(by_dc[short_dc]),
            "test supplier stops shipping",
            fill_rate=0.0,
            supplier_group=group,
        )
        for group in range(cal.SUPPLIER_GROUPS)
    ] + [
        Incident(
            "INC-LANE-TEST",
            "LANE_DELAY",
            lane_dc,
            lane_store,
            LANE_FROM,
            LANE_TO,
            "HIGH",
            (lane_store,),
            "test lane closure",
            delay_minutes=120,
        )
    ]

    def planned(*_args, **_kwargs):
        return incidents

    patch = pytest.MonkeyPatch()
    patch.setattr(fulfillment_mod, "network_incidents", planned)
    patch.setattr(inventory_mod, "network_incidents", planned)
    try:
        sales = {
            name: frame.localCheckpoint()
            for name, frame in generate_receipts_group(spark, dims, dicts.profile, cfg).items()
        }
        fulfilled = resolve_fulfillment(sales, dims, cfg)
        fulfilled.sales = {name: frame.localCheckpoint() for name, frame in fulfilled.sales.items()}
        fulfilled.return_headers = fulfilled.return_headers.localCheckpoint()
        fulfilled.events = fulfilled.events.localCheckpoint()
        inv = generate_inventory_chain(spark, fulfilled.sales, {}, dims, cfg, fulfillment=fulfilled)
        tables = {name: frame.localCheckpoint() for name, frame in inv.items()}
    finally:
        patch.undo()
    zones = {row["ID"]: row["TimeZone"] for row in dims["dim_stores"].collect()}
    return {
        "tables": tables,
        "by_dc": by_dc,
        "short_dc": short_dc,
        "lane_dc": lane_dc,
        "lane_store": lane_store,
        "zones": zones,
        "cfg": cfg,
    }


def test_delays_always_have_a_reason_and_lane_delays_stay_on_their_lane(history) -> None:
    moves = history["tables"]["fact_truck_moves"].filter("status = 'ARRIVED'").collect()
    assert moves
    for row in moves:
        late = (row.eta - row.planned_arrival_time).total_seconds() / 60
        assert late == pytest.approx(row.delay_minutes)
        assert (row.delay_minutes > 0) == (row.delay_reason is not None)
        assert row.load_units > 0
    lane = [row for row in moves if row.delay_reason == "LANE_DELAY"]
    assert lane
    assert {(row.dc_id, row.store_id) for row in lane} == {
        (history["lane_dc"], history["lane_store"])
    }
    assert all(row.delay_minutes >= 120 for row in lane)
    reasons = {row.delay_reason for row in moves if row.delay_reason}
    assert reasons <= {"LANE_DELAY", "TRAFFIC", "WEATHER", "DC_OUTAGE", "HOLIDAY_CAPACITY"}
    late_share = sum(row.delay_minutes >= cal.TRUCK_LATE_MINUTES for row in moves) / len(moves)
    print(f"STAT history late-truck share={late_share:.3f} of {len(moves)} arrivals")


def test_a_supplier_short_ship_empties_its_dc_and_its_stores_shelves(history) -> None:
    tables, short_dc = history["tables"], history["short_dc"]
    affected = set(history["by_dc"][short_dc])
    others = {
        store for dc, stores in history["by_dc"].items() if dc != short_dc for store in stores
    }
    stockouts = (
        tables["fact_stockouts"]
        .filter(F.col("event_ts").between(SHORT_FROM, SHORT_TO))
        .select("StoreID", "DCID", "ProductID")
        .collect()
    )
    assert short_dc in {int(row.DCID) for row in stockouts if row.DCID is not None}

    # A store-SKU that cannot be resupplied stays out, so count the store-SKUs
    # that ran out rather than stockout events.
    def per_store(stores: set[int]) -> float:
        pairs = {
            (int(row.StoreID), row.ProductID)
            for row in stockouts
            if row.StoreID is not None and int(row.StoreID) in stores
        }
        return len(pairs) / len(stores)

    hit, unaffected = per_store(affected), per_store(others)
    print(
        f"STAT store-SKUs out during the short-ship per store: affected={hit:.1f} "
        f"unaffected={unaffected:.1f}"
    )
    assert hit > 3 * unaffected + 2
    # Late in the incident the DC loads far less than its stores ask for.
    late = SHORT_TO - timedelta(days=14)
    asked = (
        tables["fact_reorders"]
        .filter((F.col("dc_id") == short_dc) & F.col("event_ts").between(late, SHORT_TO))
        .agg(F.sum("reorder_quantity"))
        .first()[0]
    )
    loaded = (
        tables["fact_dc_inventory_txn"]
        .filter(
            (F.col("dc_id") == short_dc)
            & (F.col("txn_type") == "OUTBOUND_SHIPMENT")
            & F.col("event_ts").between(late, SHORT_TO + timedelta(days=1))
        )
        .agg(F.sum(-F.col("quantity")))
        .first()[0]
    )
    print(f"STAT short-ship DC loaded {loaded} of {asked} units asked")
    assert loaded < 0.6 * asked


def test_dc_ledgers_conserve_stock_and_never_go_negative(history) -> None:
    dc = history["tables"]["fact_dc_inventory_txn"]
    running = Window.partitionBy("dc_id", "product_id").orderBy("event_ts", "trace_id")
    checked = dc.withColumn(
        "_expected",
        F.sum("quantity").over(running.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
    ).agg(
        F.count(F.when(F.col("balance") != F.col("_expected"), 1)).alias("drift"),
        F.count(F.when(F.col("balance") < 0, 1)).alias("negative"),
        F.countDistinct(
            F.when(F.col("txn_type") == "INITIAL", F.struct("dc_id", "product_id"))
        ).alias("opened"),
        F.countDistinct("dc_id", "product_id").alias("keys"),
    )
    row = checked.first()
    assert (row.drift, row.negative) == (0, 0)
    assert row.opened == row.keys
    kinds = {r.txn_type for r in dc.select("txn_type").distinct().collect()}
    assert {"INITIAL", "INBOUND_SHIPMENT", "OUTBOUND_SHIPMENT"} <= kinds


def test_incident_tables_list_every_affected_store(history) -> None:
    tables = history["tables"]
    headers = {row.incident_id: row for row in tables["fact_logistics_incidents"].collect()}
    links = tables["fact_logistics_incident_stores"].collect()
    assert "INC-LANE-TEST" in headers and headers["INC-LANE-TEST"].store_id == history["lane_store"]
    for incident_id, header in headers.items():
        stores = {row.store_id for row in links if row.incident_id == incident_id}
        assert len(stores) == header.affected_store_count
    short = [h for h in headers.values() if h.incident_type == "SUPPLIER_SHORT_SHIP"]
    assert short and all(h.resolved_at is not None for h in short)


def test_a_simple_dwell_model_beats_the_mean_by_far(spark, history) -> None:
    """Notebook 13's features (load size, site, hour, lateness) predict the dwell."""
    zones = spark.createDataFrame(sorted(history["zones"].items()), "store_id long, _tz string")
    rows = (
        history["tables"]["fact_truck_moves"]
        .filter("status = 'COMPLETED'")
        .join(zones, "store_id")
        .withColumn("_hour", F.hour(F.from_utc_timestamp("eta", F.col("_tz"))))
        .withColumn("_at", F.unix_timestamp("eta"))
        .collect()
    )
    assert len(rows) > 50
    stores = sorted({row.store_id for row in rows})

    def features(row) -> list[float]:
        hour = row._hour
        busy, night = cal.TRUCK_UNLOAD_BUSY_HOURS, cal.TRUCK_UNLOAD_NIGHT_HOURS
        factor = (
            cal.TRUCK_UNLOAD_BUSY_FACTOR
            if busy[0] <= hour < busy[1]
            else cal.TRUCK_UNLOAD_NIGHT_FACTOR
            if hour >= night[0] or hour < night[1]
            else 1.0
        )
        site = [float(row.store_id == store) for store in stores]
        site_units = [row.load_units * factor * flag for flag in site]
        return [*site, *site_units, float(row.delay_minutes >= cal.TRUCK_LATE_MINUTES)]

    x = np.array([features(row) for row in rows])
    y = np.array([row.actual_unload_duration for row in rows])
    order = np.argsort([row._at for row in rows])
    split = int(len(rows) * 0.7)
    train, test = order[:split], order[split:]
    coef, *_ = np.linalg.lstsq(x[train], y[train], rcond=None)
    model_mae = float(np.mean(np.abs(x[test] @ coef - y[test])))
    mean_mae = float(np.mean(np.abs(y[train].mean() - y[test])))
    print(f"STAT dwell MAE model={model_mae:.2f} mean={mean_mae:.2f} min")
    assert model_mae < TARGET_MAE
    # The noise (a stable +/- TRUCK_UNLOAD_NOISE_MINUTES) bounds how far it can win.
    assert model_mae < 0.75 * mean_mae
