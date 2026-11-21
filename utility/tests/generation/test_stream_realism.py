"""Live stream vs history: volume, hours, conversion, identities, payments,
store inventory, trucks and online order lifecycles.

Runs the inlined stream functions offline over simulated micro-batches (an
in-memory state store, the same code the notebook runs per foreachBatch) and
compares the result with a small generated history over the same dates. One
simulation feeds every assertion; it is the slow part of this module.
"""

from __future__ import annotations

import json
import re
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from statistics import mean
from zoneinfo import ZoneInfo

import pytest

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import load_dictionaries
from retail_setup.generation import vocabulary as vocab
from retail_setup.generation.calibration import (
    REFERENCE_STORE_TYPE,
    STOCKOUT_QUANTITY_THRESHOLD,
    TRUCK_LOAD_MINUTES,
)
from retail_setup.generation.stream_calendar import operating_window, plan_lanes
from retail_setup.generation.stream_state import (
    SEED_BATCH_ID,
    MemoryStateStore,
    seed_store_state,
)

pytestmark = pytest.mark.spark

START = date(2025, 3, 3)
END = date(2025, 3, 6)
STORES = 6
TRANSACTIONS = 60
BATCH = timedelta(hours=24)
# notebook 03 replays this far behind its arrival watermark
REPLAY_OVERLAP = timedelta(hours=3)
# Seeded store stock: low enough that shelves empty, reorders ship and trucks
# unload within the simulated days.
SEED_STOCK_MODULUS = 25
SEED_RATE = 1.5
NAMESPACE = "sim-0a1b2c3d"
REPLAYED_BATCHES = {2}
CODEGEN_OFF = {
    "spark.sql.codegen.wholeStage": "false",
    "spark.sql.codegen.factoryMode": "NO_CODEGEN",
}


def _rate_rows(spark, start: datetime, end: datetime, rows_per_second: float, first: int):
    """Rate-source rows (ts, v) for [start, end) at a fractional rate."""
    from pyspark.sql import functions as F

    step_ms = 1000.0 / rows_per_second
    count = int((end - start).total_seconds() * 1000 / step_ms)
    start_ms = int(start.timestamp() * 1000)
    return spark.range(first, first + count).select(
        F.timestamp_millis(
            (F.lit(start_ms) + F.floor((F.col("id") - first) * step_ms)).cast("long")
        ).alias("ts"),
        F.col("id").alias("v"),
    ), first + count


@pytest.fixture(scope="module")
def world(spark):
    # The stream's wide expressions spend longer in code generation than in
    # execution on these few rows; interpreting them keeps the test quick.
    previous = {key: spark.conf.get(key, None) for key in CODEGEN_OFF}
    for key, value in CODEGEN_OFF.items():
        spark.conf.set(key, value)
    try:
        yield _simulate(spark)
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def _simulate(spark):
    from pyspark.sql import functions as F

    from retail_setup.generation.baskets import basket_products
    from retail_setup.generation.customers import lifecycle_for
    from retail_setup.generation.dims import generate_dimensions
    from retail_setup.generation.network import dc_fleets, store_dc_map
    from retail_setup.generation.receipts import generate_receipts_group
    from retail_setup.generation.sensors import generate_ble
    from retail_setup.generation.stream_baskets import prepare_basket_catalog
    from retail_setup.generation.stream_events import (
        build_stream_context,
        commit_stream_batch,
        generate_stream_batch,
    )
    from retail_setup.generation.stream_frames import dense_id_lookup
    from retail_setup.generation.stream_supply import SupplyNetwork
    from retail_setup.generation.traffic import generate_hourly_visits

    cfg = GenerationConfig(
        store_type=REFERENCE_STORE_TYPE,
        start_date=START,
        end_date=END,
        store_count=STORES,
        dc_count=1,
        seed=11,
        transactions_per_store_day=TRANSACTIONS,
    )
    dictionaries = load_dictionaries(cfg.resolved_dictionary_root, cfg.store_type)
    profile = dictionaries.profile
    dims = {
        name: frame.cache() for name, frame in generate_dimensions(spark, dictionaries, cfg).items()
    }

    visits = generate_hourly_visits(spark, dims["dim_stores"], profile, cfg).collect()
    sales = generate_receipts_group(spark, dims, profile, cfg)
    payments = sales["fact_payments"].select("payment_method", "status").collect()
    pings, _changes = generate_ble(spark, sales["fact_receipts"], dims, cfg)
    ping_rows = pings.select("zone", "customer_id").collect()

    ids = {
        "stores": dense_id_lookup(dims["dim_stores"]),
        "customers": dense_id_lookup(dims["dim_customers"]),
        "products": dense_id_lookup(dims["dim_products"]),
        "dcs": dense_id_lookup(dims["dim_distribution_centers"]),
        "trucks": dense_id_lookup(dims["dim_trucks"]),
    }
    products = basket_products(dims["dim_products"], cfg.seed)
    store_rows = dims["dim_stores"].collect()
    plan = plan_lanes(
        profile,
        store_count=STORES,
        multiplier_total=sum(row["daily_traffic_multiplier"] for row in store_rows),
        operating_hours=[row["operating_hours"] for row in store_rows],
        transactions_per_store_day=TRANSACTIONS,
        online_orders_per_day=cfg.online_orders_per_day,
        marketing_impressions_per_day=sum(entry[2] for entry in vocab.CAMPAIGN_ARCHETYPES),
    )
    # Keep the simulation small: run at the peak rate.
    rate = sum(plan.peaks.values())
    shares = {lane: share * plan.rows_per_second / rate for lane, share in plan.shares.items()}
    lanes = replace(plan, rows_per_second=rate, shares=shares)
    store_dc = {
        row["store_id"]: row["dc_id"]
        for row in store_dc_map(
            dims["dim_stores"], dims["dim_geographies"], dims["dim_distribution_centers"]
        ).collect()
    }
    trucks = dims["dim_trucks"].select("ID", "DCID").collect()
    fleets = dc_fleets(
        ((row["ID"], row["DCID"]) for row in trucks),
        [row["ID"] for row in dims["dim_distribution_centers"].collect()],
    )
    network = SupplyNetwork(
        seed=cfg.seed,
        namespace=NAMESPACE,
        store_dc=store_dc,
        fleets=fleets,
        daily_prior=TRANSACTIONS * 10.0 / dims["dim_products"].count(),
    )
    stock = (
        dims["dim_stores"]
        .select(F.col("ID").cast("long").alias("store_id"))
        .crossJoin(dims["dim_products"].select(F.col("ID").cast("long").alias("product_id")))
        .select(
            "store_id",
            "product_id",
            F.pmod(F.col("product_id") * 7 + F.col("store_id"), F.lit(SEED_STOCK_MODULUS))
            .cast("long")
            .alias("on_hand"),
            F.lit(SEED_RATE).alias("rate"),
        )
        .persist()
    )
    ctx = build_stream_context(
        spark,
        seed=cfg.seed,
        namespace=NAMESPACE,
        source="test",
        profile=profile,
        history_start=START,
        dim_stores=dims["dim_stores"],
        dim_customers=dims["dim_customers"],
        ids=ids,
        baskets=prepare_basket_catalog(spark, products, profile, cfg.seed, START, cfg.anchor_date),
        lanes=lanes,
        supply=network,
        lifecycle=lifecycle_for(cfg, profile.resolved_customer_visits_per_year()),
        inventory_seed=stock,
        anchor_date=cfg.anchor_date,
        annual_volume_growth=cfg.annual_volume_growth,
    )

    state = MemoryStateStore(NAMESPACE)
    begin = datetime(START.year, START.month, START.day, tzinfo=timezone.utc) - timedelta(hours=12)
    state.write("stores", SEED_BATCH_ID, seed_store_state(ctx.store_hours, {}, begin))
    end = datetime(END.year, END.month, END.day, tzinfo=timezone.utc) + timedelta(days=1, hours=12)
    batches, first, clock, batch_id = [], 0, begin, 0
    while clock < end:
        rows, first = _rate_rows(spark, clock, clock + BATCH, rate, first)
        batch = generate_stream_batch(ctx, rows, batch_id, state)
        values = sorted(row["value"] for row in batch.events.collect())
        if batch_id in REPLAYED_BATCHES:
            # A replay before the commit regenerates the batch byte for byte.
            again = generate_stream_batch(ctx, rows, batch_id, state)
            assert sorted(row["value"] for row in again.events.collect()) == values
            assert again.changes == batch.changes
            again.release()
        events = [json.loads(value) for value in values]
        commit_stream_batch(state, batch_id, batch)
        batch.release()
        batches.append((batch_id, events, batch.clock_ms))
        clock += BATCH
        batch_id += 1

    zones = {row["ID"]: ZoneInfo(row["TimeZone"]) for row in store_rows}
    return {
        "cfg": cfg,
        "profile": profile,
        "stores": {row["ID"]: row for row in store_rows},
        "zones": zones,
        "visits": visits,
        "payments": payments,
        "pings": ping_rows,
        "ble_ids": {row["BLEId"] for row in dims["dim_customers"].select("BLEId").collect()},
        "batches": batches,
        "events": [event for _, events, _ in batches for event in events],
        "state": state,
        "network": network,
        "truck_dc": {row["ID"]: row["DCID"] for row in trucks},
        "store_bps": {row["ID"]: round(row["tax_rate"] * 10000) for row in store_rows},
        "batch_count": batch_id,
    }


def _of(world, event_type: str) -> list[dict]:
    return [event for event in world["events"] if event["event_type"] == event_type]


def _local(world, store_id: int, iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(world["zones"][store_id])


def _in_history(world, store_id: int, iso: str) -> bool:
    return START <= _local(world, store_id, iso).date() <= END


def test_receipts_per_store_day_match_history(world) -> None:
    history: dict[tuple[int, date], int] = {}
    for row in world["visits"]:
        history[(row["store_id"], row["day"])] = history.get(
            (row["store_id"], row["day"]), 0
        ) + int(row["n_receipts"])
    live: dict[tuple[int, date], int] = {key: 0 for key in history}
    for event in _of(world, "receipt_created"):
        payload = event["payload"]
        if _in_history(world, payload["store_id"], event["ingest_timestamp"]):
            day = _local(world, payload["store_id"], event["ingest_timestamp"]).date()
            live[(payload["store_id"], day)] += 1
    ratio = mean(live.values()) / mean(history.values())
    print(
        f"STAT receipts/store-day live={mean(live.values()):.1f} history={mean(history.values()):.1f}"
    )
    assert 0.85 <= ratio <= 1.15, (mean(live.values()), mean(history.values()))


def test_hourly_curve_follows_local_store_hours(world) -> None:
    history = [0] * 24
    for row in world["visits"]:
        history[row["hour"]] += int(row["n_receipts"])
    live = [0] * 24
    for event in _of(world, "receipt_created"):
        payload = event["payload"]
        local = _local(world, payload["store_id"], event["ingest_timestamp"])
        open_hour, close_hour = operating_window(
            world["stores"][payload["store_id"]]["operating_hours"]
        )
        assert open_hour <= local.hour < close_hour, (payload["store_id"], local)
        if START <= local.date() <= END:
            live[local.hour] += 1
    hist_mean, live_mean = mean(history), mean(live)
    covariance = sum((h - hist_mean) * (x - live_mean) for h, x in zip(history, live))
    spread = (
        sum((h - hist_mean) ** 2 for h in history) * sum((x - live_mean) ** 2 for x in live)
    ) ** 0.5
    print(f"STAT hourly correlation={covariance / spread:.3f}")
    assert covariance / spread >= 0.9, (history, live)


def test_foot_traffic_converts_like_history(world) -> None:
    history = sum(int(r["n_receipts"]) for r in world["visits"]) / sum(
        int(r["visitors"]) for r in world["visits"]
    )
    entered = sum(
        e["payload"]["customer_count"]
        for e in _of(world, "customer_entered")
        if _in_history(world, e["payload"]["store_id"], e["ingest_timestamp"])
    )
    receipts = sum(
        1
        for e in _of(world, "receipt_created")
        if _in_history(world, e["payload"]["store_id"], e["ingest_timestamp"])
    )
    print(f"STAT conversion live={receipts / entered:.3f} history={history:.3f}")
    assert 0.18 <= receipts / entered <= 0.26, (receipts, entered, history)
    assert 0.18 <= history <= 0.26


def test_live_ble_ids_resolve_at_least_as_often_as_history(world) -> None:
    history = sum(row["customer_id"] is not None for row in world["pings"]) / len(world["pings"])
    pings = _of(world, "ble_ping_detected")
    resolved = [p for p in pings if p["payload"]["customer_ble_id"] in world["ble_ids"]]
    anonymous = [p for p in pings if p["payload"]["customer_ble_id"].startswith("ANON-")]
    assert len(resolved) + len(anonymous) == len(pings)
    print(f"STAT ble resolved live={len(resolved) / len(pings):.3f} history={history:.3f}")
    assert len(resolved) / len(pings) >= history - 0.05, (len(resolved) / len(pings), history)


def test_live_vocabularies_equal_history(world) -> None:
    tenders = {
        e["payload"]["payment_method"]
        for e in _of(world, "payment_processed")
        if e["payload"].get("receipt_id")
    }
    history_tenders = {row["payment_method"] for row in world["payments"]}
    assert tenders == history_tenders == set(vocab.tender_names())
    ble_zones = {e["payload"]["zone"] for e in _of(world, "ble_ping_detected")}
    # Both producers use the store type's own zones (profile.zones).
    profile_zones = set(vocab.ble_zones_for_profile(world["profile"]))
    assert ble_zones == {row["zone"] for row in world["pings"]} == profile_zones
    sensor_zones = {e["payload"]["zone"] for e in _of(world, "customer_entered")}
    assert sensor_zones <= set(vocab.FOOT_TRAFFIC_ZONES)
    for event in _of(world, "ble_ping_detected"):
        payload = event["payload"]
        assert payload["beacon_id"] == vocab.beacon_id(payload["store_id"], payload["zone"])
    for event in _of(world, "customer_entered"):
        payload = event["payload"]
        assert payload["sensor_id"] == vocab.sensor_id(payload["store_id"], payload["zone"])
    impressions = _of(world, "ad_impression")
    assert impressions
    assert {e["payload"]["channel"] for e in impressions} <= set(vocab.MARKETING_CHANNELS)
    assert {e["payload"]["device_type"] for e in impressions} <= set(vocab.DEVICE_TYPES)
    for event in impressions:
        payload = event["payload"]
        assert re.fullmatch(r"CAMP\d{8}0[1-4]", payload["campaign_id"]), payload
        assert re.fullmatch(r"AD\d{8,}", payload["customer_ad_id"]), payload
        lo, hi = vocab.CHANNEL_COSTS[payload["channel"]]
        assert lo * 0.8 - 0.01 <= payload["cost"] <= hi * 1.2 + 0.01, payload
    codes = {e["payload"]["promo_code"] for e in _of(world, "promotion_applied")}
    assert codes <= {c for c, *_ in vocab.PROMO_CATALOG}


def test_store_payments_decline_at_the_historical_rate(world) -> None:
    payments = [
        e["payload"] for e in _of(world, "payment_processed") if e["payload"].get("receipt_id")
    ]
    expected = sum(
        weight * vocab.decline_probability(name) for name, weight, *_ in vocab.TENDERS
    ) / sum(weight for _, weight, *_ in vocab.TENDERS)
    declined = sum(p["status"] == "DECLINED" for p in payments) / len(payments)
    history = sum(row["status"] == "DECLINED" for row in world["payments"]) / len(world["payments"])
    print(f"STAT declines live={declined:.4f} expected={expected:.4f} history={history:.4f}")
    assert abs(declined - expected) <= 0.012, (declined, expected, history)
    assert all(
        p["decline_reason"] in vocab.DECLINE_REASONS for p in payments if p["status"] == "DECLINED"
    )
    receipts = {e["payload"].get("receipt_id") for e in _of(world, "receipt_created")}
    assert {p["receipt_id"] for p in payments} <= receipts


def test_purchase_money_reconciles_across_events(world) -> None:
    receipts = {e["payload"]["receipt_id"]: e["payload"] for e in _of(world, "receipt_created")}
    payments = {
        e["payload"]["receipt_id"]: e["payload"]
        for e in _of(world, "payment_processed")
        if e["payload"].get("receipt_id")
    }
    promotions: dict[str, int] = {}
    for event in _of(world, "promotion_applied"):
        payload = event["payload"]
        assert payload["product_ids"] == sorted(set(payload["product_ids"]))
        promotions[payload["receipt_id"]] = (
            promotions.get(payload["receipt_id"], 0) + payload["discount_cents"]
        )
    lines: dict[str, int] = {}
    for event in _of(world, "receipt_line_added"):
        payload = event["payload"]
        lines[payload["receipt_id"]] = lines.get(payload["receipt_id"], 0) + round(
            payload["extended_price"] * 100
        )
    assert receipts
    for receipt_id, receipt in receipts.items():
        assert (
            receipt["gross_subtotal_cents"] - receipt["discount_cents"] == receipt["subtotal_cents"]
        )
        assert receipt["subtotal_cents"] + receipt["tax_cents"] == receipt["total_cents"]
        assert promotions.get(receipt_id, 0) == receipt["discount_cents"]
        assert lines[receipt_id] == receipt["subtotal_cents"]
        if receipt_id in payments:
            assert payments[receipt_id]["amount_cents"] == receipt["total_cents"]
    # Each payment follows its receipt by the tender's processing time.
    times = {
        e["payload"]["receipt_id"]: e["ingest_timestamp"] for e in _of(world, "receipt_created")
    }
    for receipt_id, payment in payments.items():
        if receipt_id in times:
            assert payment["processing_time"] > times[receipt_id]


def test_journey_touches_arrive_inside_the_silver_replay_window(world) -> None:
    newest_seen = None
    touches = 0
    for _batch_id, events, _clock in world["batches"]:
        arrivals = [e["ingest_timestamp"] for e in events]
        for event in events:
            if event["event_type"] != "ad_impression" or not event.get("correlation_id"):
                continue
            touches += 1
            arrival = datetime.fromisoformat(event["ingest_timestamp"].replace("Z", "+00:00"))
            touched = datetime.fromisoformat(
                event["payload"]["impression_time"].replace("Z", "+00:00")
            )
            assert timedelta(seconds=60) <= arrival - touched <= timedelta(days=7)
            if newest_seen is not None:
                assert arrival >= newest_seen - REPLAY_OVERLAP
        if arrivals:
            batch_newest = max(datetime.fromisoformat(a.replace("Z", "+00:00")) for a in arrivals)
            newest_seen = batch_newest if newest_seen is None else max(newest_seen, batch_newest)
    assert touches > 0


def test_store_operations_pair_at_local_boundaries(world) -> None:
    ops = sorted(
        (e["payload"]["store_id"], e["payload"]["operation_time"], e["payload"]["operation_type"])
        for e in world["events"]
        if e["event_type"] in {"store_opened", "store_closed"}
    )
    assert ops
    for store_id, store in world["stores"].items():
        sequence = [op for sid, _, op in ops if sid == store_id]
        assert all(a != b for a, b in zip(sequence, sequence[1:])), sequence
        assert set(sequence) <= {"opened", "closed"}
        open_hour, close_hour = operating_window(store["operating_hours"])
        for sid, when, op in ops:
            if sid != store_id:
                continue
            local = _local(world, store_id, when)
            expected = (
                (open_hour, 0)
                if op == "opened"
                else ((23, 59) if close_hour == 24 else (close_hour, 0))
            )
            assert (local.hour, local.minute) == expected, (store_id, op, local)


# ---------------------------------------------------------------------------
# Store inventory, trucks and online orders (stream_supply, plan_orders)
# ---------------------------------------------------------------------------

MINUTE_MS = 60_000


def _ms(iso: str) -> int:
    return round(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def _seed_stock(store_id: int, product_id: int) -> int:
    return (product_id * 7 + store_id) % SEED_STOCK_MODULUS


def _ledger(world) -> dict[tuple[int, int], list[tuple[int, int, int]]]:
    """(ms, balance before, balance after) per store and product, from the live moves."""
    if "ledger" not in world:
        deltas: dict[tuple[int, int], dict[int, int]] = {}
        for event in _of(world, "inventory_updated"):
            payload = event["payload"]
            key = (payload["store_id"], payload["product_id"])
            at = _ms(event["ingest_timestamp"])
            deltas.setdefault(key, {})[at] = (
                deltas.setdefault(key, {}).get(at, 0) + payload["quantity_delta"]
            )
        ledger = {}
        for key, by_ms in deltas.items():
            balance, steps = _seed_stock(*key), []
            for at in sorted(by_ms):
                steps.append((at, balance, balance + by_ms[at]))
                balance += by_ms[at]
            ledger[key] = steps
        world["ledger"] = ledger
    return world["ledger"]


def test_no_event_is_emitted_before_its_time(world) -> None:
    for _batch_id, events, clock_ms in world["batches"]:
        assert all(_ms(e["ingest_timestamp"]) <= clock_ms for e in events)


def test_store_stock_never_goes_negative_and_reconciles_with_sales_and_unloads(world) -> None:
    ledger = _ledger(world)
    assert ledger
    assert all(after >= 0 for steps in ledger.values() for _at, _before, after in steps)
    inventory = world["state"].read("inventory", world["batch_count"])
    for key, entry in inventory.items():
        _, store, product = key.split(":")
        steps = ledger.get((int(store), int(product)))
        expected = steps[-1][2] if steps else _seed_stock(int(store), int(product))
        assert entry["on_hand"] == expected, key

    moves = [e["payload"] for e in _of(world, "inventory_updated")]
    network = world["network"]
    assert all(m["dc_id"] == network.dc_of(m["store_id"]) for m in moves)
    lost = [m for m in moves if m["reason"] == "LOST_SALE"]
    assert lost and all(m["quantity_delta"] == 0 and m["requested_quantity"] > 0 for m in lost)

    # Every unit a store sold is on an emitted receipt line or a store-fulfilled
    # online line, and no lost sale is.
    sold: dict[tuple[int, int], int] = {}
    for m in moves:
        if m["reason"] == "SALE":
            assert m["requested_quantity"] == -m["quantity_delta"] > 0
            key = (m["store_id"], m["product_id"])
            sold[key] = sold.get(key, 0) - m["quantity_delta"]
    receipt_store = {
        e["payload"]["receipt_id"]: e["payload"]["store_id"] for e in _of(world, "receipt_created")
    }
    cancelled = {
        e["payload"]["order_id"]
        for e in _of(world, "online_order_created")
        if e["payload"]["total_cents"] == 0
    }
    lines: dict[tuple[int, int], int] = {}
    for e in _of(world, "receipt_line_added"):
        key = (receipt_store[e["payload"]["receipt_id"]], e["payload"]["product_id"])
        lines[key] = lines.get(key, 0) + e["payload"]["quantity"]
    for e in _of(world, "online_order_line_added"):
        payload = e["payload"]
        if payload["node_type"] == "STORE" and payload["order_id"] not in cancelled:
            key = (payload["node_id"], payload["product_id"])
            lines[key] = lines.get(key, 0) + payload["quantity"]
    assert sold == lines
    print(
        f"STAT live lines lost={len(lost)} sold={sum(sold.values())} units, "
        f"{len(ledger)} store-products moved"
    )


def test_stockouts_fire_only_when_a_sale_empties_the_shelf(world) -> None:
    ledger = _ledger(world)
    stockouts = [e["payload"] for e in _of(world, "stockout_detected")]
    assert stockouts
    seen = set()
    for payload in stockouts:
        key = (payload["store_id"], payload["product_id"])
        at = _ms(payload["detection_time"])
        before, after = next((b, a) for t, b, a in ledger[key] if t == at)
        # Sales of one SKU in the same instant share a ledger step.
        assert after < STOCKOUT_QUANTITY_THRESHOLD <= payload["last_known_quantity"] <= before
        assert payload["dc_id"] == world["network"].dc_of(payload["store_id"])
        day = (key, payload["detection_time"][:10])
        assert day not in seen
        seen.add(day)
    for key, steps in ledger.items():
        crossings = [t for t, b, a in steps if b >= STOCKOUT_QUANTITY_THRESHOLD > a]
        reported = [
            _ms(p["detection_time"]) for p in stockouts if (p["store_id"], p["product_id"]) == key
        ]
        assert set(reported) <= set(crossings)
        assert len({t // 86_400_000 for t in crossings}) == len(reported), key
    print(f"STAT live stockouts={len(stockouts)}")


def test_reorders_ship_on_the_dc_fleet_and_unload_into_stock(world) -> None:
    network = world["network"]
    reorders = [e["payload"] for e in _of(world, "reorder_triggered")]
    assert reorders
    ordered: dict[tuple[int, int], int] = {}
    for r in reorders:
        assert r["reorder_quantity"] > 0 and r["reorder_quantity"] % 12 == 0
        assert r["current_quantity"] >= 0 and r["dc_id"] == network.dc_of(r["store_id"])
        assert (r["priority"] == "URGENT") == (r["current_quantity"] == 0)
        key = (r["store_id"], r["product_id"])
        ordered[key] = ordered.get(key, 0) + r["reorder_quantity"]

    arrivals = {e["payload"]["shipment_id"]: e["payload"] for e in _of(world, "truck_arrived")}
    departures = {e["payload"]["shipment_id"]: e["payload"] for e in _of(world, "truck_departed")}
    assert departures and set(departures) <= set(arrivals)
    received: dict[tuple[int, int], int] = {}
    for e in _of(world, "inventory_updated"):
        payload = e["payload"]
        if payload["reason"] == "INBOUND_SHIPMENT":
            departure = departures[payload["source"]]
            assert _ms(e["ingest_timestamp"]) == _ms(departure["departure_time"])
            key = (payload["store_id"], payload["product_id"])
            received[key] = received.get(key, 0) + payload["quantity_delta"]
    in_flight: dict[tuple[int, int], int] = {}
    logistics = world["state"].read("logistics", world["batch_count"])
    for key, payload in logistics.items():
        if key.startswith(("pending:", "shipment:")):
            store = int(payload.get("store_id", key.split(":")[-1]))
            for product, quantity in payload["lines"].items():
                in_flight[(store, int(product))] = (
                    in_flight.get((store, int(product)), 0) + quantity
                )
    for key, quantity in ordered.items():
        assert quantity == received.get(key, 0) + in_flight.get(key, 0), key

    by_truck: dict[str, list[dict]] = {}
    dwell = []
    for shipment_id, arrival in arrivals.items():
        truck = int(arrival["truck_id"][3:])
        home = world["truck_dc"][truck]
        assert home is None or int(home) == arrival["dc_id"]
        assert truck in network.fleets[arrival["dc_id"]]
        assert arrival["dc_id"] == network.dc_of(arrival["store_id"])
        assert arrival["planned_arrival_time"] == arrival["arrival_time"]
        # On time until the incident model adds delays: no reason (JSON omits nulls).
        assert arrival["delay_minutes"] == 0 and arrival.get("delay_reason") is None
        departure = departures.get(shipment_id)
        if departure is not None:
            minutes = (_ms(departure["departure_time"]) - _ms(arrival["arrival_time"])) / MINUTE_MS
            assert minutes == departure["actual_unload_duration"] > 0
            dwell.append(minutes)
        by_truck.setdefault(arrival["truck_id"], []).append({**arrival, "departure": departure})
    for trips in by_truck.values():
        trips.sort(key=lambda trip: _ms(trip["arrival_time"]))
        for previous, following in zip(trips, trips[1:]):
            assert previous["departure"] is not None
            back = (
                _ms(previous["departure"]["departure_time"])
                + (
                    network.transit_minutes(previous["store_id"])
                    + TRUCK_LOAD_MINUTES
                    + network.transit_minutes(following["store_id"])
                )
                * MINUTE_MS
            )
            assert _ms(following["arrival_time"]) >= back
    print(
        f"STAT trucks shipments={len(arrivals)} on {len(by_truck)} trucks, "
        f"mean dwell={mean(dwell):.1f} min, max dwell={max(dwell):.0f} min"
    )


def test_online_orders_follow_a_staged_lifecycle(world) -> None:
    created = {e["payload"]["order_id"]: e for e in _of(world, "online_order_created")}
    assert created
    lines: dict[str, list[dict]] = {}
    for e in _of(world, "online_order_line_added"):
        lines.setdefault(e["payload"]["order_id"], []).append(e["payload"])
    payments = {
        e["payload"]["order_id"]: e["payload"]
        for e in _of(world, "payment_processed")
        if e["payload"].get("order_id")
    }
    stages = {
        name: {e["payload"]["order_id"]: _ms(e["payload"][field]) for e in _of(world, name)}
        for name, field in (
            ("online_order_picked", "picked_time"),
            ("online_order_shipped", "shipped_time"),
            ("online_order_delivered", "delivered_time"),
        )
    }
    picked, shipped, delivered = stages.values()
    modes: dict[str, int] = {}
    for order_id, event in created.items():
        order = event["payload"]
        at = _ms(event["ingest_timestamp"])
        modes[order["fulfillment_mode"]] = modes.get(order["fulfillment_mode"], 0) + 1
        order_lines = lines[order_id]
        assert len(order_lines) == order["item_count"]
        assert {line["fulfillment_mode"] for line in order_lines} == {order["fulfillment_mode"]}
        assert order["gross_subtotal_cents"] - order["discount_cents"] == order["subtotal_cents"]
        assert order["subtotal_cents"] + order["tax_cents"] == order["total_cents"]
        if order["total_cents"] == 0:
            assert order_id not in payments and order_id not in picked
            continue
        assert (
            sum(round(line["extended_price"] * 100) for line in order_lines)
            == (order["subtotal_cents"])
        )
        assert order["tender_type"] in vocab.tender_names(vocab.ONLINE_TENDERS)
        assert payments[order_id]["amount_cents"] == order["total_cents"]
        assert payments[order_id].get("store_id") is None
        if order["node_type"] == "STORE":
            assert order["node_id"] in world["stores"]
        if order_id not in picked:
            continue
        bopis = order["fulfillment_mode"] == "BOPIS"
        low, high = vocab.ONLINE_PICK_SECONDS["BOPIS" if bopis else "SHIP"]
        assert low * 1000 <= picked[order_id] - at <= high * 1000
        if bopis:
            assert order_id not in shipped
            if order_id in delivered:
                assert delivered[order_id] > picked[order_id]
        else:
            if order_id in shipped:
                assert shipped[order_id] > picked[order_id]
            if order_id in delivered:
                assert delivered[order_id] > shipped[order_id]
    assert set(shipped) <= set(picked) and set(delivered) <= set(picked)
    assert delivered and any(
        created[o]["payload"]["fulfillment_mode"] == "BOPIS" for o in delivered
    )
    days = (END - START).days + 1
    in_window = sum(
        1
        for e in created.values()
        if START <= datetime.fromisoformat(e["ingest_timestamp"][:10]).date() <= END
    )
    expected = world["cfg"].online_orders_per_day * days
    ship_from_dc = modes.get("SHIP_FROM_DC", 0) / len(created)
    print(
        f"STAT online orders={in_window} expected~{expected} "
        f"delivered={len(delivered)} ship_from_dc={ship_from_dc:.2f}"
    )
    assert 0.6 * expected <= in_window <= 1.4 * expected
    assert 0.45 <= ship_from_dc <= 0.75
