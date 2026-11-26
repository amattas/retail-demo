"""Every history ends inside the end-of-history DC outage (Spark).

Small histories of each preset (and a slow, custom-like hardware layout) end
with the outage still open, and the store shelves it starves give the
Operations Agent real positions to flag: notebook 12's current-position
features (its own functions, executed here) put some store/product positions
under the agent's three-day cover rule, and they sit at the outage's stores.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pytest
from pyspark.sql import functions as F

from deploy.scripts._operations_scenario import meets_alert_rule, require_alertable
from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.fulfillment import resolve_fulfillment
from retail_setup.generation.incidents import end_of_history_incident
from retail_setup.generation.inventory import generate_inventory_chain
from retail_setup.generation.network import incident_network
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.timezones import localize_by_store
from tests.generation.test_ml_notebook_runtime import _load

END = date(2025, 8, 17)
DAYS = 35


@dataclass(frozen=True)
class Case:
    store_type: str
    stores: int
    dcs: int = 1
    # None: the preset's own volume.
    transactions_per_store_day: int | None = None


CASES = {
    # The busy presets run at reduced volume to keep the test fast; they
    # still have many SKUs that sell several units a day.
    "grocery": Case("grocery", 2, transactions_per_store_day=120),
    "hardware": Case("hardware", 2),
    # Two DCs: the outage hits one DC's stores only.
    "supercenter": Case("supercenter", 4, dcs=2, transactions_per_store_day=120),
    # Luxury sells slowly: about one SKU per store moves two units a day.
    "luxury": Case("luxury", 4),
    # A custom-like hardware layout: few stores, a third of the preset volume.
    "hardware-slow": Case("hardware", 3, transactions_per_store_day=60),
}


def _history(spark, case: Case) -> dict:
    dicts = load_dictionaries(default_dictionary_root(), case.store_type)
    cfg = GenerationConfig(
        store_type=case.store_type,
        start_date=END - timedelta(days=DAYS - 1),
        end_date=END,
        store_count=case.stores,
        dc_count=case.dcs,
        customer_count=300,
        seed=29,
        transactions_per_store_day=case.transactions_per_store_day
        or int(dicts.profile.resolved_transactions_per_store_day()),
    )
    # Eager checkpoints, as the engine checkpoints stage outputs.
    dims = {
        name: frame.localCheckpoint()
        for name, frame in generate_dimensions(spark, dicts, cfg).items()
    }
    sales = {
        name: frame.localCheckpoint()
        for name, frame in generate_receipts_group(spark, dims, dicts.profile, cfg).items()
    }
    fulfilled = resolve_fulfillment(sales, dims, cfg)
    fulfilled.sales = {name: frame.localCheckpoint() for name, frame in fulfilled.sales.items()}
    fulfilled.events = fulfilled.events.localCheckpoint()
    tables = {
        name: frame.localCheckpoint()
        for name, frame in generate_inventory_chain(
            spark, fulfilled.sales, {}, dims, cfg, fulfillment=fulfilled
        ).items()
    }
    return {
        "cfg": cfg,
        "dims": dims,
        "tables": tables,
        # Published receipts are UTC, as the ledger is (engine._build_attributed).
        "receipts": localize_by_store(fulfilled.sales["fact_receipts"], dims, ["event_ts"]),
        "lines": fulfilled.sales["fact_receipt_lines"],
    }


def _current_positions(history: dict):
    """Notebook 12's current positions and demand features at its as-of."""
    nb12 = _load(
        "12-ml-stockout-prediction.ipynb",
        {"build_end_of_day_inventory", "roll_forward_current_positions", "build_snapshot_features"},
        LOOKBACK_DAYS=30,
    )
    receipts = history["receipts"]
    as_of = receipts.agg(F.max("event_ts")).first()[0]
    ledger = (
        history["tables"]["fact_store_inventory_txn"]
        .select("store_id", "product_id", "event_ts", "trace_id", F.col("balance").cast("double"))
        .filter(F.col("event_ts") <= F.lit(as_of))
        .withColumn("event_date", F.to_date("event_ts"))
    )
    sales = (
        history["lines"]
        .select("receipt_id_ext", "product_id", F.col("quantity").cast("double"))
        .join(
            receipts.select(
                "receipt_id_ext",
                "store_id",
                "receipt_type",
                F.col("event_ts").alias("sales_event_ts"),
            ),
            "receipt_id_ext",
        )
        .filter(
            (F.col("receipt_type") == "SALE")
            & (F.col("quantity") > 0)
            & (F.col("sales_event_ts") <= F.lit(as_of))
        )
        .withColumn("sales_event_date", F.to_date("sales_event_ts"))
    )
    products = history["dims"]["dim_products"].select(F.col("ID").alias("product_id"))
    current = nb12["roll_forward_current_positions"](
        nb12["build_end_of_day_inventory"](ledger), as_of
    )
    features = nb12["build_snapshot_features"](current, sales, products)
    return as_of, features.select(
        "store_id", "product_id", "current_inventory", "demand_velocity_daily", "days_of_inventory"
    ).collect()


@pytest.mark.parametrize("name", list(CASES))
def test_history_ends_inside_the_outage_with_positions_under_three_days(spark, name) -> None:
    history = _history(spark, CASES[name])
    cfg, dims, tables = history["cfg"], history["dims"], history["tables"]
    network = incident_network(
        dims["dim_stores"], dims["dim_geographies"], dims["dim_distribution_centers"]
    )
    expected = end_of_history_incident(cfg.seed, network, cfg.end_date)
    assert expected is not None
    affected = set(expected.affected_stores)
    if CASES[name].dcs > 1:
        assert affected < set(network.store_dc), "the outage must hit one DC's stores only"

    # The outage is published open: NULL resolved_at, one bridge row per store.
    header = [
        row
        for row in tables["fact_logistics_incidents"].collect()
        if row.incident_id == expected.incident_id
    ]
    assert len(header) == 1
    assert (header[0].incident_type, header[0].dc_id, header[0].resolved_at) == (
        "DC_OUTAGE",
        expected.dc_id,
        None,
    )
    assert header[0].severity == "HIGH" and header[0].affected_store_count == len(affected)
    bridge = [
        row
        for row in tables["fact_logistics_incident_stores"].collect()
        if row.incident_id == expected.incident_id
    ]
    assert {row.store_id for row in bridge} == affected
    assert all(row.resolved_at is None for row in bridge)

    as_of, positions = _current_positions(history)
    # scenario_sql's incident join: open at the risk snapshot's as-of.
    assert all(row.started_at <= as_of for row in bridge)
    signals = [
        {"stockout_probability": None, "days_of_inventory": row.days_of_inventory, **row.asDict()}
        for row in positions
    ]
    alertable = [signal for signal in signals if meets_alert_rule(signal)]
    at_outage = [signal for signal in alertable if signal["store_id"] in affected]
    print(
        f"STAT {name}: {len(alertable)} of {len(positions)} positions under 3 days of cover "
        f"({len(at_outage)} at the {len(affected)} outage stores); as-of {as_of}"
    )
    assert require_alertable(at_outage) >= 1
    # The starved shelves are the outage's: elsewhere the policy keeps cover.
    others = set(network.store_dc) - affected
    if others:
        per_store = len(at_outage) / len(affected)
        elsewhere = (len(alertable) - len(at_outage)) / len(others)
        assert per_store >= 5 * elsewhere
