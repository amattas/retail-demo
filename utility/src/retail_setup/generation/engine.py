"""Orchestrates full generation. Returns DataFrames; writing happens in 2c."""

from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import DictionarySet
from retail_setup.generation import (
    dims as dims_mod,
    inventory,
    marketing,
    online_orders,
    promotions,
    receipts as receipts_mod,
    returns as returns_mod,
    sensors,
    store_activity,
)


@dataclass
class GenerationResult:
    tables: dict[str, DataFrame]


def _shift_year(d: date, years: int) -> date:
    """Shift a date by whole years; Feb 29 falls back to Feb 28."""
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        return date(d.year + years, d.month, 28)


def generate_all(
    spark: SparkSession, dicts: DictionarySet, cfg: GenerationConfig
) -> GenerationResult:
    t: dict[str, DataFrame] = {}
    t.update(dims_mod.generate_dimensions(spark, dicts, cfg))
    t["dim_date"] = dims_mod.generate_dim_date(
        spark, _shift_year(cfg.start_date, -5), _shift_year(cfg.end_date, 5))

    sales = receipts_mod.generate_receipts_group(spark, t, dicts.profile, cfg)
    # fact_receipts/lines (SALE-only) each feed several independent builders —
    # returns, promotions, foot traffic, BLE, inventory — plus the SALE/RETURN
    # unions below. Persist them so this shared, expensive lineage (xxhash draws
    # + line explode) is computed once instead of once per consumer. Generation
    # is fully deterministic, so a cached frame is byte-identical to a recomputed
    # one: realism is unchanged, only the redundant recomputation is removed.
    sales["fact_receipts"] = sales["fact_receipts"].cache()
    sales["fact_receipt_lines"] = sales["fact_receipt_lines"].cache()
    rets = returns_mod.generate_returns(spark, sales, t, cfg)
    t["fact_receipts"] = sales["fact_receipts"].unionByName(rets["fact_receipts"])
    t["fact_receipt_lines"] = sales["fact_receipt_lines"].unionByName(
        rets["fact_receipt_lines"])

    online = online_orders.generate_online_orders(spark, t, dicts.profile, cfg)
    t["fact_online_order_headers"] = online["fact_online_order_headers"]
    t["fact_online_order_lines"] = online["fact_online_order_lines"]
    # single-writer union for the shared payments table (2a carry-note)
    t["fact_payments"] = (sales["fact_payments"]
                          .unionByName(rets["fact_payments"])
                          .unionByName(online["payments"]))

    promos, promo_lines = promotions.generate_promotions(spark, sales)
    t["fact_promotions"], t["fact_promo_lines"] = promos, promo_lines
    t["fact_marketing"] = marketing.generate_marketing(spark, t, cfg)
    t["fact_store_ops"] = store_activity.generate_store_ops(spark, t, cfg)
    t["fact_foot_traffic"] = store_activity.generate_foot_traffic(
        spark, sales["fact_receipts"], t, cfg)
    pings, zc = sensors.generate_ble(spark, sales["fact_receipts"], t, cfg)
    t["fact_ble_pings"], t["fact_customer_zone_changes"] = pings, zc
    t.update(inventory.generate_inventory_chain(spark, sales, rets, t, cfg))
    # Downstream the driver runs run_invariants (50+ count/join/distinct actions
    # over these frames) and then write_all (one write + count per table).
    # Without caching, every one of those actions re-executes the full generation
    # DAG from scratch — the dominant cost of the setup run. Persist each table so
    # it materializes exactly once (on the first invariant pass) and all later
    # reads hit the cache. Deterministic generation ⇒ cached == recomputed, so
    # the simulation output is identical. cache() is MEMORY_AND_DISK, so large
    # frames spill to local SSD rather than failing under memory pressure.
    for name in t:
        t[name] = t[name].cache()
    return GenerationResult(tables=t)
