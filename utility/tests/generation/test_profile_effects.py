from datetime import date

from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.receipts import generate_receipts_group


def _avg_ticket(spark, store_type: str) -> float:
    cfg = GenerationConfig(
        store_type=store_type,
        start_date=date(2025, 6, 2),
        end_date=date(2025, 6, 8),
        store_count=2,
        dc_count=1,
        customer_count=200,
        seed=11,
        transactions_per_store_day=30,
    )
    dicts = load_dictionaries(default_dictionary_root(), store_type)
    dims = generate_dimensions(spark, dicts, cfg)
    g = generate_receipts_group(spark, dims, dicts.profile, cfg)
    return g["fact_receipts"].agg(F.avg("total_cents")).first()[0] / 100.0


def test_luxury_ticket_dwarfs_grocery(spark):
    lux, gro = _avg_ticket(spark, "luxury"), _avg_ticket(spark, "grocery")
    assert lux > gro * 5, (lux, gro)


def test_luxury_boutiques_see_a_tenth_of_supercenter_traffic(spark):
    from retail_setup.generation.traffic import generate_hourly_visits

    def receipts_per_store_day(store_type: str) -> float:
        # Profile-derived volume: no transactions_per_store_day override.
        cfg = GenerationConfig(
            store_type=store_type,
            start_date=date(2025, 6, 2),
            end_date=date(2025, 6, 8),
            store_count=4,
            dc_count=1,
            seed=11,
        )
        dicts = load_dictionaries(default_dictionary_root(), store_type)
        dims = generate_dimensions(spark, dicts, cfg)
        grid = generate_hourly_visits(
            spark, dims["dim_stores"], dicts.profile, cfg, dims["dim_geographies"]
        )
        total = grid.agg(F.sum("n_receipts")).first()[0]
        multipliers = dims["dim_stores"].agg(F.sum("daily_traffic_multiplier")).first()[0]
        return total / 7 / multipliers

    luxury, supercenter = receipts_per_store_day("luxury"), receipts_per_store_day("supercenter")
    # Profiles: 40 vs 400 receipts per store-day at multiplier 1.
    assert 25 < luxury < 60, luxury
    assert 0.06 < luxury / supercenter < 0.16, (luxury, supercenter)


def test_grocery_promo_rate_visible(spark):
    cfg = GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 6, 2),
        end_date=date(2025, 6, 8),
        store_count=2,
        dc_count=1,
        customer_count=200,
        seed=11,
        transactions_per_store_day=30,
    )
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    g = generate_receipts_group(spark, dims, dicts.profile, cfg)
    lines = g["fact_receipt_lines"]
    promo_share = lines.filter(F.col("promo_code").isNotNull()).count() / lines.count()
    assert 0.12 < promo_share < 0.32  # profile promo_rate 0.22, loose bounds
