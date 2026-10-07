"""Dimension generation using bounded driver metadata and Spark-native rows.

Semantics ported from datagen master_generators (ID schemes, tax lookup,
pricing rules); column names/types from schemas.TABLES, which the TMDL
contract test guards.
"""

from datetime import date, datetime, timedelta

import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import DictionarySet
from retail_setup.generation.runtime import (
    date_grid,
    derive_seed,
    fixed_width_decimal,
    minimum_width_decimal,
    minimum_width_text,
    seeded_draws,
)
from retail_setup.generation.schemas import spark_schema

# datagen StoreProfiler equivalents (volume class -> traffic multiplier range).
# Five tiers incl. KIOSK, matching datagen's StoreVolumeClass (the kiosk tier
# was dropped in the first port; restored here for store-volume variety).
VOLUME_CLASSES = [
    ("flagship", 0.05, (1.8, 2.5)),
    ("high_volume", 0.15, (1.3, 1.8)),
    ("standard", 0.55, (0.8, 1.3)),
    ("low_volume", 0.20, (0.4, 0.8)),
    ("kiosk", 0.05, (0.25, 0.35)),
]
# Five formats incl. EXPRESS (datagen's StoreFormat.EXPRESS). store_activity
# carries a matching express zone-share row.
STORE_FORMATS = ["hypermarket", "superstore", "standard", "neighborhood", "express"]
VOLUME_FORMATS = {
    "flagship": ["hypermarket", "superstore"],
    "high_volume": ["hypermarket", "superstore", "standard"],
    "standard": ["superstore", "standard", "neighborhood"],
    "low_volume": ["standard", "neighborhood", "express"],
    "kiosk": ["express"],
}
OPERATING_HOURS = ["6-22", "7-22", "7-23", "24h"]
DEFAULT_TAX_RATE = 0.07407  # datagen receipts_mixin fallback
REFRIGERATED_CATEGORIES = {"Produce", "Dairy & Eggs", "Dairy & Alternatives",
                           "Meat & Poultry", "Meat & Seafood", "Seafood", "Frozen"}

# Share of customers placed in a geography that has a store (the rest are
# scattered anywhere). Restores datagen's "customers live near stores" locality
# so receipts can draw a same-geography customer most of the time.
CUSTOMER_HOME_AFFINITY = 0.70

# Map a product department/category token to a brand-dictionary Category so a
# product's brand fits its department (datagen product_generator parity: a
# grocery item never carries a hardware brand). Tokens are matched lowercased;
# substring matches (e.g. "home" in "home & garden") are tried before falling
# back to the whole brand pool.
_BRAND_CAT_SYNONYMS = {
    "grocery": "food", "fresh": "food", "pantry": "food", "snacks": "food",
    "beverages": "food", "health & beauty": "health", "home & garden": "home",
    "office supplies": "office", "pet supplies": "pet",
    "sports & recreation": "sports", "apparel": "clothing",
}


def _match_brand_category(key: str, brands_by_cat: dict[str, list]) -> list | None:
    """Resolve a product department/category to a brand pool, or None."""
    k = key.strip().lower()
    if k in brands_by_cat:
        return brands_by_cat[k]
    syn = _BRAND_CAT_SYNONYMS.get(k)
    if syn and syn in brands_by_cat:
        return brands_by_cat[syn]
    for cat, pool in brands_by_cat.items():
        if cat and (cat in k or k in cat):
            return pool
    return None


def compute_pricing(base_price: float, rng: np.random.Generator) -> tuple[float, float, float]:
    """Return ``(cost, msrp, sale_price)`` for a base price.

    Ported from datagen ``PricingCalculator`` (shared/validators/pricing.py):

    - MSRP = BasePrice +/-15%
    - SalePrice = MSRP (60% of the time) OR MSRP discounted 5-35% (40%)
    - Cost = 50-85% of SalePrice

    Always guarantees ``Cost < SalePrice <= MSRP``.
    """
    # A positive cost and a strict one-cent margin require a minimum sale price
    # of two cents. This keeps the documented Cost < SalePrice invariant even
    # for adversarial one-cent source prices.
    msrp = max(0.02, round(base_price * (1.0 + rng.uniform(-0.15, 0.15)), 2))
    if rng.random() < 0.60:
        sale = msrp
    else:
        sale = max(0.02, round(msrp * (1.0 - rng.uniform(0.05, 0.35)), 2))
    cost = round(sale * rng.uniform(0.50, 0.85), 2)
    cost = max(0.01, min(cost, round(sale - 0.01, 2)))
    return float(cost), float(msrp), float(sale)


def _addr(rng: np.random.Generator) -> str:
    return (
        f"{int(rng.integers(100, 9999))} "
        f"{str(rng.choice(['Main', 'Oak', 'Maple', 'Market', 'Commerce', 'Liberty']))} "
        f"{str(rng.choice(['St', 'Ave', 'Blvd', 'Rd']))}"
    )


def generate_dimensions(
    spark: SparkSession, dicts: DictionarySet, cfg: GenerationConfig
) -> dict[str, DataFrame]:
    rng = np.random.default_rng(derive_seed(cfg.seed, "dims", 0, cfg.start_date))
    out: dict[str, DataFrame] = {}

    # --- geographies: sample from dictionary, sequential IDs
    n_geo = min(len(dicts.geographies), max(cfg.store_count * 2, cfg.dc_count * 2, 20))
    geo_idx = rng.choice(len(dicts.geographies), size=n_geo, replace=False)
    geos = [dicts.geographies[i] for i in geo_idx]
    geo_rows = [
        (i + 1, g.City, g.State, g.Zip, g.District, g.Region)
        for i, g in enumerate(geos)
    ]
    out["dim_geographies"] = spark.createDataFrame(geo_rows, spark_schema("dim_geographies"))

    # tax lookup hierarchy (datagen TaxCalculator parity):
    #   exact (State, City) -> (State, County) average -> State average -> default.
    # County is resolved from the tax dictionary; geographies carry no county of
    # their own, so the county tier only applies to cities present in the dict.
    city_acc: dict[tuple[str, str], list[float]] = {}
    city_county: dict[tuple[str, str], str] = {}
    county_acc: dict[tuple[str, str], list[float]] = {}
    state_rates: dict[str, list[float]] = {}
    for t in dicts.tax_rates:
        rate = float(t.CombinedRate)
        city_acc.setdefault((t.StateCode, t.City), []).append(rate)
        city_county[(t.StateCode, t.City)] = t.County
        county_acc.setdefault((t.StateCode, t.County), []).append(rate)
        state_rates.setdefault(t.StateCode, []).append(rate)
    by_city = {k: float(np.mean(v)) for k, v in city_acc.items()}
    by_county = {k: float(np.mean(v)) for k, v in county_acc.items()}

    def tax_for(state: str, city: str) -> float:
        if (state, city) in by_city:
            return by_city[(state, city)]
        county = city_county.get((state, city))
        if county is not None and (state, county) in by_county:
            return by_county[(state, county)]
        if state in state_rates:
            return float(np.mean(state_rates[state]))
        return DEFAULT_TAX_RATE

    # --- DCs first (datagen: stores constrained to DC states)
    dc_geo_idx = rng.choice(n_geo, size=cfg.dc_count, replace=cfg.dc_count > n_geo)
    dc_rows = [
        (i + 1, f"DC{i + 1:03d}", _addr(rng), int(dc_geo_idx[i]) + 1)
        for i in range(cfg.dc_count)
    ]
    out["dim_distribution_centers"] = spark.createDataFrame(
        dc_rows, spark_schema("dim_distribution_centers")
    )

    # --- stores in DC states
    dc_states = {geos[int(g)].State for g in dc_geo_idx}
    eligible = [i for i, g in enumerate(geos) if g.State in dc_states] or list(range(n_geo))
    store_rows = []
    store_geo_indices: list[int] = []
    for sid in range(1, cfg.store_count + 1):
        gi = int(rng.choice(eligible))
        store_geo_indices.append(gi)
        g = geos[gi]
        classes, probs = zip(*[(c, p) for c, p, _ in VOLUME_CLASSES])
        vc = str(rng.choice(classes, p=probs))
        lo, hi = next(r for c, _, r in VOLUME_CLASSES if c == vc)
        store_rows.append((
            sid,
            f"S{sid:06d}",
            _addr(rng),
            gi + 1,
            tax_for(g.State, g.City),
            vc,
            str(rng.choice(VOLUME_FORMATS[vc])),
            str(rng.choice(OPERATING_HOURS)),
            float(np.round(rng.uniform(lo, hi), 2)),
        ))
    out["dim_stores"] = spark.createDataFrame(store_rows, spark_schema("dim_stores"))

    # --- trucks: 85% assigned round-robin to DCs, 15% pool (DCID NULL); 40% refrigerated
    n_trucks = max(cfg.dc_count * 3, 6)
    n_assigned = int(round(n_trucks * 0.85))
    truck_rows = []
    for tid in range(1, n_trucks + 1):
        plate = f"TRK{tid:04d}{chr(65 + tid % 26)}"
        refrig = bool(rng.random() < 0.4)
        dcid = float((tid - 1) % cfg.dc_count + 1) if tid <= n_assigned else None
        truck_rows.append((tid, plate, refrig, dcid))
    out["dim_trucks"] = spark.createDataFrame(truck_rows, spark_schema("dim_trucks"))

    # --- customers; use Spark range + deterministic hashes so customer scale
    #     does not materialize one Python tuple per customer on the driver.
    #     ~70% are placed in a store's geography (datagen home-store locality).
    first = [n.Name for n in dicts.first_names]
    last = [n.Name for n in dicts.last_names]
    customer_draws = seeded_draws(cfg.seed)
    customers = spark.range(1, cfg.customer_count + 1).select(
        F.col("id").cast("long").alias("ID")
    )
    first_names = F.array(*[F.lit(str(name)) for name in first])
    last_names = F.array(*[F.lit(str(name)) for name in last])
    store_geographies = F.array(
        *[F.lit(index + 1).cast("long") for index in store_geo_indices]
    )
    global_geography = (
        customer_draws.h64(["ID"], "customer-global-geo") % F.lit(n_geo) + 1
    ).cast("long")
    if store_geo_indices:
        local_geography = F.element_at(
            store_geographies,
            (
                customer_draws.h64(["ID"], "customer-local-geo")
                % F.lit(len(store_geo_indices))
                + 1
            ).cast("int"),
        )
        geography = F.when(
            customer_draws.u(["ID"], "customer-home-affinity")
            < F.lit(CUSTOMER_HOME_AFFINITY),
            local_geography,
        ).otherwise(global_geography)
    else:
        geography = global_geography

    street_names = F.array(
        *[F.lit(name) for name in ["Main", "Oak", "Maple", "Market", "Commerce", "Liberty"]]
    )
    street_types = F.array(*[F.lit(name) for name in ["St", "Ave", "Blvd", "Rd"]])
    first_index = (
        customer_draws.h64(["ID"], "customer-first") % F.lit(len(first)) + 1
    ).cast("long")
    last_index = (
        customer_draws.h64(["ID"], "customer-last") % F.lit(len(last)) + 1
    ).cast("long")
    street_number = (
        customer_draws.h64(["ID"], "customer-street-number") % F.lit(9900) + 100
    ).cast("long")
    street_name = F.element_at(
        street_names,
        (
            customer_draws.h64(["ID"], "customer-street-name")
            % F.lit(6)
            + 1
        ).cast("int"),
    )
    street_type = F.element_at(
        street_types,
        (
            customer_draws.h64(["ID"], "customer-street-type")
            % F.lit(4)
            + 1
        ).cast("int"),
    )
    loyalty_suffix = (
        customer_draws.h64(["ID"], "customer-loyalty") % F.lit(1000)
    ).cast("long")
    phone_exchange = (
        customer_draws.h64(["ID"], "customer-phone-exchange") % F.lit(799) + 200
    ).cast("long")
    phone_line = (
        customer_draws.h64(["ID"], "customer-phone-line") % F.lit(8999) + 1000
    ).cast("long")
    ble_id = F.upper(F.conv(F.col("ID").cast("string"), 10, 36))

    customers = (
        customers
        .select(
            "ID",
            F.element_at(first_names, first_index.cast("int")).alias("FirstName"),
            F.element_at(last_names, last_index.cast("int")).alias("LastName"),
            F.format_string(
                "%d %s %s", street_number, street_name, street_type
            ).alias("Address"),
            geography.alias("GeographyID"),
            F.concat(
                F.lit("LC"),
                minimum_width_decimal(F.col("ID"), 6),
                fixed_width_decimal(loyalty_suffix, 3, "loyalty suffix"),
            ).alias("LoyaltyCard"),
            F.format_string(
                "555-%03d-%04d", phone_exchange, phone_line
            ).alias("Phone"),
            F.concat(
                F.lit("BLE"), minimum_width_text(ble_id, 6)
            ).alias("BLEId"),
            F.concat(
                F.lit("AD"), minimum_width_decimal(F.col("ID"), 8)
            ).alias("AdId"),
        )
    )
    out["dim_customers"] = customers.select(
        *[field.name for field in spark_schema("dim_customers").fields]
    )

    # --- products: each base product is offered by up to brands_per_product
    #     category-matched brands (datagen combinatorial SKUs). Pricing/launch
    #     are re-rolled per branded variant for realistic price spread.
    brands_by_cat: dict[str, list] = {}
    for b in dicts.brands:
        brands_by_cat.setdefault(b.Category.strip().lower(), []).append(b)
    all_brands = list(dicts.brands)
    tags_by_product = {t.ProductName: t.Tags for t in dicts.tags}
    # Use naive UTC datetimes — Spark session timezone is UTC (set in conftest fixture)
    hist_start = datetime.combine(cfg.start_date, datetime.min.time())
    # Guarantee every department has at least one product available from the
    # first day of history: without this, an adversarial seed could push every
    # product in a department past `hist_start`, leaving a sale-eligible
    # department with no launched product and forcing receipts.py to either drop
    # lines or violate the no-pre-launch-sales invariant (IMP-010). The first
    # generated variant per department is pinned to a pre-history launch.
    dept_covered: set[str] = set()
    prod_rows = []
    pid = 0
    for p in dicts.products:
        pool = (_match_brand_category(p.Department, brands_by_cat)
                or _match_brand_category(p.Category, brands_by_cat)
                or all_brands)
        k = min(cfg.brands_per_product, len(pool))
        brand_idx = rng.choice(len(pool), size=k, replace=False)
        taxability = (
            "NON_TAXABLE" if p.Department in {"Fresh", "Grocery"} and "Candy" not in p.Category
            else "REDUCED_RATE" if p.Department in {"Clothing", "Apparel"}
            else "TAXABLE"
        )
        refrigerated = bool(p.Category in REFRIGERATED_CATEGORIES)
        tags = p.Tags or tags_by_product.get(p.ProductName)
        for j in brand_idx:
            pid += 1
            chosen = pool[int(j)]
            launch_r = float(rng.random())  # 60% before history, 30% first half, 10% later
            if p.Department not in dept_covered:
                # First product seen for this department: pin pre-history so the
                # department is sale-eligible on day one. Draw the RNG regardless
                # (above) to keep every other product's launch stream unchanged.
                launch = hist_start - timedelta(days=int(rng.integers(30, 1500)))
                dept_covered.add(p.Department)
            elif launch_r < 0.6:
                launch = hist_start - timedelta(days=int(rng.integers(30, 1500)))
            elif launch_r < 0.9:
                launch = hist_start + timedelta(days=int(rng.integers(0, 183)))
            else:
                launch = hist_start + timedelta(days=int(rng.integers(183, 366)))
            cost, msrp, sale = compute_pricing(float(p.BasePrice), rng)
            prod_rows.append((
                pid,
                p.ProductName,
                chosen.Brand,
                chosen.Company,
                p.Department,
                p.Category,
                p.Subcategory,
                cost,
                msrp,
                sale,
                refrigerated,
                launch,
                taxability,
                tags,
            ))
    out["dim_products"] = spark.createDataFrame(prod_rows, spark_schema("dim_products"))
    return out


def generate_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    """Exact port of 02-historical-data-load's dim_date (fiscal year starts July)."""
    dates = date_grid(spark, start, end, "date")
    month = F.month("date")
    iso_day = F.pmod(F.dayofweek("date") + F.lit(5), F.lit(7)) + F.lit(1)
    week_of_year = F.floor(
        (
            F.dayofyear("date")
            + F.lit(7)
            - F.dayofweek("date")
        )
        / F.lit(7)
    )
    return dates.select(
        F.date_format("date", "yyyyMMdd").cast("long").alias("date_key"),
        "date",
        F.year("date").cast("long").alias("year"),
        F.quarter("date").cast("long").alias("quarter"),
        month.cast("long").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").cast("long").alias("day"),
        iso_day.cast("long").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        week_of_year.cast("long").alias("week_of_year"),
        F.when(iso_day >= 6, 1).otherwise(0).cast("long").alias("is_weekend"),
        F.when(month >= 7, F.year("date"))
        .otherwise(F.year("date") - 1)
        .cast("long")
        .alias("fiscal_year"),
        (
            F.floor(F.pmod(month - F.lit(7), F.lit(12)) / F.lit(3))
            + F.lit(1)
        )
        .cast("long")
        .alias("fiscal_quarter"),
    )
