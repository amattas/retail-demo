"""Dimension generation: driver-side numpy/pandas -> Spark DataFrames.

Semantics ported from datagen master_generators (ID schemes, tax lookup,
pricing rules); column names/types from schemas.TABLES, which the TMDL
contract test guards.
"""

from datetime import date, datetime, timedelta

import numpy as np
from pyspark.sql import DataFrame, SparkSession

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import DictionarySet
from retail_setup.generation.runtime import derive_seed
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
    msrp = max(0.01, round(base_price * (1.0 + rng.uniform(-0.15, 0.15)), 2))
    if rng.random() < 0.60:
        sale = msrp
    else:
        sale = max(0.01, round(msrp * (1.0 - rng.uniform(0.05, 0.35)), 2))
    cost = sale * rng.uniform(0.50, 0.85)
    cost = min(cost, sale - 0.01)
    cost = max(0.01, round(cost, 2))
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
            str(rng.choice(STORE_FORMATS)),
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

    # --- customers; ~70% placed in a store's geography (datagen home-store
    #     locality) so receipts can resolve a same-geography "local" shopper.
    first = [n.Name for n in dicts.first_names]
    last = [n.Name for n in dicts.last_names]
    cust_rows = []
    for cid in range(1, cfg.customer_count + 1):
        if store_geo_indices and rng.random() < CUSTOMER_HOME_AFFINITY:
            gi = int(rng.choice(store_geo_indices))
        else:
            gi = int(rng.integers(0, n_geo))
        cust_rows.append((
            cid,
            str(rng.choice(first)),
            str(rng.choice(last)),
            _addr(rng),
            gi + 1,
            f"LC{cid:06d}{int(rng.integers(0, 1000)):03d}",
            f"555-{int(rng.integers(200, 999))}-{int(rng.integers(1000, 9999))}",
            "BLE" + np.base_repr(cid, 36).rjust(6, "0"),
            f"AD{cid:08d}",
        ))
    out["dim_customers"] = spark.createDataFrame(cust_rows, spark_schema("dim_customers"))

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
            if launch_r < 0.6:
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
    rows = []
    d = start
    while d <= end:
        rows.append((
            int(d.strftime("%Y%m%d")),
            d,
            d.year,
            (d.month - 1) // 3 + 1,
            d.month,
            d.strftime("%B"),
            d.day,
            d.isoweekday(),
            d.strftime("%A"),
            int(d.strftime("%U")),
            1 if d.isoweekday() >= 6 else 0,
            d.year if d.month >= 7 else d.year - 1,
            ((d.month - 7) % 12) // 3 + 1,
        ))
        d += timedelta(days=1)
    return spark.createDataFrame(rows, spark_schema("dim_date"))
