from collections import Counter
from datetime import date

import pytest

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dim_date, generate_dimensions
from retail_setup.generation.network import fleet_sizes, nearest_dc, pool_size, store_dc_map
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def small_cfg():
    return GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 7),
        store_count=4,
        dc_count=2,
        customer_count=200,
        seed=42,
    )


@pytest.fixture(scope="module")
def dicts():
    return load_dictionaries(default_dictionary_root(), "grocery")


@pytest.fixture(scope="module")
def dims(spark, small_cfg, dicts):
    return generate_dimensions(spark, dicts, small_cfg)


def test_all_dims_present_with_contract_columns(dims):
    for table in [
        "dim_geographies",
        "dim_stores",
        "dim_distribution_centers",
        "dim_trucks",
        "dim_customers",
        "dim_products",
    ]:
        assert table in dims
        assert dims[table].columns == column_names(table)


def test_row_counts(dims, small_cfg, dicts):
    assert dims["dim_stores"].count() == small_cfg.store_count
    assert dims["dim_distribution_centers"].count() == small_cfg.dc_count
    assert dims["dim_customers"].count() == small_cfg.customer_count
    # each base product yields up to brands_per_product category-matched SKUs
    n_prod = dims["dim_products"].count()
    assert len(dicts.products) <= n_prod <= len(dicts.products) * small_cfg.brands_per_product


def test_fk_integrity(dims):
    geo_ids = {r.ID for r in dims["dim_geographies"].select("ID").collect()}
    for t in ["dim_stores", "dim_distribution_centers", "dim_customers"]:
        refs = {r.GeographyID for r in dims[t].select("GeographyID").collect()}
        assert refs <= geo_ids, t


def test_store_fields(dims, dicts):
    rows = dims["dim_stores"].collect()
    assert all(r.StoreNumber == f"S{r.ID:06d}" for r in rows)
    assert all(0 < r.tax_rate < 0.20 for r in rows)
    assert all(r.daily_traffic_multiplier > 0 for r in rows)
    # Formats and hours come from the store type (grocery never runs hypermarkets).
    assert {r.store_format for r in rows} <= set(dicts.profile.resolved_store_formats())
    assert {r.operating_hours for r in rows} <= set(dicts.profile.resolved_operating_hours())


def test_luxury_stores_are_boutiques_with_daytime_hours(spark):
    cfg = GenerationConfig(
        store_type="luxury",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 7),
        store_count=30,
        dc_count=2,
        seed=42,
    )
    luxury = load_dictionaries(default_dictionary_root(), "luxury")
    rows = generate_dimensions(spark, luxury, cfg)["dim_stores"].collect()
    assert {r.store_format for r in rows} == {"boutique"}
    assert {r.operating_hours for r in rows} <= {"10-19", "10-20", "11-19"}
    assert "24h" not in {r.operating_hours for r in rows}


def test_product_pricing_invariants(dims):
    rows = dims["dim_products"].collect()
    for r in rows:
        assert 0 < r.Cost < r.SalePrice <= r.MSRP, r.ProductName
        assert r.taxability in {"TAXABLE", "NON_TAXABLE", "REDUCED_RATE"}


def test_trucks_dc_assignment(dims):
    rows = dims["dim_trucks"].collect()
    dc_ids = {float(r.ID) for r in dims["dim_distribution_centers"].collect()}
    assigned = [r for r in rows if r.DCID is not None]
    pool = [r for r in rows if r.DCID is None]
    assert assigned and pool  # both populations exist
    assert {r.DCID for r in assigned} <= dc_ids


def test_dc_fleets_are_sized_to_the_stores_each_dc_serves(dims):
    served = Counter(
        row.dc_id
        for row in store_dc_map(
            dims["dim_stores"], dims["dim_geographies"], dims["dim_distribution_centers"]
        ).collect()
    )
    rows = dims["dim_trucks"].collect()
    assigned = Counter(int(r.DCID) for r in rows if r.DCID is not None)
    dc_ids = [r.ID for r in dims["dim_distribution_centers"].collect()]
    # dims routes stores in Python exactly as the Spark store_dc_map does.
    assert dict(assigned) == fleet_sizes(dict(served), dc_ids)
    assert sum(r.DCID is None for r in rows) == pool_size(sum(assigned.values()))
    assert [r.ID for r in rows] == list(range(1, len(rows) + 1))


def test_determinism(spark, small_cfg, dicts):
    a = generate_dimensions(spark, dicts, small_cfg)
    b = generate_dimensions(spark, dicts, small_cfg)
    assert sorted(map(tuple, a["dim_customers"].collect())) == sorted(
        map(tuple, b["dim_customers"].collect())
    )


def test_dimensions_do_not_depend_on_the_history_window(spark, dicts):
    # The same seed and pinned anchor, rendered for a later (months-based)
    # window, must regenerate identical dimensions (B-13).
    base = dict(store_type="grocery", store_count=4, dc_count=2, customer_count=200, seed=42)
    first = GenerationConfig(
        **base,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 3, 31),
        anchor_date=date(2025, 1, 1),
    )
    later = GenerationConfig(
        **base, start_date=date(2025, 2, 9), end_date=date(2025, 5, 8), anchor_date=date(2025, 1, 1)
    )
    a = generate_dimensions(spark, dicts, first)
    b = generate_dimensions(spark, dicts, later)
    assert set(a) == set(b)
    for table in a:
        assert sorted(map(tuple, a[table].collect())) == sorted(map(tuple, b[table].collect())), (
            table
        )


def test_anchor_defaults_to_the_history_start():
    cfg = GenerationConfig(start_date=date(2025, 1, 1), end_date=date(2025, 1, 7))
    assert cfg.anchor_date == date(2025, 1, 1)


def test_customers_are_generated_from_a_spark_range(dims):
    plan = dims["dim_customers"]._jdf.queryExecution().logical().toString()
    assert "Range" in plan


def test_dim_date(spark):
    df = generate_dim_date(spark, date(2025, 1, 1), date(2025, 12, 31))
    assert df.columns == column_names("dim_date")
    rows = {r.date_key: r for r in df.collect()}
    assert len(rows) == 365
    r = rows[20250704]  # 2025-07-04, a Friday
    assert (r.year, r.month, r.day, r.day_of_week) == (2025, 7, 4, 5)
    assert r.fiscal_year == 2025 and r.fiscal_quarter == 1  # July = FY start
    sat = rows[20250705]
    assert sat.is_weekend == 1


def test_dim_date_is_generated_from_a_spark_range(spark):
    df = generate_dim_date(spark, date(2025, 1, 1), date(2035, 12, 31))
    plan = df._jdf.queryExecution().logical().toString()
    assert "Range" in plan


def test_explicit_product_attributes_override_name_rules(spark, small_cfg, dicts):
    from dataclasses import replace

    # "Fresh" is NON_TAXABLE and "Produce" refrigerated by name; explicit values win.
    product = dicts.products[0].model_copy(
        update={
            "Department": "Fresh",
            "Category": "Produce",
            "Taxability": "TAXABLE",
            "Refrigerated": False,
        }
    )
    custom = replace(dicts, products=[product, *dicts.products[1:]])

    products = generate_dimensions(spark, custom, small_cfg)["dim_products"]
    row = products.filter(products.ProductName == product.ProductName).first()

    assert row is not None
    assert (row.taxability, row.RequiresRefrigeration) == ("TAXABLE", False)


def test_nearest_dc_prefers_state_then_region_then_lowest_id():
    dcs = [(3, "TX", "South"), (1, "CA", "West"), (2, "OR", "West")]
    assert nearest_dc("TX", "South", dcs) == 3
    assert nearest_dc("WA", "West", dcs) == 1
    assert nearest_dc("NY", "Northeast", dcs) == 1
    assert fleet_sizes({1: 25, 2: 4}, [1, 2, 3]) == {1: 10, 2: 2, 3: 2}
    assert pool_size(14) == 2 and pool_size(1) == 1
