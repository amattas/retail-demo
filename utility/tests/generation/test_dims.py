from datetime import date

import pytest

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dim_date, generate_dimensions
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def small_cfg():
    return GenerationConfig(
        store_type="grocery", start_date=date(2025, 1, 1), end_date=date(2025, 1, 7),
        store_count=4, dc_count=2, customer_count=200, seed=42,
    )


@pytest.fixture(scope="module")
def dicts():
    return load_dictionaries(default_dictionary_root(), "grocery")


@pytest.fixture(scope="module")
def dims(spark, small_cfg, dicts):
    return generate_dimensions(spark, dicts, small_cfg)


def test_all_dims_present_with_contract_columns(dims):
    for table in ["dim_geographies", "dim_stores", "dim_distribution_centers",
                  "dim_trucks", "dim_customers", "dim_products"]:
        assert table in dims
        assert dims[table].columns == column_names(table)


def test_row_counts(dims, small_cfg, dicts):
    assert dims["dim_stores"].count() == small_cfg.store_count
    assert dims["dim_distribution_centers"].count() == small_cfg.dc_count
    assert dims["dim_customers"].count() == small_cfg.customer_count
    assert dims["dim_products"].count() == len(dicts.products)


def test_fk_integrity(dims):
    geo_ids = {r.ID for r in dims["dim_geographies"].select("ID").collect()}
    for t in ["dim_stores", "dim_distribution_centers", "dim_customers"]:
        refs = {r.GeographyID for r in dims[t].select("GeographyID").collect()}
        assert refs <= geo_ids, t


def test_store_fields(dims):
    rows = dims["dim_stores"].collect()
    assert all(r.StoreNumber == f"S{r.ID:06d}" for r in rows)
    assert all(0 < r.tax_rate < 0.20 for r in rows)
    assert all(r.daily_traffic_multiplier > 0 for r in rows)


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


def test_determinism(spark, small_cfg, dicts):
    a = generate_dimensions(spark, dicts, small_cfg)
    b = generate_dimensions(spark, dicts, small_cfg)
    assert sorted(map(tuple, a["dim_customers"].collect())) == \
           sorted(map(tuple, b["dim_customers"].collect()))


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
