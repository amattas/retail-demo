from collections import Counter
from datetime import date, datetime, timezone

import pytest

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dim_date, generate_dimensions
from retail_setup.generation.dims import balanced_geography_sample
from retail_setup.generation.network import (
    assign_dcs,
    fleet_sizes,
    next_slot_ms,
    plan_routes,
    pool_size,
    route_budget_minutes,
    routes_per_dc,
    store_dc_map,
    store_routes,
)
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


def test_dc_fleets_run_one_truck_per_delivery_route(dims, small_cfg):
    routes = store_routes(
        small_cfg.seed,
        dims["dim_stores"],
        dims["dim_geographies"],
        dims["dim_distribution_centers"],
    )
    served = {
        row.store_id: row.dc_id
        for row in store_dc_map(
            dims["dim_stores"], dims["dim_geographies"], dims["dim_distribution_centers"]
        ).collect()
    }
    assert served == {store: route.dc_id for store, route in routes.items()}
    rows = dims["dim_trucks"].collect()
    assigned = Counter(int(r.DCID) for r in rows if r.DCID is not None)
    dc_ids = [r.ID for r in dims["dim_distribution_centers"].collect()]
    # dims plans routes in Python exactly as the inventory chain and stream do.
    assert dict(assigned) == fleet_sizes(routes_per_dc(routes), dc_ids)
    assert sum(r.DCID is None for r in rows) == pool_size(sum(assigned.values()))
    assert [r.ID for r in rows] == list(range(1, len(rows) + 1))


def test_geographies_and_dc_load_are_spread_over_states(spark, dicts):
    cfg = GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 7),
        store_count=200,
        dc_count=20,
        customer_count=200,
        seed=42,
    )
    dims = generate_dimensions(spark, dicts, cfg)
    state_of = {r.ID: r.State for r in dims["dim_geographies"].collect()}
    # The shared dictionary has 85% of its cities in five states; stores and
    # DCs are spread over the states instead.
    dc_states = Counter(state_of[r.GeographyID] for r in dims["dim_distribution_centers"].collect())
    assert len(dc_states) >= 15 and max(dc_states.values()) <= 2, dc_states
    stores = Counter(state_of[r.GeographyID] for r in dims["dim_stores"].collect())
    top_five = sum(count for _, count in stores.most_common(5)) / cfg.store_count
    assert top_five < 0.5, stores
    load = Counter(
        row.dc_id
        for row in store_dc_map(
            dims["dim_stores"], dims["dim_geographies"], dims["dim_distribution_centers"]
        ).collect()
    )
    assert len(load) == cfg.dc_count, "every DC serves stores"
    assert max(load.values()) <= 3 * cfg.store_count / cfg.dc_count, load


def test_assign_dcs_balances_dcs_in_one_state_then_falls_back_to_region():
    dcs = [(3, "TX", "South"), (1, "CA", "West"), (2, "CA", "West"), (4, "NY", "Northeast")]
    stores = [(s, "CA", "West") for s in range(1, 8)] + [(8, "OR", "West"), (9, "FL", "South")]
    assigned = assign_dcs(stores, dcs)
    california = Counter(assigned[s] for s in range(1, 8))
    assert set(california) == {1, 2} and abs(california[1] - california[2]) <= 1
    assert assigned[8] in {1, 2} and assigned[9] == 3
    assert assign_dcs([(1, "WA", "Pacific")], dcs) == {1: 1}


def test_routes_never_overbook_a_truck_day_and_fleets_cover_routes():
    store_dc = {store: 1 + store % 2 for store in range(1, 41)}
    zones = {store: ("America/Chicago" if store % 3 else "America/Denver") for store in store_dc}
    routes = plan_routes(7, store_dc, zones)
    assert set(routes) == set(store_dc)
    by_truck = {}
    for store, route in routes.items():
        assert route.dc_id == store_dc[store] and 0 <= route.slot_minute < 1440
        by_truck.setdefault((route.dc_id, route.route), []).append((store, route))
    for members in by_truck.values():
        assert len({zones[store] for store, _ in members}) == 1
        assert sum(route_budget_minutes(r.transit_minutes) for _, r in members) <= 1440
        # Slots follow each other by the previous store's whole budget.
        slots = sorted(r.slot_minute for _, r in members)
        gaps = [b - a for a, b in zip(slots, slots[1:])] + [slots[0] + 1440 - slots[-1]]
        assert min(gaps) >= min(route_budget_minutes(r.transit_minutes) for _, r in members)
    assert fleet_sizes(routes_per_dc(routes), [1, 2, 3]) == {
        1: routes_per_dc(routes)[1],
        2: routes_per_dc(routes)[2],
        3: 2,
    }
    assert pool_size(14) == 2 and pool_size(1) == 1


def test_delivery_slots_are_local_wall_clock_times():
    since = int(datetime(2025, 3, 8, 12, tzinfo=timezone.utc).timestamp() * 1000)
    # 22:30 in New York is 03:30 UTC in winter and 02:30 UTC after the spring change.
    first = next_slot_ms(since, 22 * 60 + 30, "America/New_York")
    second = next_slot_ms(first + 1, 22 * 60 + 30, "America/New_York")
    assert datetime.fromtimestamp(first / 1000, timezone.utc) == datetime(
        2025, 3, 9, 3, 30, tzinfo=timezone.utc
    )
    assert datetime.fromtimestamp(second / 1000, timezone.utc) == datetime(
        2025, 3, 10, 2, 30, tzinfo=timezone.utc
    )
    assert next_slot_ms(since, 12 * 60) == since


def test_balanced_geography_sample_spreads_a_skewed_dictionary():
    import numpy as np

    states = ["CA"] * 100 + ["CT"] * 100 + ["TX"] * 5 + ["VT"]
    picked = balanced_geography_sample(states, 16, np.random.default_rng(1))
    counts = Counter(states[i] for i in picked)
    assert len(set(picked)) == 16
    assert counts["VT"] == 1 and counts["TX"] == 5 and counts["CA"] + counts["CT"] == 10
    assert picked == balanced_geography_sample(states, 16, np.random.default_rng(1))


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
