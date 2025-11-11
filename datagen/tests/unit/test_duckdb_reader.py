"""
Unit tests for DuckDB reader service.
"""

import pandas as pd
from retail_datagen.services import duckdb_reader as db_reader


def test_read_all_master_tables_smoke():
    data = db_reader.read_all_master_tables()
    assert set(data.keys()) == {
        "dim_geographies",
        "dim_stores",
        "dim_distribution_centers",
        "dim_trucks",
        "dim_customers",
        "dim_products",
    }
    for df in data.values():
        assert isinstance(df, pd.DataFrame)


def test_read_all_fact_tables_smoke():
    result = db_reader.read_all_fact_tables()
    assert set(result.keys()) == set(db_reader.FACT_TABLES)
    for df in result.values():
        assert isinstance(df, pd.DataFrame)


def test_get_all_fact_table_date_ranges_shape():
    ranges = db_reader.get_all_fact_table_date_ranges()
    assert set(ranges.keys()) == set(db_reader.FACT_TABLES)
    for rng in ranges.values():
        assert isinstance(rng, tuple) and len(rng) == 2

