"""
Unit tests for router constants and table mappings.

These tests verify that all fact tables are properly registered in the
router configuration, ensuring consistency between FACT_TABLES and DUCK_FACT_MAP.
"""

from retail_datagen.generators.routers.common import (
    DUCK_FACT_MAP,
    DUCK_MASTER_MAP,
    FACT_TABLES,
    MASTER_TABLES,
    get_physical_table_name,
)


class TestRouterConstants:
    """Test router constant definitions."""

    def test_reorders_in_fact_tables(self):
        """Test that reorders is registered in FACT_TABLES."""
        assert "reorders" in FACT_TABLES, "reorders missing from FACT_TABLES"

    def test_reorders_in_duck_fact_map(self):
        """Test that reorders is registered in DUCK_FACT_MAP."""
        assert "reorders" in DUCK_FACT_MAP, "reorders missing from DUCK_FACT_MAP"

    def test_reorders_mapping_correct(self):
        """Test that reorders maps to correct physical table name."""
        assert DUCK_FACT_MAP["reorders"] == "fact_reorders"

    def test_all_fact_tables_have_mapping(self):
        """Test that all FACT_TABLES have corresponding DUCK_FACT_MAP entries."""
        for table in FACT_TABLES:
            assert table in DUCK_FACT_MAP, f"{table} missing from DUCK_FACT_MAP"

    def test_all_duck_fact_map_entries_in_fact_tables(self):
        """Test that all DUCK_FACT_MAP keys are in FACT_TABLES."""
        for table in DUCK_FACT_MAP:
            assert table in FACT_TABLES, (
                f"{table} in DUCK_FACT_MAP but not in FACT_TABLES"
            )

    def test_all_master_tables_have_mapping(self):
        """Test that all MASTER_TABLES have corresponding DUCK_MASTER_MAP entries."""
        for table in MASTER_TABLES:
            assert table in DUCK_MASTER_MAP, f"{table} missing from DUCK_MASTER_MAP"

    def test_fact_payments_in_router_config(self):
        """Test that fact_payments is registered in router configuration."""
        assert "fact_payments" in FACT_TABLES, "fact_payments missing from FACT_TABLES"
        assert "fact_payments" in DUCK_FACT_MAP, (
            "fact_payments missing from DUCK_FACT_MAP"
        )


class TestGetPhysicalTableName:
    """Test get_physical_table_name function."""

    def test_get_physical_table_name_master(self):
        """Test physical table name lookup for master tables."""
        assert get_physical_table_name("stores", "master") == "dim_stores"
        assert get_physical_table_name("customers", "master") == "dim_customers"

    def test_get_physical_table_name_fact(self):
        """Test physical table name lookup for fact tables."""
        assert get_physical_table_name("receipts", "fact") == "fact_receipts"
        assert get_physical_table_name("reorders", "fact") == "fact_reorders"

    def test_get_physical_table_name_unknown(self):
        """Test that unknown table names return themselves."""
        assert get_physical_table_name("unknown_table", "master") == "unknown_table"
        assert get_physical_table_name("unknown_table", "fact") == "unknown_table"
