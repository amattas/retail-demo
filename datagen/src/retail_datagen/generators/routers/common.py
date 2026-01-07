"""
Common constants and utilities for generator routers.

This module contains shared constants and helper functions used across
the generator router endpoints.
"""

import logging

logger = logging.getLogger(__name__)

# Available master table definitions
MASTER_TABLES = [
    "geographies_master",
    "stores",
    "distribution_centers",
    "trucks",
    "customers",
    "products_master",
]

# DuckDB logical->physical mappings for master tables
DUCK_MASTER_MAP = {
    "geographies_master": "dim_geographies",
    "stores": "dim_stores",
    "distribution_centers": "dim_distribution_centers",
    "trucks": "dim_trucks",
    "customers": "dim_customers",
    "products_master": "dim_products",
}

# DuckDB logical->physical mappings for fact tables
DUCK_FACT_MAP = {
    "dc_inventory_txn": "fact_dc_inventory_txn",
    "truck_moves": "fact_truck_moves",
    "store_inventory_txn": "fact_store_inventory_txn",
    "receipts": "fact_receipts",
    "receipt_lines": "fact_receipt_lines",
    "foot_traffic": "fact_foot_traffic",
    "ble_pings": "fact_ble_pings",
    "marketing": "fact_marketing",
    "online_orders": "fact_online_order_headers",
    "online_order_lines": "fact_online_order_lines",
}

# Available fact tables
FACT_TABLES = [
    "dc_inventory_txn",
    "truck_moves",
    "store_inventory_txn",
    "receipts",
    "receipt_lines",
    "foot_traffic",
    "ble_pings",
    "marketing",
    "online_orders",
    "online_order_lines",
]

# Combined mapping for unified lookups
ALL_TABLE_MAP = {**DUCK_MASTER_MAP, **DUCK_FACT_MAP}


def get_physical_table_name(logical_name: str, table_type: str = "master") -> str:
    """Get the physical DuckDB table name for a logical table name."""
    if table_type == "master":
        return DUCK_MASTER_MAP.get(logical_name, logical_name)
    return DUCK_FACT_MAP.get(logical_name, logical_name)


def get_duckdb_connection():
    """Get a DuckDB connection (lazy import to avoid circular dependencies)."""
    from retail_datagen.db.duckdb_engine import get_duckdb_conn

    return get_duckdb_conn()
