#!/usr/bin/env python3
"""
Code generation helper for async method wrappers (ONE-TIME USE).

Purpose:
    Generates boilerplate async method wrappers for MasterDataGenerator.
    This is a one-time code generation tool, not a runtime utility.

Output:
    Prints Python code for async versions of generation methods that:
    1. Call the existing sync method for data generation
    2. Insert to database if session provided
    3. Handle CSV export based on flags

Usage:
    python add_async_methods.py > async_methods.py
    # Then manually integrate into master_generator.py

Note:
    This is a development artifact used during Phase 3A implementation.
    It should NOT be run as part of normal workflows.
"""

import re

# Mapping of methods to their corresponding SQLAlchemy models
METHOD_TO_MODEL = {
    "generate_distribution_centers": ("DistributionCenterModel", "distribution_centers"),
    "generate_trucks": ("TruckModel", "trucks"),
    "generate_customers": ("CustomerModel", "customers"),
    "generate_products_master": ("ProductModel", "products_master"),
    "generate_dc_inventory_snapshots": (None, "dc_inventory_snapshots"),  # No DB model yet
    "generate_store_inventory_snapshots": (None, "store_inventory_snapshots"),  # No DB model yet
}

ASYNC_METHOD_TEMPLATE = '''
    async def {method_name}_async(self{params}) -> None:
        """Generate {table_name} data with optional database insertion (async version)."""
        # Call sync method for generation logic
        self.{method_name}({call_params})

        # Insert to database if session provided
        if hasattr(self, '_db_session') and self._db_session and {model_class}:
            await self._insert_to_db(
                self._db_session,
                {model_class},
                self.{attr_name}
            )
'''

def generate_async_methods():
    """Generate async method definitions for remaining methods."""
    methods = []

    for sync_method, (model_class, table_name) in METHOD_TO_MODEL.items():
        # Determine parameters and attributes
        if sync_method == "generate_customers":
            params = ""
            call_params = ""
            attr_name = "customers"
        elif sync_method == "generate_products_master":
            params = ""
            call_params = ""
            attr_name = "products_master"
        elif sync_method == "generate_dc_inventory_snapshots":
            params = ""
            call_params = ""
            attr_name = "dc_inventory_snapshots"
        elif sync_method == "generate_store_inventory_snapshots":
            params = ""
            call_params = ""
            attr_name = "store_inventory_snapshots"
        else:
            params = ""
            call_params = ""
            attr_name = table_name.replace("_", "")

        async_method = ASYNC_METHOD_TEMPLATE.format(
            method_name=sync_method,
            params=params,
            table_name=table_name,
            call_params=call_params,
            model_class=model_class if model_class else "None",
            attr_name=attr_name
        )

        methods.append(f"# Async version for {sync_method}")
        methods.append(async_method)

    return "\n".join(methods)

if __name__ == "__main__":
    print(generate_async_methods())
