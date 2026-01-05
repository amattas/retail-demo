"""
Inventory snapshot generation for DCs and stores.
"""

import logging
from datetime import UTC, datetime
from typing import Any

import numpy as np

from retail_datagen.config.models import RetailConfig
from retail_datagen.shared.models import (
    DCInventorySnapshot,
    DistributionCenter,
    ProductMaster,
    Store,
    StoreInventorySnapshot,
)

logger = logging.getLogger(__name__)


class InventoryGeneratorMixin:
    """Mixin for inventory snapshot generation."""

    def generate_dc_inventory_snapshots(
        self,
        distribution_centers: list[DistributionCenter],
        products_master: list[ProductMaster],
        config: RetailConfig,
        np_rng: Any,
    ) -> list[DCInventorySnapshot]:
        """
        Generate realistic initial inventory snapshots for distribution centers.
        
        Args:
            distribution_centers: DC records
            products_master: Product records
            config: Retail configuration
            np_rng: NumPy random generator
            
        Returns:
            List of DCInventorySnapshot records
        """
        print("Generating DC inventory snapshots...")

        if not distribution_centers or not products_master:
            raise ValueError("DCs and products must be generated before inventory snapshots")

        # Vectorized implementation
        current_time = datetime.now(UTC)

        dcs = np.array([dc.ID for dc in distribution_centers], dtype=np.int32)
        prods = np.array([p.ID for p in products_master], dtype=np.int32)

        D = len(dcs)
        P = len(prods)
        total_records = D * P
        print(f"Generating {total_records:,} DC inventory records (vectorized)...")

        # Cartesian product of DC x Product
        dc_ids = np.repeat(dcs, P)
        prod_ids = np.tile(prods, D)

        # Quantities
        q_min = int(config.volume.dc_initial_inventory_min)
        q_max = int(config.volume.dc_initial_inventory_max)
        qty = np_rng.integers(q_min, q_max + 1, size=total_records, dtype=np.int32)

        # Reorder point bounds
        rp_min = int(config.volume.dc_reorder_point_min)
        rp_cfg_max = int(config.volume.dc_reorder_point_max)
        rp_hi = np.minimum(rp_cfg_max, np.maximum(1, qty // 5))
        base_rp = np_rng.integers(1, rp_hi + 1, size=total_records, dtype=np.int32)
        rp = np.where(rp_hi >= rp_min, np.maximum(base_rp, rp_min), rp_hi)

        # Build Pydantic models
        snapshots: list[DCInventorySnapshot] = []
        for i in range(total_records):
            snapshots.append(
                DCInventorySnapshot(
                    DCID=int(dc_ids[i]),
                    ProductID=int(prod_ids[i]),
                    CurrentQuantity=int(qty[i]),
                    ReorderPoint=int(rp[i]),
                    LastUpdated=current_time,
                )
            )

        print(f"Generated {len(snapshots):,} DC inventory records (vectorized)")
        return snapshots

    def generate_store_inventory_snapshots(
        self,
        stores: list[Store],
        products_master: list[ProductMaster],
        config: RetailConfig,
        np_rng: Any,
    ) -> list[StoreInventorySnapshot]:
        """
        Generate realistic initial inventory snapshots for stores.
        
        Args:
            stores: Store records
            products_master: Product records
            config: Retail configuration
            np_rng: NumPy random generator
            
        Returns:
            List of StoreInventorySnapshot records
        """
        print("Generating store inventory snapshots...")

        if not stores or not products_master:
            raise ValueError("Stores and products must be generated before inventory snapshots")

        # Vectorized implementation
        current_time = datetime.now(UTC)

        store_array = np.array([s.ID for s in stores], dtype=np.int32)
        prods = np.array([p.ID for p in products_master], dtype=np.int32)

        S = len(store_array)
        P = len(prods)
        total_records = S * P
        print(f"Generating {total_records:,} store inventory records (vectorized)...")

        store_ids = np.repeat(store_array, P)
        prod_ids = np.tile(prods, S)

        q_min = int(config.volume.store_initial_inventory_min)
        q_max = int(config.volume.store_initial_inventory_max)
        qty = np_rng.integers(q_min, q_max + 1, size=total_records, dtype=np.int32)

        rp_min = int(config.volume.store_reorder_point_min)
        rp_cfg_max = int(config.volume.store_reorder_point_max)
        rp_hi = np.minimum(rp_cfg_max, np.maximum(1, qty // 3))
        base_rp = np_rng.integers(1, rp_hi + 1, size=total_records, dtype=np.int32)
        rp = np.where(rp_hi >= rp_min, np.maximum(base_rp, rp_min), rp_hi)

        snapshots: list[StoreInventorySnapshot] = []
        for i in range(total_records):
            snapshots.append(
                StoreInventorySnapshot(
                    StoreID=int(store_ids[i]),
                    ProductID=int(prod_ids[i]),
                    CurrentQuantity=int(qty[i]),
                    ReorderPoint=int(rp[i]),
                    LastUpdated=current_time,
                )
            )

        print(f"Generated {len(snapshots):,} store inventory records (vectorized)")
        return snapshots
