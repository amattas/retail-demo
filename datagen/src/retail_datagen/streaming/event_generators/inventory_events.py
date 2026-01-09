"""
Inventory event generation mixin.

Handles inventory updates, stockout detection, and reorder trigger events.
"""

from datetime import datetime

from retail_datagen.shared.models import InventoryReason
from retail_datagen.streaming.schemas import (
    InventoryUpdatedPayload,
    ReorderTriggeredPayload,
    StockoutDetectedPayload,
)


class InventoryEventsMixin:
    """
    Mixin providing inventory-related event generation.

    Handles inventory level updates, stockout detection, and automatic
    reorder triggering based on inventory thresholds.

    Requires parent class to provide:
        - rng: random.Random instance
        - state: EventGenerationState
        - stores: dict[int, Store]
        - dcs: dict[int, DistributionCenter]
        - products: dict[int, ProductMaster]
    """

    def _generate_inventory_updated(
        self, timestamp: datetime
    ) -> tuple[InventoryUpdatedPayload, str, str]:
        """Generate inventory updated event."""
        # Decide between store or DC inventory
        is_store = self.rng.random() < 0.7  # 70% store, 30% DC

        if is_store:
            location_id = self.rng.choice(list(self.stores.keys()))
            store_id = location_id
            dc_id = None
            partition_key = f"store_{location_id}"
        else:
            location_id = self.rng.choice(list(self.dcs.keys()))
            store_id = None
            dc_id = location_id
            partition_key = f"dc_{location_id}"

        product_id = self.rng.choice(list(self.products.keys()))
        reason = self.rng.choice(list(InventoryReason))

        # Generate realistic quantity delta based on reason
        if reason in [
            InventoryReason.SALE,
            InventoryReason.DAMAGED,
            InventoryReason.LOST,
        ]:
            qty_delta = -self.rng.randint(1, 10)
        else:
            qty_delta = self.rng.randint(10, 100)

        # Update internal inventory tracking
        inventory_key = (location_id, product_id)
        if is_store:
            self.state.store_inventory[inventory_key] += qty_delta
        else:
            self.state.dc_inventory[inventory_key] += qty_delta

        payload = InventoryUpdatedPayload(
            store_id=store_id,
            dc_id=dc_id,
            product_id=product_id,
            quantity_delta=qty_delta,
            reason=reason.value,
            source=f"truck_{self.rng.randint(1000, 9999)}" if qty_delta > 0 else None,
        )

        return payload, f"inventory_{location_id}_{product_id}", partition_key

    def _generate_stockout_detected(
        self, timestamp: datetime
    ) -> tuple[StockoutDetectedPayload, str, str]:
        """Generate stockout detected event."""
        # Find low inventory items
        low_inventory_items = [
            (location_id, product_id, qty)
            for (location_id, product_id), qty in self.state.store_inventory.items()
            if qty <= 5
        ]

        if not low_inventory_items:
            # Generate a random stockout
            store_id = self.rng.choice(list(self.stores.keys()))
            product_id = self.rng.choice(list(self.products.keys()))
            last_known_quantity = 0
            dc_id = None
        else:
            location_id, product_id, last_known_quantity = self.rng.choice(
                low_inventory_items
            )
            store_id = location_id
            dc_id = None

        payload = StockoutDetectedPayload(
            store_id=store_id,
            dc_id=dc_id,
            product_id=product_id,
            last_known_quantity=last_known_quantity,
            detection_time=timestamp,
        )

        return payload, f"stockout_{store_id}_{product_id}", f"store_{store_id}"

    def _generate_reorder_triggered(
        self, timestamp: datetime
    ) -> tuple[ReorderTriggeredPayload, str, str]:
        """Generate reorder triggered event."""
        store_id = self.rng.choice(list(self.stores.keys()))
        product_id = self.rng.choice(list(self.products.keys()))

        current_quantity = max(
            0, self.state.store_inventory.get((store_id, product_id), 0)
        )
        reorder_point = self.rng.randint(10, 30)
        reorder_quantity = self.rng.randint(50, 200)

        priority = self.rng.choices(
            ["NORMAL", "HIGH", "URGENT"], weights=[0.7, 0.2, 0.1]
        )[0]

        payload = ReorderTriggeredPayload(
            store_id=store_id,
            dc_id=None,
            product_id=product_id,
            current_quantity=current_quantity,
            reorder_quantity=reorder_quantity,
            reorder_point=reorder_point,
            priority=priority,
        )

        return payload, f"reorder_{store_id}_{product_id}", f"store_{store_id}"
