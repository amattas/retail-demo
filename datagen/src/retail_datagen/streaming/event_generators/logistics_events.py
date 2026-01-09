"""
Logistics event generation mixin.

Handles truck arrivals/departures and store opening/closing events.
"""

from datetime import datetime

from retail_datagen.streaming.schemas import (
    StoreOperationPayload,
    TruckArrivedPayload,
    TruckDepartedPayload,
)


class LogisticsEventsMixin:
    """
    Mixin providing logistics-related event generation.

    Handles truck arrival and departure events for deliveries, as well as
    store opening and closing operations.

    Requires parent class to provide:
        - rng: random.Random instance
        - state: EventGenerationState
        - stores: dict[int, Store]
        - dcs: dict[int, DistributionCenter]
    """

    def _generate_truck_arrived(
        self, timestamp: datetime
    ) -> tuple[TruckArrivedPayload, str, str]:
        """Generate truck arrived event."""
        truck_id = f"TRUCK_{self.rng.randint(1000, 9999)}"

        # 70% to stores, 30% to DCs
        if self.rng.random() < 0.7:
            store_id = self.rng.choice(list(self.stores.keys()))
            dc_id = None
            partition_key = f"store_{store_id}"
        else:
            dc_id = self.rng.choice(list(self.dcs.keys()))
            store_id = None
            partition_key = f"dc_{dc_id}"

        shipment_id = f"SHIP_{int(timestamp.timestamp())}_{self.rng.randint(100, 999)}"
        estimated_unload_duration = self.rng.randint(30, 180)  # 30-180 minutes

        # Track active truck
        self.state.active_trucks[truck_id] = {
            "store_id": store_id,
            "dc_id": dc_id,
            "arrival_time": timestamp,
            "shipment_id": shipment_id,
        }

        payload = TruckArrivedPayload(
            truck_id=truck_id,
            store_id=store_id,
            dc_id=dc_id,
            shipment_id=shipment_id,
            arrival_time=timestamp,
            estimated_unload_duration=estimated_unload_duration,
        )

        return payload, truck_id, partition_key

    def _generate_truck_departed(
        self, timestamp: datetime
    ) -> tuple[TruckDepartedPayload, str, str] | None:
        """Generate truck departed event."""
        if not self.state.active_trucks:
            return None

        truck_id = self.rng.choice(list(self.state.active_trucks.keys()))
        truck_info = self.state.active_trucks[truck_id]

        actual_unload_duration = self.rng.randint(25, 200)  # Actual vs estimated

        if truck_info["store_id"]:
            partition_key = f"store_{truck_info['store_id']}"
        else:
            partition_key = f"dc_{truck_info['dc_id']}"

        payload = TruckDepartedPayload(
            truck_id=truck_id,
            store_id=truck_info["store_id"],
            dc_id=truck_info["dc_id"],
            shipment_id=truck_info["shipment_id"],
            departure_time=timestamp,
            actual_unload_duration=actual_unload_duration,
        )

        # Remove from active trucks
        del self.state.active_trucks[truck_id]

        return payload, truck_id, partition_key

    def _generate_store_opened(
        self, timestamp: datetime
    ) -> tuple[StoreOperationPayload, str, str]:
        """Generate store opened event."""
        store_id = self.rng.choice(list(self.stores.keys()))
        self.state.store_hours[store_id]["is_open"] = True

        payload = StoreOperationPayload(
            store_id=store_id, operation_time=timestamp, operation_type="opened"
        )

        return payload, f"store_ops_{store_id}", f"store_{store_id}"

    def _generate_store_closed(
        self, timestamp: datetime
    ) -> tuple[StoreOperationPayload, str, str]:
        """Generate store closed event."""
        store_id = self.rng.choice(list(self.stores.keys()))
        self.state.store_hours[store_id]["is_open"] = False
        self.state.store_hours[store_id]["current_customers"] = 0

        payload = StoreOperationPayload(
            store_id=store_id, operation_time=timestamp, operation_type="closed"
        )

        return payload, f"store_ops_{store_id}", f"store_{store_id}"
