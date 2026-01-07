"""
Customer zone change generation (derived from BLE pings)
"""

from __future__ import annotations

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class CustomerZoneChangesMixin:
    """Customer zone change tracking derived from BLE ping sequences"""

    def _generate_customer_zone_changes(
        self, ble_pings: list[dict]
    ) -> list[dict]:
        """Derive customer zone change events from BLE ping sequences.

        Args:
            ble_pings: List of BLE ping records for an hour

        Returns:
            List of zone change records with event_ts, store_id, customer_ble_id,
            from_zone, to_zone

        Note:
            Zone changes are derived by tracking each customer's zone transitions
            based on their BLE ping sequence. Only actual zone changes are recorded
            (same-zone consecutive pings are filtered out).
        """
        if not ble_pings:
            return []

        # Group BLE pings by customer and store, then sort by timestamp
        customer_pings = {}
        for ping in ble_pings:
            store_id = ping.get("StoreID")
            customer_ble_id = ping.get("CustomerBLEId")
            event_ts = ping.get("EventTS")
            zone = ping.get("Zone")

            if not all([store_id, customer_ble_id, event_ts, zone]):
                continue

            key = (store_id, customer_ble_id)
            if key not in customer_pings:
                customer_pings[key] = []

            customer_pings[key].append(
                {
                    "event_ts": event_ts,
                    "zone": zone,
                }
            )

        # Generate zone changes for each customer
        zone_changes = []
        for (store_id, customer_ble_id), pings in customer_pings.items():
            # Sort pings by timestamp
            sorted_pings = sorted(pings, key=lambda p: p["event_ts"])

            # Track zone transitions
            previous_zone = None
            for ping in sorted_pings:
                current_zone = ping["zone"]
                event_ts = ping["event_ts"]

                # Only record actual zone changes (not same-zone consecutive pings)
                if previous_zone is not None and previous_zone != current_zone:
                    zone_changes.append(
                        {
                            "TraceId": self._generate_trace_id(),
                            "EventTS": event_ts,
                            "StoreID": store_id,
                            "CustomerBLEId": customer_ble_id,
                            "FromZone": previous_zone,
                            "ToZone": current_zone,
                        }
                    )

                previous_zone = current_zone

        return zone_changes
