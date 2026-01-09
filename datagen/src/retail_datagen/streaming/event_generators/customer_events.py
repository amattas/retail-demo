"""
Customer event generation mixin.

Handles customer entry, zone changes, and BLE ping detection events.
"""

from datetime import datetime, timedelta

from retail_datagen.streaming.schemas import (
    BLEPingDetectedPayload,
    CustomerEnteredPayload,
    CustomerZoneChangedPayload,
)


class CustomerEventsMixin:
    """
    Mixin providing customer-related event generation.

    Handles customer store entry, zone changes within stores, and BLE beacon
    ping detection for location tracking.

    Requires parent class to provide:
        - rng: random.Random instance
        - state: EventGenerationState
        - stores: dict[int, Store]
        - customers: dict[int, Customer]
    """

    def _generate_customer_entered(
        self, timestamp: datetime
    ) -> tuple[CustomerEnteredPayload, str, str]:
        """Generate customer entered event.

        Includes session tracking and marketing conversions.
        """
        store_id = self.rng.choice(list(self.stores.keys()))
        sensor_id = f"SENSOR_{store_id}_1"  # Use store-specific entrance sensor
        zone = "ENTRANCE"  # Always start at entrance

        # Check for marketing-driven visits first
        marketing_driven_customers = []
        for impression_id, conversion in self.state.marketing_conversions.items():
            if (
                not conversion["converted"]
                and timestamp >= conversion["scheduled_visit_time"]
                and timestamp <= conversion["scheduled_visit_time"] + timedelta(hours=2)
            ):  # 2-hour window
                customer_id = conversion["customer_id"]
                customer = self.customers.get(customer_id)
                session_key = f"{customer_id}_{store_id}"

                if customer and session_key not in self.state.customer_sessions:
                    marketing_driven_customers.append(
                        (customer, impression_id, conversion)
                    )

        # Select customers (prioritize marketing-driven visits)
        available_customers = [
            cust
            for cust in self.customers.values()
            if f"{cust.ID}_{store_id}" not in self.state.customer_sessions
        ]

        entering_customers = []

        # First, add marketing-driven customers
        if marketing_driven_customers:
            # Select 1-2 marketing-driven customers
            selected_marketing = self.rng.sample(
                marketing_driven_customers,
                min(self.rng.randint(1, 2), len(marketing_driven_customers)),
            )
            for customer, impression_id, conversion in selected_marketing:
                entering_customers.append(customer)
                # Mark conversion as completed
                conversion["converted"] = True
                conversion["actual_visit_time"] = timestamp

        # Then add random customers if needed
        remaining_slots = self.rng.randint(1, 3) - len(entering_customers)
        if remaining_slots > 0 and available_customers:
            random_customers = [
                c for c in available_customers if c not in entering_customers
            ]
            if random_customers:
                additional_customers = self.rng.sample(
                    random_customers, min(remaining_slots, len(random_customers))
                )
                entering_customers.extend(additional_customers)

        customer_count = len(entering_customers)
        if customer_count == 0:
            # Fallback: generic foot traffic event
            customer_count = self.rng.randint(1, 2)
        else:
            # Create customer sessions
            for customer in entering_customers:
                session_id = f"{customer.ID}_{store_id}"
                base_visit_duration = self.rng.randint(10, 45)

                # Marketing-driven customers tend to stay longer and are more
                # likely to purchase
                is_marketing_driven = any(
                    customer == mc[0]
                    for mc in marketing_driven_customers
                    if mc[0] in entering_customers
                )

                if is_marketing_driven:
                    # 20% longer visit, higher purchase intent
                    visit_duration = int(base_visit_duration * 1.2)
                    purchase_likelihood = 0.8  # 80% likely to purchase
                else:
                    visit_duration = base_visit_duration
                    purchase_likelihood = 0.4  # 40% likely to purchase

                self.state.customer_sessions[session_id] = {
                    "customer_id": customer.ID,
                    "customer_ble_id": customer.BLEId,
                    "store_id": store_id,
                    "entered_at": timestamp,
                    "current_zone": "ENTRANCE",
                    "has_made_purchase": False,
                    "expected_exit_time": timestamp + timedelta(minutes=visit_duration),
                    "marketing_driven": is_marketing_driven,
                    "purchase_likelihood": purchase_likelihood,
                }

        # Update store customer count
        self.state.store_hours[store_id]["current_customers"] += customer_count

        payload = CustomerEnteredPayload(
            store_id=store_id,
            sensor_id=sensor_id,
            zone=zone,
            customer_count=customer_count,
            dwell_time=0,
        )

        return payload, f"foottraffic_{store_id}", f"store_{store_id}"

    def _generate_customer_zone_changed(
        self, timestamp: datetime
    ) -> tuple[CustomerZoneChangedPayload, str, str] | None:
        """Generate customer zone changed event."""
        store_id = self.rng.choice(list(self.stores.keys()))
        customer = self.rng.choice(list(self.customers.values()))
        customer_ble_id = customer.BLEId

        zones = ["ENTRANCE", "ELECTRONICS", "GROCERY", "CLOTHING", "CHECKOUT"]
        from_zone = self.rng.choice(zones)
        to_zone = self.rng.choice([z for z in zones if z != from_zone])

        payload = CustomerZoneChangedPayload(
            store_id=store_id,
            customer_ble_id=customer_ble_id,
            from_zone=from_zone,
            to_zone=to_zone,
            timestamp=timestamp,
        )

        return payload, customer_ble_id, f"store_{store_id}"

    def _generate_ble_ping_detected(
        self, timestamp: datetime
    ) -> tuple[BLEPingDetectedPayload, str, str] | None:
        """Generate BLE ping detected event - only for customers currently in store."""
        # Get customers who are currently in stores
        active_sessions = [
            session
            for session in self.state.customer_sessions.values()
            if timestamp < session["expected_exit_time"]
        ]

        if not active_sessions:
            return None  # No customers in any store

        session = self.rng.choice(active_sessions)
        store_id = session["store_id"]
        customer_ble_id = session["customer_ble_id"]
        current_zone = session["current_zone"]

        # Use appropriate beacon for the zone
        beacon_id = f"BEACON_{store_id}_{current_zone}"
        rssi = self.rng.randint(-80, -30)  # Typical RSSI range

        # Occasionally move customer to a different zone (20% chance)
        zones = ["ENTRANCE", "ELECTRONICS", "GROCERY", "CLOTHING", "CHECKOUT"]
        if self.rng.random() < 0.2:
            new_zone = self.rng.choice([z for z in zones if z != current_zone])
            session["current_zone"] = new_zone
            current_zone = new_zone
            beacon_id = f"BEACON_{store_id}_{new_zone}"

        payload = BLEPingDetectedPayload(
            store_id=store_id,
            beacon_id=beacon_id,
            customer_ble_id=customer_ble_id,
            rssi=rssi,
            zone=current_zone,
        )

        return payload, customer_ble_id, f"store_{store_id}"

    def _cleanup_expired_sessions(self, timestamp: datetime) -> None:
        """Clean up expired customer sessions and update store occupancy."""
        expired_sessions = []

        for session_id, session in self.state.customer_sessions.items():
            if timestamp >= session["expected_exit_time"]:
                expired_sessions.append(session_id)

                # Decrease store occupancy count
                store_id = session["store_id"]
                if store_id in self.state.store_hours:
                    current_count = self.state.store_hours[store_id][
                        "current_customers"
                    ]
                    self.state.store_hours[store_id]["current_customers"] = max(
                        0, current_count - 1
                    )

        # Remove expired sessions
        for session_id in expired_sessions:
            del self.state.customer_sessions[session_id]

        # Clean up old marketing conversions (older than 72 hours)
        expired_conversions = []
        cutoff_time = timestamp - timedelta(hours=72)

        for impression_id, conversion in self.state.marketing_conversions.items():
            scheduled_time = conversion["scheduled_visit_time"]
            if scheduled_time < cutoff_time:
                expired_conversions.append(impression_id)

        for impression_id in expired_conversions:
            conversion = self.state.marketing_conversions[impression_id]
            customer_id = conversion["customer_id"]
            # Clean up O(1) lookup index
            self.state.customer_to_campaign.pop(customer_id, None)
            del self.state.marketing_conversions[impression_id]
