"""
Daily fact data generation orchestration.

Contains the main orchestration logic for generating all fact data
for a single day, calling out to specialized generators for each
data type.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime
from typing import TYPE_CHECKING

from retail_datagen.shared.models import InventoryReason

from .base_types import FactGeneratorBase

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class DailyFactsMixin(FactGeneratorBase):
    """Orchestrates daily fact data generation across all fact tables.

    This mixin contains the main _generate_daily_facts method that coordinates
    generation of all fact data for a single day, calling specialized generators
    for each data type (receipts, inventory, trucks, marketing, etc.).

    The method is organized into numbered sections:
    1. DC inventory transactions (supplier deliveries)
    2. Marketing campaigns and impressions
    3. Hourly store operations (receipts, foot traffic, BLE pings)
    4. Truck movements and lifecycle
    5. Online orders and fulfillment
    6. Store operations (open/close events)
    7. Supply chain disruptions
    8. Inventory adjustments and stockouts
    9. Return receipts

    Each section generates data, inserts it to the database, and updates
    progress tracking.
    """

    async def _generate_daily_facts(
        self,
        date: datetime,
        active_tables: list[str],
        day_index: int,
        total_days: int,
    ) -> dict[str, list[dict]]:
        """
        Generate all fact data for a single day.

        Hourly progress updates are sent after each hour's data is exported, with
        thread-safe locking to prevent race conditions.

        Args:
            date: Date to generate facts for
            active_tables: List of fact tables to generate
            day_index: Current day number (1-indexed)
            total_days: Total number of days being generated

        Returns:
            Dictionary of fact tables with their records

        Note:
            Now async to support database operations during hourly exports.
        """
        daily_facts = {t: [] for t in active_tables}

        # Update available products for this date
        available_products = self._get_available_products_for_date(date)
        if self.customer_journey_sim:
            self.customer_journey_sim.update_available_products(available_products)

        # Generate base activity level for the day
        base_multiplier = self.temporal_patterns.get_overall_multiplier(date)

        # Phase 1: Run independent sections in parallel
        # These sections have no shared state dependencies:
        # - DC inventory: writes to _dc_inventory and daily_facts["dc_inventory_txn"]
        # - Marketing: writes only to daily_facts["marketing"]
        # - Store ops: writes only to daily_facts["store_ops"]
        # Note: asyncio is single-threaded cooperative multitasking, so dict
        # operations are safe (only one coroutine runs at a time).
        await asyncio.gather(
            self._generate_dc_inventory_section(
                date, base_multiplier, active_tables, daily_facts, day_index, total_days
            ),
            self._generate_marketing_section(
                date, active_tables, daily_facts, day_index, total_days
            ),
            self._generate_store_ops_section(
                date, active_tables, daily_facts, day_index, total_days
            ),
        )

        # Supply chain disruptions (sync, writes to daily_facts["supply_chain"])
        self._generate_supply_chain_section(date, active_tables, daily_facts)

        # Phase 2: Hourly store section (generates core data others depend on)
        # Must complete before truck movements, online orders, stockouts, returns
        await self._generate_hourly_store_section(
            date, active_tables, daily_facts, day_index, total_days
        )

        # 4. Generate truck movements (based on inventory needs)
        await self._generate_truck_movements_section(
            date, active_tables, daily_facts, day_index, total_days
        )

        # 4a. Generate truck inventory tracking events
        await self._generate_truck_inventory_section(
            date, active_tables, daily_facts, day_index, total_days
        )

        # 5. Legacy delivery processing - now handled by _process_truck_lifecycle
        # Kept for backward compatibility but will be empty since lifecycle handles it
        if "store_inventory_txn" in active_tables:
            base_truck_moves = daily_facts.get("truck_moves", [])
            delivery_transactions = self._process_truck_deliveries(
                date, base_truck_moves
            )
            if delivery_transactions:
                logger.debug(
                    f"Legacy delivery processing added "
                    f"{len(delivery_transactions)} transactions"
                )
                # Skip adding these since _process_truck_lifecycle handles it

        # Phase 3: Online orders (touches inventory, must be after hourly store)
        await self._generate_online_orders_section(
            date, active_tables, daily_facts, day_index, total_days
        )

        # Phase 4: Post-processing sections
        # Small store inventory adjustments for audit realism
        await self._generate_inventory_adjustments_section(
            date, active_tables, day_index, total_days
        )

        # 8.5. Generate stockout events from inventory transactions (Issue #8)
        await self._generate_stockouts_section(
            date, active_tables, daily_facts, day_index, total_days
        )

        # 9. Generate return receipts and inventory effects
        try:
            if getattr(self, "_use_duckdb", False):
                await self._generate_and_insert_returns_duckdb(date, active_tables)
            else:
                await self._generate_and_insert_returns(date, active_tables)
        except Exception as e:
            logger.warning(
                f"Return generation failed for {date.strftime('%Y-%m-%d')}: {e}"
            )

        return daily_facts

    async def _generate_dc_inventory_section(
        self,
        date: datetime,
        base_multiplier: float,
        active_tables: list[str],
        daily_facts: dict[str, list[dict]],
        day_index: int,
        total_days: int,
    ) -> None:
        """Generate DC inventory transactions (supplier deliveries)."""
        if "dc_inventory_txn" not in active_tables:
            return

        dc_transactions = self._generate_dc_inventory_transactions(
            date, base_multiplier
        )
        daily_facts["dc_inventory_txn"].extend(dc_transactions)

        # Insert daily DC transactions immediately (not hourly)
        if dc_transactions:
            try:
                await self._insert_hourly_to_db(
                    self._session,
                    "dc_inventory_txn",
                    dc_transactions,
                    hour=0,
                    commit_every_batches=0,
                )
                # Update progress for this daily-generated table
                for hour in range(24):
                    self.hourly_tracker.update_hourly_progress(
                        "dc_inventory_txn", day_index, hour, total_days
                    )
            except Exception as e:
                logger.error(
                    f"Failed to insert dc_inventory_txn for "
                    f"{date.strftime('%Y-%m-%d')}: {e}"
                )

    async def _generate_marketing_section(
        self,
        date: datetime,
        active_tables: list[str],
        daily_facts: dict[str, list[dict]],
        day_index: int,
        total_days: int,
    ) -> None:
        """Generate marketing campaigns and impressions."""
        if "marketing" not in active_tables:
            return

        # Digital marketing runs 24/7 independently of store traffic/hours
        marketing_boost = 1.0
        try:
            marketing_boost = self._compute_marketing_multiplier(date)
        except Exception as e:
            logger.warning(
                f"Failed to compute marketing multiplier for {date}, "
                f"using default 1.0: {e}"
            )

        marketing_records = self._generate_marketing_activity(date, marketing_boost)
        if marketing_records:
            logger.debug(
                f"Generated {len(marketing_records)} marketing records "
                f"for {date.strftime('%Y-%m-%d')}"
            )
        daily_facts["marketing"].extend(marketing_records)

        # Update marketing progress (treated as completing all 24 hours)
        for hour in range(24):
            self.hourly_tracker.update_hourly_progress(
                table="marketing", day=day_index, hour=hour, total_days=total_days
            )

        # Insert marketing records for the day directly (not hourly)
        if marketing_records:
            try:
                await self._insert_hourly_to_db(
                    self._session,
                    "marketing",
                    marketing_records,
                    hour=0,
                    commit_every_batches=0,
                )
                # Update progress for this daily-generated table
                for hour in range(24):
                    self.hourly_tracker.update_hourly_progress(
                        "marketing", day_index, hour, total_days
                    )
                # Verify daily insert size once for marketing
                try:
                    from sqlalchemy import func, select

                    model = self._get_model_for_table("marketing")
                    total_db = (
                        await self._session.execute(
                            select(func.count()).select_from(model)
                        )
                    ).scalar() or 0
                    logger.info(
                        f"marketing verification (daily): "
                        f"inserted={len(marketing_records)}, "
                        f"db_total={int(total_db)}"
                    )
                except Exception as ve:
                    logger.debug(f"Marketing verification skipped: {ve}")
            except Exception as e:
                logger.error(
                    f"Failed to insert marketing for {date.strftime('%Y-%m-%d')}: {e}"
                )

    async def _generate_hourly_store_section(
        self,
        date: datetime,
        active_tables: list[str],
        daily_facts: dict[str, list[dict]],
        day_index: int,
        total_days: int,
    ) -> None:
        """Generate store operations hour-by-hour to minimize memory."""
        hourly_generated_tables = [
            "receipts",
            "receipt_lines",
            "store_inventory_txn",
            "foot_traffic",
            "ble_pings",
            "fact_payments",
            "customer_zone_changes",
            "promotions",
            "promo_lines",
        ]

        logger.debug(
            f"Day {day_index}/{total_days} ({date.strftime('%Y-%m-%d')}): "
            f"Processing 24 hours"
        )

        # Generate and export each hour immediately
        for hour_idx in range(24):
            hour_datetime = date.replace(
                hour=hour_idx, minute=0, second=0, microsecond=0
            )
            hour_multiplier = self.temporal_patterns.get_overall_multiplier(
                hour_datetime
            )

            # Early heartbeat for this hour before heavy generation
            try:
                if self._progress_callback:
                    progress_state = self.hourly_tracker.get_current_progress()
                    self._send_throttled_progress_update(
                        day_counter=day_index,
                        message=(
                            f"Preparing hour {hour_idx + 1}/24 for "
                            f"{date.strftime('%Y-%m-%d')}"
                        ),
                        total_days=total_days,
                        table_progress=progress_state.get("per_table_progress", {}),
                        tables_in_progress=progress_state.get("tables_in_progress", []),
                    )
            except Exception as e:
                logger.debug(
                    f"Failed to send progress update during hourly generation: {e}"
                )

            hour_data = self._generate_hour_data(date, hour_idx, hour_multiplier)

            hourly_subset = {
                t: (hour_data.get(t, []) if t in active_tables else [])
                for t in active_tables
            }

            try:
                await self._export_hourly_facts(date, hour_idx, hourly_subset)

                # Update hourly progress tracker after successful export
                for table in hourly_generated_tables:
                    if table in active_tables:
                        self.hourly_tracker.update_hourly_progress(
                            table=table,
                            day=day_index,
                            hour=hour_idx,
                            total_days=total_days,
                        )
                        if table == "receipts":
                            logger.debug(
                                f"Receipts progress updated: day={day_index}, "
                                f"hour={hour_idx}, total_days={total_days}"
                            )

                # Send progress update after hourly exports complete (throttled)
                if self._progress_callback:
                    progress_data = self.hourly_tracker.get_current_progress()
                    table_progress = progress_data.get("per_table_progress", {})
                    thread_name = threading.current_thread().name
                    logger.debug(
                        f"[{thread_name}] Sending hourly progress: "
                        f"day {day_index}/{total_days}, hour {hour_idx + 1}/24"
                    )
                    self._send_throttled_progress_update(
                        day_counter=day_index,
                        message=(
                            f"Generating {date.strftime('%Y-%m-%d')} "
                            f"(day {day_index}/{total_days}, hour {hour_idx + 1}/24)"
                        ),
                        total_days=total_days,
                        table_progress=table_progress,
                        tables_in_progress=progress_data.get("tables_in_progress", []),
                    )
            except Exception as e:
                logger.error(f"Hourly export failed for {date} hour {hour_idx}: {e}")

            for fact_type, records in hour_data.items():
                if fact_type in active_tables:
                    daily_facts[fact_type].extend(records)

    def _generate_hour_data(
        self, date: datetime, hour_idx: int, hour_multiplier: float
    ) -> dict[str, list[dict]]:
        """Generate data for a specific hour."""
        if hour_multiplier == 0:  # Store closed
            return {
                "receipts": [],
                "receipt_lines": [],
                "store_inventory_txn": [],
                "foot_traffic": [],
                "ble_pings": [],
                "fact_payments": [],
                "customer_zone_changes": [],
                "promotions": [],
                "promo_lines": [],
            }

        hour_data: dict[str, list[dict]] = {
            "receipts": [],
            "receipt_lines": [],
            "store_inventory_txn": [],
            "foot_traffic": [],
            "ble_pings": [],
            "fact_payments": [],
            "customer_zone_changes": [],
            "promotions": [],
            "promo_lines": [],
        }

        hour_datetime = date.replace(hour=hour_idx, minute=0, second=0, microsecond=0)

        # Generate customer transactions for each store for this hour
        for store in self.stores:
            store_hour_data = self._generate_store_hour_activity(
                store, hour_datetime, hour_multiplier
            )
            for fact_type, records in store_hour_data.items():
                hour_data[fact_type].extend(records)

        return hour_data

    async def _generate_truck_movements_section(
        self,
        date: datetime,
        active_tables: list[str],
        daily_facts: dict[str, list[dict]],
        day_index: int,
        total_days: int,
    ) -> None:
        """Generate truck movements based on inventory needs."""
        if "truck_moves" not in active_tables:
            return

        base_store_txn = daily_facts.get("store_inventory_txn", [])
        truck_movements, reorder_records = self._generate_truck_movements(
            date, base_store_txn
        )
        daily_facts["truck_moves"].extend(truck_movements)

        # Add reorder records to daily facts and insert to DuckDB
        if "reorders" in active_tables and reorder_records:
            daily_facts["reorders"].extend(reorder_records)
            try:
                await self._insert_hourly_to_db(
                    self._session,
                    "reorders",
                    reorder_records,
                    hour=0,
                    commit_every_batches=0,
                )
                # Update progress for this daily-generated table
                for hour in range(24):
                    self.hourly_tracker.update_hourly_progress(
                        "reorders", day_index, hour, total_days
                    )
            except Exception as e:
                logger.error(
                    f"Failed to insert reorders for {date.strftime('%Y-%m-%d')}: {e}"
                )

        # Process all active shipments and generate status progression
        truck_lifecycle_records, dc_outbound_txn, store_inbound_txn = (
            self._process_truck_lifecycle(date)
        )

        # Add truck status progression records
        daily_facts["truck_moves"].extend(truck_lifecycle_records)

        # Add DC outbound transactions (when trucks are loaded)
        if "dc_inventory_txn" in active_tables and dc_outbound_txn:
            daily_facts["dc_inventory_txn"].extend(dc_outbound_txn)
            try:
                await self._insert_hourly_to_db(
                    self._session,
                    "dc_inventory_txn",
                    dc_outbound_txn,
                    hour=0,
                    commit_every_batches=0,
                )
                for hour in range(24):
                    self.hourly_tracker.update_hourly_progress(
                        "dc_inventory_txn", day_index, hour, total_days
                    )
            except Exception as e:
                logger.error(
                    f"Failed to insert lifecycle dc_inventory_txn for "
                    f"{date.strftime('%Y-%m-%d')}: {e}"
                )

        # Add store inbound transactions (when trucks are unloaded)
        if "store_inventory_txn" in active_tables and store_inbound_txn:
            daily_facts["store_inventory_txn"].extend(store_inbound_txn)
            try:
                await self._insert_hourly_to_db(
                    self._session,
                    "store_inventory_txn",
                    store_inbound_txn,
                    hour=0,
                    commit_every_batches=0,
                )
                for hour in range(24):
                    self.hourly_tracker.update_hourly_progress(
                        "store_inventory_txn", day_index, hour, total_days
                    )
            except Exception as e:
                logger.error(
                    f"Failed to insert lifecycle store_inventory_txn for "
                    f"{date.strftime('%Y-%m-%d')}: {e}"
                )

        # Write all truck_moves records (including lifecycle progression)
        all_truck_moves = daily_facts.get("truck_moves", [])
        if all_truck_moves:
            try:
                await self._insert_hourly_to_db(
                    self._session,
                    "truck_moves",
                    all_truck_moves,
                    hour=0,
                    commit_every_batches=0,
                )
                for hour in range(24):
                    self.hourly_tracker.update_hourly_progress(
                        "truck_moves", day_index, hour, total_days
                    )
            except Exception as e:
                logger.error(
                    f"Failed to insert truck_moves for {date.strftime('%Y-%m-%d')}: {e}"
                )

    async def _generate_truck_inventory_section(
        self,
        date: datetime,
        active_tables: list[str],
        daily_facts: dict[str, list[dict]],
        day_index: int,
        total_days: int,
    ) -> None:
        """Generate truck inventory tracking events."""
        if "truck_inventory" not in active_tables:
            return

        truck_inventory_events = self.inventory_flow_sim.track_truck_inventory_status(
            date
        )
        truck_inventory_records = []
        for event in truck_inventory_events:
            record = {
                "TraceId": self._generate_trace_id(),
                "EventTS": self._randomize_time_within_day(event["EventTS"]),
                "TruckId": event["TruckId"],
                "ShipmentId": event["ShipmentId"],
                "ProductID": event["ProductID"],
                "Quantity": event["Quantity"],
                "Action": event["Action"],
                "LocationID": event["LocationID"],
                "LocationType": event["LocationType"],
            }
            daily_facts["truck_inventory"].append(record)
            truck_inventory_records.append(record)

        # Insert truck inventory tracking events immediately (not hourly)
        if truck_inventory_records:
            try:
                await self._insert_hourly_to_db(
                    self._session,
                    "truck_inventory",
                    truck_inventory_records,
                    hour=0,
                    commit_every_batches=0,
                )
                # Update progress for this daily-generated table
                for hour in range(24):
                    self.hourly_tracker.update_hourly_progress(
                        "truck_inventory", day_index, hour, total_days
                    )
            except Exception as e:
                logger.error(
                    f"Failed to insert truck_inventory for "
                    f"{date.strftime('%Y-%m-%d')}: {e}"
                )

    async def _generate_online_orders_section(
        self,
        date: datetime,
        active_tables: list[str],
        daily_facts: dict[str, list[dict]],
        day_index: int,
        total_days: int,
    ) -> None:
        """Generate online orders and integrate inventory effects."""
        if "online_orders" not in active_tables:
            return

        online_orders, online_store_txn, online_dc_txn, online_order_lines = (
            self._generate_online_orders(date)
        )
        daily_facts["online_orders"].extend(online_orders)

        # Generate payments for online orders
        online_order_payments: list[dict] = []
        if "fact_payments" in active_tables and online_orders:
            for order in online_orders:
                payment = self._generate_payment_for_online_order(
                    order, order.get("EventTS", date)
                )
                online_order_payments.append(payment)

        # First write online order headers (so lines can resolve order_id)
        if online_orders:
            try:
                await self._insert_hourly_to_db(
                    self._session,
                    "online_orders",
                    online_orders,
                    hour=0,
                    commit_every_batches=0,
                )
                for hour in range(24):
                    self.hourly_tracker.update_hourly_progress(
                        "online_orders", day_index, hour, total_days
                    )
            except Exception as e:
                logger.error(
                    f"Failed to insert online_orders for "
                    f"{date.strftime('%Y-%m-%d')}: {e}"
                )

        # Then write online order lines (daily batch)
        if online_order_lines:
            try:
                await self._insert_hourly_to_db(
                    self._session,
                    "online_order_lines",
                    online_order_lines,
                    hour=0,
                    commit_every_batches=0,
                )
                for hour in range(24):
                    self.hourly_tracker.update_hourly_progress(
                        "online_order_lines", day_index, hour, total_days
                    )
            except Exception as e:
                logger.error(
                    f"Failed to insert online_order_lines for "
                    f"{date.strftime('%Y-%m-%d')}: {e}"
                )

        # Insert online order payments
        if online_order_payments:
            try:
                await self._insert_hourly_to_db(
                    self._session,
                    "fact_payments",
                    online_order_payments,
                    hour=0,
                    commit_every_batches=0,
                )
                for hour in range(24):
                    self.hourly_tracker.update_hourly_progress(
                        "fact_payments", day_index, hour, total_days
                    )
            except Exception as e:
                logger.error(
                    f"Failed to insert online order payments for "
                    f"{date.strftime('%Y-%m-%d')}: {e}"
                )

        # Cascade inventory effects
        if "store_inventory_txn" in active_tables and online_store_txn:
            daily_facts["store_inventory_txn"].extend(online_store_txn)
        if "dc_inventory_txn" in active_tables and online_dc_txn:
            daily_facts["dc_inventory_txn"].extend(online_dc_txn)

    async def _generate_store_ops_section(
        self,
        date: datetime,
        active_tables: list[str],
        daily_facts: dict[str, list[dict]],
        day_index: int,
        total_days: int,
    ) -> None:
        """Generate store operations (open/close events)."""
        if "store_ops" not in active_tables:
            return

        store_ops_records = []
        for store in self.stores:
            store_ops = self._generate_store_operations_for_day(store, date)
            store_ops_records.extend(store_ops)

        daily_facts["store_ops"].extend(store_ops_records)

        # Insert store operations immediately (daily batch)
        if store_ops_records:
            try:
                await self._insert_hourly_to_db(
                    self._session,
                    "store_ops",
                    store_ops_records,
                    hour=0,
                    commit_every_batches=0,
                )
                for hour in range(24):
                    self.hourly_tracker.update_hourly_progress(
                        "store_ops", day_index, hour, total_days
                    )
            except Exception as e:
                logger.error(
                    f"Failed to insert store_ops for {date.strftime('%Y-%m-%d')}: {e}"
                )

    def _generate_supply_chain_section(
        self,
        date: datetime,
        active_tables: list[str],
        daily_facts: dict[str, list[dict]],
    ) -> None:
        """Generate supply chain disruptions."""
        if "supply_chain_disruption" not in active_tables:
            return

        disruption_events = self.inventory_flow_sim.simulate_supply_chain_disruptions(
            date
        )
        for disruption in disruption_events:
            daily_facts["supply_chain_disruption"].append(
                {
                    "TraceId": self._generate_trace_id(),
                    "EventTS": self._randomize_time_within_day(disruption["EventTS"]),
                    "DCID": disruption["DCID"],
                    "Type": disruption["DisruptionType"].value,
                    "Severity": disruption["Severity"].value,
                    "Description": disruption["Description"],
                    "StartTime": disruption["StartTime"],
                    "EndTime": disruption["EndTime"],
                    "ImpactPercentage": disruption["ImpactPercentage"],
                    "AffectedProducts": disruption["AffectedProducts"],
                }
            )

    async def _generate_inventory_adjustments_section(
        self,
        date: datetime,
        active_tables: list[str],
        day_index: int,
        total_days: int,
    ) -> None:
        """Generate small store inventory adjustments for audit realism."""
        if "store_inventory_txn" not in active_tables:
            return

        try:
            adjustments: list[dict] = []
            # Create a few random adjustments per day across the network
            # ~0.02% of store-product combinations touched (bounded)
            num_stores = len(self.stores)
            samples = max(1, int(num_stores * 0.10))  # ~10% of stores per day
            sampled_stores = (
                self._rng.sample(self.stores, min(samples, num_stores))
                if self.stores
                else []
            )

            for st in sampled_stores:
                # Pick 1-3 random products to adjust
                k = self._rng.randint(1, 3)
                prods = self._rng.sample(self.products, k) if self.products else []
                for p in prods:
                    # ±1 to ±5 units
                    delta = self._rng.randint(-5, 5)
                    if delta == 0:
                        continue
                    # Update in-memory balance
                    key = (st.ID, p.ID)
                    cur = self.inventory_flow_sim._store_inventory.get(key, 0)
                    new_bal = max(0, cur + delta)
                    self.inventory_flow_sim._store_inventory[key] = new_bal
                    adjustments.append(
                        {
                            "TraceId": self._generate_trace_id(),
                            "EventTS": date.replace(hour=22, minute=0, second=0),
                            "StoreID": st.ID,
                            "ProductID": p.ID,
                            "QtyDelta": delta,
                            "Reason": InventoryReason.ADJUSTMENT.value,
                            "Source": "CYCLE_COUNT",
                            "Balance": new_bal,
                        }
                    )

            if adjustments:
                await self._insert_hourly_to_db(
                    self._session,
                    "store_inventory_txn",
                    adjustments,
                    hour=22,
                    commit_every_batches=0,
                )
                for hour in range(22, 24):
                    self.hourly_tracker.update_hourly_progress(
                        "store_inventory_txn", day_index, hour, total_days
                    )
        except Exception as e:
            logger.warning(
                f"Adjustment generation failed for {date.strftime('%Y-%m-%d')}: {e}"
            )

    async def _generate_stockouts_section(
        self,
        date: datetime,
        active_tables: list[str],
        daily_facts: dict[str, list[dict]],
        day_index: int,
        total_days: int,
    ) -> None:
        """Generate stockout events from inventory transactions."""
        if "stockouts" not in active_tables:
            return

        try:
            # Collect all inventory transactions generated today
            store_inv_txns = daily_facts.get("store_inventory_txn", [])
            dc_inv_txns = daily_facts.get("dc_inventory_txn", [])

            # Detect stockouts from inventory transactions
            stockout_records = self._generate_stockouts_from_inventory_txns(
                store_inv_txns, dc_inv_txns
            )

            # Add to daily facts
            daily_facts["stockouts"].extend(stockout_records)

            # Insert stockouts immediately (daily batch)
            if stockout_records:
                await self._insert_hourly_to_db(
                    self._session,
                    "stockouts",
                    stockout_records,
                    hour=0,
                    commit_every_batches=0,
                )
                # Mark all hours complete for this daily-generated table
                for hour in range(24):
                    self.hourly_tracker.update_hourly_progress(
                        "stockouts", day_index, hour, total_days
                    )
        except Exception as e:
            logger.warning(
                f"Stockout generation failed for {date.strftime('%Y-%m-%d')}: {e}"
            )
