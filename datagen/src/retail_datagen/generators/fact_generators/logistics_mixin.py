"""
Truck logistics including movements, lifecycle, and deliveries
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from retail_datagen.shared.models import (
    TruckStatus,
)

logger = logging.getLogger(__name__)


def _is_beyond_end_date(event_ts: datetime | None, end_date: datetime | None) -> bool:
    """Check if event timestamp is beyond the generation end date.

    Args:
        event_ts: The event timestamp to check
        end_date: The generation end date (end of day is considered inclusive)

    Returns:
        True if event_ts is beyond end_date (next day or later)
    """
    if event_ts is None or end_date is None:
        return False
    # Normalize to end of day for comparison (events on end_date are OK)
    end_of_day = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    return event_ts > end_of_day


class LogisticsMixin:
    """Truck logistics including movements, lifecycle, and deliveries"""

    def _generate_truck_movements(
        self, date: datetime, store_transactions: list[dict]
    ) -> tuple[list[dict], list[dict]]:
        """Generate truck movements based on store inventory needs.

        This method creates initial shipments in SCHEDULED status.
        The _process_truck_lifecycle method will handle status progression.

        Shipments with departure times beyond the generation end date are
        stored in a staging table for later processing when the next day is generated.

        Returns:
            Tuple of (truck_movements, reorder_records)
        """
        truck_movements = []
        reorder_records = []
        pending_shipments = []  # Shipments with future departure times

        # Get generation end date for filtering future shipments
        generation_end_date = getattr(self, "_generation_end_date", None)

        # Use current day as the cutoff for staging - shipments with departure
        # beyond today should be staged, not just beyond generation_end_date
        # This prevents "stuck in SCHEDULED" warnings on subsequent days
        current_day_end = date.replace(
            hour=23, minute=59, second=59, microsecond=999999
        )

        # Restore pending shipments from staging that are ready for this day
        # These are shipments that were deferred from earlier generation runs
        restored_movements = self._restore_pending_shipments_for_date(date)
        if restored_movements:
            truck_movements.extend(restored_movements)
            logger.info(
                f"Restored {len(restored_movements)} pending shipments for {date.date()}"
            )

        # Analyze store inventory needs (vectorized when possible)
        store_demands: dict[int, int] = {}
        if not store_transactions:
            store_demands = {}
        else:
            try:
                import pandas as _pd

                df = _pd.DataFrame(store_transactions)
                if not df.empty and {"StoreID", "QtyDelta"}.issubset(df.columns):
                    demands = (
                        df.loc[df["QtyDelta"] < 0, ["StoreID", "QtyDelta"]]
                        .assign(QtyDelta=lambda x: x["QtyDelta"].abs())
                        .groupby("StoreID", as_index=False)["QtyDelta"]
                        .sum()
                    )
                    store_demands = {
                        int(row.StoreID): int(row.QtyDelta)
                        for row in demands.itertuples(index=False)
                    }
                else:
                    # Fallback to loop if schema unexpected
                    for tr in store_transactions:
                        if tr.get("QtyDelta", 0) < 0:
                            sid = tr.get("StoreID")
                            if sid is None:
                                continue
                            store_demands[sid] = store_demands.get(sid, 0) + abs(
                                int(tr["QtyDelta"])
                            )
            except Exception as e:
                logger.debug(
                    f"Failed to aggregate store demands via pandas, using fallback: {e}"
                )
                for tr in store_transactions:
                    if tr.get("QtyDelta", 0) < 0:
                        sid = tr.get("StoreID")
                        if sid is None:
                            continue
                        store_demands[sid] = store_demands.get(sid, 0) + abs(
                            int(tr["QtyDelta"])
                        )

        # Generate truck shipments for stores with high demand
        for store_id, demand in store_demands.items():
            if demand > 100:  # Threshold for triggering shipment
                # Find nearest DC (simplified - use first DC)
                dc = self.distribution_centers[0]

                # Check reorder needs
                reorder_list = self.inventory_flow_sim.check_reorder_needs(store_id)

                if reorder_list:
                    # Generate reorder events for analytics
                    reorder_time = date.replace(
                        hour=4, minute=0
                    )  # Reorders triggered at 4 AM
                    for product_id, reorder_qty in reorder_list:
                        # Get current inventory levels
                        current_qty = self.inventory_flow_sim.get_store_balance(
                            store_id, product_id
                        )
                        reorder_point = self.inventory_flow_sim._reorder_points.get(
                            (store_id, product_id), 10
                        )

                        # Calculate priority based on how far below reorder point
                        # URGENT: 50%+ below reorder point (critical stockout risk)
                        # HIGH: 25-50% below reorder point (significant risk)
                        # NORMAL: at or slightly below reorder point
                        deficit_pct = (
                            (reorder_point - current_qty) / reorder_point * 100
                            if reorder_point > 0
                            else 0
                        )
                        if deficit_pct >= 50:
                            priority = "URGENT"
                        elif deficit_pct >= 25:
                            priority = "HIGH"
                        else:
                            priority = "NORMAL"

                        reorder_records.append(
                            {
                                "TraceId": self._generate_trace_id(),
                                "EventTS": reorder_time,
                                "StoreID": store_id,
                                "DCID": dc.ID,
                                "ProductID": product_id,
                                "CurrentQuantity": current_qty,
                                "ReorderQuantity": reorder_qty,
                                "ReorderPoint": reorder_point,
                                "Priority": priority,
                            }
                        )

                    # Generate truck shipments (may be multiple if order exceeds capacity)
                    departure_time = date.replace(hour=6, minute=0)  # 6 AM departure
                    shipments = self.inventory_flow_sim.generate_truck_shipments(
                        dc.ID, store_id, reorder_list, departure_time
                    )

                    # Create truck_move records for each shipment
                    for shipment_info in shipments:
                        event_ts = shipment_info.get("departure_time", departure_time)

                        truck_record = {
                            "TraceId": self._generate_trace_id(),
                            "EventTS": event_ts,
                            "TruckId": shipment_info["truck_id"],
                            "DCID": shipment_info["dc_id"],
                            "StoreID": shipment_info["store_id"],
                            "ShipmentId": shipment_info["shipment_id"],
                            "Status": shipment_info["status"].value,
                            "ETA": shipment_info["eta"],
                            "ETD": shipment_info["etd"],
                            "DepartureTime": event_ts,
                        }

                        # Check if shipment departure is beyond current day OR generation end date
                        # Stage if beyond current day to prevent "stuck in SCHEDULED" warnings
                        is_beyond_today = event_ts > current_day_end
                        is_beyond_generation = _is_beyond_end_date(
                            event_ts, generation_end_date
                        )

                        if is_beyond_today or is_beyond_generation:
                            # Stage for future processing instead of writing to fact table
                            # Do NOT add to _active_shipments - will be restored when generating future dates
                            pending_shipments.append(truck_record)
                            logger.debug(
                                f"Staging shipment {shipment_info['shipment_id']} with "
                                f"departure {event_ts} (beyond current day {date.date()} or end date {generation_end_date})"
                            )
                        else:
                            truck_movements.append(truck_record)
                            # Only track for lifecycle processing if not staged
                            self._active_shipments[shipment_info["shipment_id"]] = (
                                shipment_info
                            )

        # Store pending shipments in staging table (DuckDB only)
        if pending_shipments and getattr(self, "_use_duckdb", False):
            try:
                from retail_datagen.db.duckdb_engine import (
                    get_duckdb_conn,
                    pending_shipments_insert,
                )

                conn = get_duckdb_conn()
                staged_count = pending_shipments_insert(
                    conn, pending_shipments, generation_end_date
                )
                logger.info(
                    f"Staged {staged_count} pending shipments for future streaming"
                )
            except Exception as e:
                logger.error(f"Failed to stage pending shipments: {e}")
                # Don't fail generation - these shipments just won't be in staging

        return truck_movements, reorder_records

    def _process_truck_lifecycle(
        self, date: datetime
    ) -> tuple[list[dict], list[dict], list[dict]]:
        """Process truck lifecycle for all active shipments on this date.

        Generates status progression records and inventory transactions:
        - SCHEDULED → LOADING: Generate DC OUTBOUND transactions
        - LOADING → IN_TRANSIT → ARRIVED
        - ARRIVED → UNLOADING: Generate Store INBOUND transactions
        - UNLOADING → COMPLETED

        Lifecycle events whose actual transition time falls beyond the current day
        are deferred - they will be recorded when the appropriate day is generated.

        Returns:
            Tuple of (truck_move_records, dc_outbound_txn, store_inbound_txn)
        """
        truck_lifecycle_records = []
        dc_outbound_txn = []
        store_inbound_txn = []

        # Define current day boundary for deferring future events
        current_day_end = date.replace(
            hour=23, minute=59, second=59, microsecond=999999
        )

        # Process each active shipment to check for status changes on this date
        shipments_to_process = list(self._active_shipments.values())

        for shipment_info in shipments_to_process:
            shipment_id = shipment_info["shipment_id"]
            previous_status = shipment_info.get("status")

            # Get shipment timing info to calculate actual transition times
            departure_time = shipment_info.get("departure_time")
            eta = shipment_info.get("eta")
            etd = shipment_info.get("etd")

            # Calculate actual transition times (same logic as update_shipment_status)
            # These determine when each lifecycle event actually occurs
            loading_start = (
                departure_time + timedelta(hours=2) if departure_time else None
            )
            transit_start = (
                departure_time + timedelta(hours=4) if departure_time else None
            )
            unloading_start = eta + timedelta(hours=1) if eta else None

            # Build list of check times: use actual transition times instead of hourly
            # to avoid missing short state windows (e.g., transit_start to eta < 1 hour)
            transition_times_list = [
                (TruckStatus.SCHEDULED, departure_time),
                (TruckStatus.LOADING, loading_start),
                (TruckStatus.IN_TRANSIT, transit_start),
                (TruckStatus.ARRIVED, eta),
                (TruckStatus.UNLOADING, unloading_start),
                (TruckStatus.COMPLETED, etd),
            ]

            # Filter to transitions within today and sort by time
            start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
            check_times_to_process = []
            for status, trans_time in transition_times_list:
                if trans_time is None:
                    continue
                # Only process transitions that fall within the current day
                if start_of_day <= trans_time <= current_day_end:
                    check_times_to_process.append(trans_time)

            # Sort and deduplicate
            check_times_to_process = sorted(set(check_times_to_process))

            # If no transitions today but shipment is active, check once at start of day
            # to see if there are any pending state changes from previous days
            if not check_times_to_process:
                # Check at start of day to process any deferred transitions
                check_times_to_process = [start_of_day]

            # Process each transition time
            for check_time in check_times_to_process:
                updated_info = self.inventory_flow_sim.update_shipment_status(
                    shipment_id, check_time
                )

                if updated_info is None:
                    # Shipment was completed and removed from tracking
                    break

                current_status = updated_info["status"]

                # Generate records for status transitions
                if current_status != previous_status:
                    # Create truck_move record for this status change
                    truck_record = {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": check_time,
                        "TruckId": updated_info["truck_id"],
                        "DCID": updated_info["dc_id"],
                        "StoreID": updated_info["store_id"],
                        "ShipmentId": shipment_id,
                        "Status": current_status.value,
                        "ETA": updated_info["eta"],
                        "ETD": updated_info["etd"],
                    }

                    # Add departure fields for COMPLETED status (truck_departed event)
                    if current_status == TruckStatus.COMPLETED:
                        # Departure time is when the truck leaves after unloading
                        truck_record["DepartureTime"] = check_time
                        # Calculate actual unload duration in minutes
                        # (from ARRIVED/UNLOADING start to COMPLETED)
                        eta = updated_info["eta"]
                        if eta:
                            unload_duration_minutes = int(
                                (check_time - eta).total_seconds() / 60
                            )
                            truck_record["ActualUnloadDuration"] = max(
                                self.MIN_UNLOAD_DURATION_MINUTES,
                                unload_duration_minutes,
                            )
                        else:
                            truck_record["ActualUnloadDuration"] = (
                                self.DEFAULT_UNLOAD_DURATION_MINUTES
                            )

                    truck_lifecycle_records.append(truck_record)

                    # Generate inventory transactions at specific lifecycle stages
                    if current_status == TruckStatus.LOADING:
                        # Generate DC OUTBOUND transactions
                        dc_txn = (
                            self.inventory_flow_sim.generate_dc_outbound_transactions(
                                updated_info, check_time
                            )
                        )
                        for txn in dc_txn:
                            dc_outbound_txn.append(
                                {
                                    "TraceId": self._generate_trace_id(),
                                    "EventTS": txn["EventTS"],
                                    "DCID": txn["DCID"],
                                    "ProductID": txn["ProductID"],
                                    "QtyDelta": txn["QtyDelta"],
                                    "Reason": txn["Reason"].value,
                                    "Source": txn["Source"],
                                    # Ensure NOT NULL balance is populated for DC inventory
                                    "Balance": txn.get(
                                        "Balance",
                                        self.inventory_flow_sim.get_dc_balance(
                                            txn["DCID"], txn["ProductID"]
                                        ),
                                    ),
                                }
                            )

                    elif current_status == TruckStatus.UNLOADING:
                        # Generate Store INBOUND transactions
                        store_txn = (
                            self.inventory_flow_sim.generate_store_inbound_transactions(
                                updated_info, check_time
                            )
                        )
                        for txn in store_txn:
                            store_inbound_txn.append(
                                {
                                    "TraceId": self._generate_trace_id(),
                                    "EventTS": txn["EventTS"],
                                    "StoreID": txn["StoreID"],
                                    "ProductID": txn["ProductID"],
                                    "QtyDelta": txn["QtyDelta"],
                                    "Reason": txn["Reason"].value,
                                    "Source": txn["Source"],
                                    "Balance": txn["Balance"],
                                }
                            )
                    elif current_status == TruckStatus.ARRIVED:
                        # Also emit store INBOUND at ARRIVED to guarantee receipt at store
                        store_txn = (
                            self.inventory_flow_sim.generate_store_inbound_transactions(
                                updated_info, check_time
                            )
                        )
                        for txn in store_txn:
                            store_inbound_txn.append(
                                {
                                    "TraceId": self._generate_trace_id(),
                                    "EventTS": txn["EventTS"],
                                    "StoreID": txn["StoreID"],
                                    "ProductID": txn["ProductID"],
                                    "QtyDelta": txn["QtyDelta"],
                                    "Reason": txn["Reason"].value,
                                    "Source": txn["Source"],
                                    "Balance": txn["Balance"],
                                }
                            )

                    previous_status = current_status

        return truck_lifecycle_records, dc_outbound_txn, store_inbound_txn

    def _process_truck_deliveries(
        self, date: datetime, truck_moves: list[dict]
    ) -> list[dict]:
        """Process truck deliveries and generate inventory transactions."""
        delivery_transactions = []

        # Check for shipments completing delivery
        completed_shipments = []
        for shipment_id, shipment_info in self._active_shipments.items():
            if shipment_info["etd"].date() <= date.date():
                # Complete delivery
                transactions = self.inventory_flow_sim.complete_delivery(shipment_id)

                for transaction in transactions:
                    # Get current balance after this transaction
                    balance = self.inventory_flow_sim.get_store_balance(
                        transaction["StoreID"], transaction["ProductID"]
                    )

                    delivery_transactions.append(
                        {
                            "TraceId": self._generate_trace_id(),
                            "EventTS": transaction["EventTS"],
                            "StoreID": transaction["StoreID"],
                            "ProductID": transaction["ProductID"],
                            "QtyDelta": transaction["QtyDelta"],
                            "Reason": transaction["Reason"].value,
                            "Source": transaction["Source"],
                            "Balance": balance,
                        }
                    )

                completed_shipments.append(shipment_id)

        # Remove completed shipments
        for shipment_id in completed_shipments:
            del self._active_shipments[shipment_id]

        return delivery_transactions

    def _restore_pending_shipments_for_date(self, date: datetime) -> list[dict]:
        """Restore pending shipments from staging table that are ready for this date.

        This method retrieves shipments that were deferred from earlier generation
        runs because their departure time was beyond the generation end date.
        When generating new data, these shipments are restored if their departure
        time falls within the current date.

        Args:
            date: The date being generated

        Returns:
            List of truck movement records restored from staging
        """
        if not getattr(self, "_use_duckdb", False):
            return []

        try:
            from retail_datagen.db.duckdb_engine import (
                get_duckdb_conn,
                pending_shipments_delete,
                pending_shipments_get_ready,
            )

            conn = get_duckdb_conn()

            # Get shipments ready for this date (departure_time <= end of day)
            end_of_day = date.replace(hour=23, minute=59, second=59, microsecond=999999)
            ready_shipments = pending_shipments_get_ready(conn, end_of_day)

            if not ready_shipments:
                return []

            # Filter to only shipments whose departure_time is actually on this date
            start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
            todays_shipments = []
            staging_ids_to_delete = []

            for shipment in ready_shipments:
                departure_time = shipment.get("DepartureTime") or shipment.get(
                    "departure_time"
                )
                if departure_time is None:
                    departure_time = shipment.get("EventTS") or shipment.get("event_ts")

                # Parse datetime if string
                if isinstance(departure_time, str):
                    from datetime import datetime as dt

                    try:
                        departure_time = dt.fromisoformat(
                            departure_time.replace("Z", "+00:00")
                        )
                    except (ValueError, AttributeError):
                        continue

                # Check if departure is on this date
                if departure_time and start_of_day <= departure_time <= end_of_day:
                    # Convert back to truck_moves record format
                    truck_record = {
                        "TraceId": shipment.get("TraceId")
                        or shipment.get("trace_id")
                        or self._generate_trace_id(),
                        "EventTS": departure_time,
                        "TruckId": shipment.get("TruckId") or shipment.get("truck_id"),
                        "DCID": shipment.get("DCID") or shipment.get("dc_id"),
                        "StoreID": shipment.get("StoreID") or shipment.get("store_id"),
                        "ShipmentId": shipment.get("ShipmentId")
                        or shipment.get("shipment_id"),
                        "Status": shipment.get("Status")
                        or shipment.get("status", "SCHEDULED"),
                        "ETA": shipment.get("ETA") or shipment.get("eta"),
                        "ETD": shipment.get("ETD") or shipment.get("etd"),
                        "DepartureTime": departure_time,
                    }
                    todays_shipments.append(truck_record)

                    # Track staging ID for deletion
                    staging_id = shipment.get("_staging_id")
                    if staging_id:
                        staging_ids_to_delete.append(staging_id)

            # Delete processed shipments from staging
            if staging_ids_to_delete:
                deleted = pending_shipments_delete(conn, staging_ids_to_delete)
                logger.debug(
                    f"Removed {deleted} shipments from staging after restoration"
                )

            return todays_shipments

        except Exception as e:
            logger.warning(
                f"Failed to restore pending shipments for {date.date()}: {e}"
            )
            return []
