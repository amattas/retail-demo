"""
Truck logistics including movements, lifecycle, and deliveries
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
import pandas as pd
from retail_datagen.shared.models import DistributionCenter, ProductMaster, Store, Truck

logger = logging.getLogger(__name__)


class LogisticsMixin:
    """Truck logistics including movements, lifecycle, and deliveries"""

    def _generate_truck_movements(
        self, date: datetime, store_transactions: list[dict]
    ) -> list[dict]:
        """Generate truck movements based on store inventory needs.

        This method creates initial shipments in SCHEDULED status.
        The _process_truck_lifecycle method will handle status progression.
        """
        truck_movements = []

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
                        int(row.StoreID): int(row.QtyDelta) for row in demands.itertuples(index=False)
                    }
                else:
                    # Fallback to loop if schema unexpected
                    for tr in store_transactions:
                        if tr.get("QtyDelta", 0) < 0:
                            sid = tr.get("StoreID")
                            if sid is None:
                                continue
                            store_demands[sid] = store_demands.get(sid, 0) + abs(int(tr["QtyDelta"]))
            except Exception as e:
                logger.debug(f"Failed to aggregate store demands via pandas, using fallback: {e}")
                for tr in store_transactions:
                    if tr.get("QtyDelta", 0) < 0:
                        sid = tr.get("StoreID")
                        if sid is None:
                            continue
                        store_demands[sid] = store_demands.get(sid, 0) + abs(int(tr["QtyDelta"]))

        # Generate truck shipments for stores with high demand
        for store_id, demand in store_demands.items():
            if demand > 100:  # Threshold for triggering shipment

                # Find nearest DC (simplified - use first DC)
                dc = self.distribution_centers[0]

                # Check reorder needs
                reorder_list = self.inventory_flow_sim.check_reorder_needs(store_id)

                if reorder_list:
                    # Generate truck shipment (initial status: SCHEDULED)
                    departure_time = date.replace(hour=6, minute=0)  # 6 AM departure
                    shipment_info = self.inventory_flow_sim.generate_truck_shipment(
                        dc.ID, store_id, reorder_list, departure_time
                    )

                    # Create initial truck_move record in SCHEDULED status
                    truck_record = {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": departure_time,
                        "TruckId": shipment_info["truck_id"],
                        "DCID": shipment_info["dc_id"],
                        "StoreID": shipment_info["store_id"],
                        "ShipmentId": shipment_info["shipment_id"],
                        "Status": shipment_info["status"].value,
                        "ETA": shipment_info["eta"],
                        "ETD": shipment_info["etd"],
                    }
                    truck_movements.append(truck_record)

                    # Track shipment for lifecycle processing
                    self._active_shipments[shipment_info["shipment_id"]] = shipment_info

        return truck_movements


    def _process_truck_lifecycle(
        self, date: datetime
    ) -> tuple[list[dict], list[dict], list[dict]]:
        """Process truck lifecycle for all active shipments on this date.

        Generates status progression records and inventory transactions:
        - SCHEDULED → LOADING: Generate DC OUTBOUND transactions
        - LOADING → IN_TRANSIT → ARRIVED
        - ARRIVED → UNLOADING: Generate Store INBOUND transactions
        - UNLOADING → COMPLETED

        Returns:
            Tuple of (truck_move_records, dc_outbound_txn, store_inbound_txn)
        """
        truck_lifecycle_records = []
        dc_outbound_txn = []
        store_inbound_txn = []

        # Process each active shipment to check for status changes on this date
        shipments_to_process = list(self._active_shipments.values())

        for shipment_info in shipments_to_process:
            shipment_id = shipment_info["shipment_id"]
            previous_status = shipment_info.get("status")

            # Update shipment status based on current date/time
            # We'll check at multiple times throughout the day to capture transitions
            for hour in range(24):
                check_time = date.replace(hour=hour, minute=0)
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
                            unload_duration_minutes = int((check_time - eta).total_seconds() / 60)
                            truck_record["ActualUnloadDuration"] = max(
                                self.MIN_UNLOAD_DURATION_MINUTES, unload_duration_minutes
                            )
                        else:
                            truck_record["ActualUnloadDuration"] = self.DEFAULT_UNLOAD_DURATION_MINUTES

                    truck_lifecycle_records.append(truck_record)

                    # Generate inventory transactions at specific lifecycle stages
                    from retail_datagen.shared.models import TruckStatus

                    if current_status == TruckStatus.LOADING:
                        # Generate DC OUTBOUND transactions
                        dc_txn = self.inventory_flow_sim.generate_dc_outbound_transactions(
                            updated_info, check_time
                        )
                        for txn in dc_txn:
                            dc_outbound_txn.append({
                                "TraceId": self._generate_trace_id(),
                                "EventTS": txn["EventTS"],
                                "DCID": txn["DCID"],
                                "ProductID": txn["ProductID"],
                                "QtyDelta": txn["QtyDelta"],
                                "Reason": txn["Reason"].value,
                                "Source": txn["Source"],
                                # Ensure NOT NULL balance is populated for DC inventory
                                "Balance": txn.get("Balance", self.inventory_flow_sim.get_dc_balance(txn["DCID"], txn["ProductID"]))
                            })

                    elif current_status == TruckStatus.UNLOADING:
                        # Generate Store INBOUND transactions
                        store_txn = self.inventory_flow_sim.generate_store_inbound_transactions(
                            updated_info, check_time
                        )
                        for txn in store_txn:
                            store_inbound_txn.append({
                                "TraceId": self._generate_trace_id(),
                                "EventTS": txn["EventTS"],
                                "StoreID": txn["StoreID"],
                                "ProductID": txn["ProductID"],
                                "QtyDelta": txn["QtyDelta"],
                                "Reason": txn["Reason"].value,
                                "Source": txn["Source"],
                                "Balance": txn["Balance"],
                            })
                    elif current_status == TruckStatus.ARRIVED:
                        # Also emit store INBOUND at ARRIVED to guarantee receipt at store
                        store_txn = self.inventory_flow_sim.generate_store_inbound_transactions(
                            updated_info, check_time
                        )
                        for txn in store_txn:
                            store_inbound_txn.append({
                                "TraceId": self._generate_trace_id(),
                                "EventTS": txn["EventTS"],
                                "StoreID": txn["StoreID"],
                                "ProductID": txn["ProductID"],
                                "QtyDelta": txn["QtyDelta"],
                                "Reason": txn["Reason"].value,
                                "Source": txn["Source"],
                                "Balance": txn["Balance"],
                            })

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


