"""
Stockout detection and tracking for supply chain analytics.

This mixin tracks stockout events when inventory balances reach zero,
providing critical data for supply chain optimization and demand planning.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from .base_types import FactGeneratorBase

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class StockoutsMixin(FactGeneratorBase):
    """Stockout detection and tracking mixin for fact data generation."""

    def _detect_and_record_stockout(
        self,
        node_type: str,
        node_id: int,
        product_id: int,
        last_known_quantity: int,
        detection_time: datetime,
        current_balance: int,
    ) -> dict | None:
        """
        Detect and record a stockout event when inventory reaches zero.

        Args:
            node_type: "STORE" or "DC"
            node_id: Store ID or DC ID
            product_id: Product ID that stocked out
            last_known_quantity: Last positive quantity before stockout
            detection_time: Timestamp when stockout was detected
            current_balance: Current inventory balance (should be 0)

        Returns:
            Stockout record dict if a new stockout is detected, None otherwise
        """
        # Only record stockouts when balance actually reaches zero
        if current_balance != 0:
            return None

        # Construct key for tracking duplicate detections
        key = (node_id, product_id)

        # Check if we already recorded a stockout for this node-product combination
        # Only record new stockouts if enough time has passed (1 day min)
        if key in self._last_stockout_detection:
            last_detection = self._last_stockout_detection[key]
            hours_since_last = (detection_time - last_detection).total_seconds() / 3600
            # Require at least 24 hours between stockout detections for same product
            # This prevents recording multiple stockouts during the same depletion
            if hours_since_last < 24:
                return None

        # Record this stockout detection
        self._last_stockout_detection[key] = detection_time

        # Build stockout record matching the schema
        stockout_record = {
            "TraceId": self._generate_trace_id(),
            "EventTS": detection_time,
            "StoreID": node_id if node_type == "STORE" else None,
            "DCID": node_id if node_type == "DC" else None,
            "ProductID": product_id,
            "LastKnownQuantity": max(0, last_known_quantity),
            "DetectionTime": detection_time,
        }

        logger.debug(
            f"Stockout detected: {node_type} {node_id}, Product {product_id} "
            f"(last qty: {last_known_quantity})"
        )

        return stockout_record

    def _generate_stockouts_from_inventory_txns(
        self,
        store_inventory_txns: list[dict],
        dc_inventory_txns: list[dict],
    ) -> list[dict]:
        """
        Generate stockout events from inventory transaction records.

        Analyzes inventory transactions to detect when balances hit zero,
        indicating a stockout condition.

        Args:
            store_inventory_txns: List of store inventory transaction records
            dc_inventory_txns: List of DC inventory transaction records

        Returns:
            List of stockout event records
        """
        stockout_records = []

        # Process store inventory transactions
        for txn in store_inventory_txns:
            balance = txn.get("Balance", 0)
            if balance == 0:
                # Inventory hit zero - potential stockout
                store_id = txn.get("StoreID")
                product_id = txn.get("ProductID")
                event_ts = txn.get("EventTS")
                qty_delta = txn.get("QtyDelta", 0)

                # Last known quantity is the absolute value of the delta that
                # brought us to zero (if delta is negative, it represents the
                # sale/usage that depleted inventory)
                last_known_qty = abs(qty_delta) if qty_delta < 0 else 0

                stockout = self._detect_and_record_stockout(
                    node_type="STORE",
                    node_id=store_id,
                    product_id=product_id,
                    last_known_quantity=last_known_qty,
                    detection_time=event_ts,
                    current_balance=balance,
                )

                if stockout:
                    stockout_records.append(stockout)

        # Process DC inventory transactions
        for txn in dc_inventory_txns:
            balance = txn.get("Balance", 0)
            if balance == 0:
                # DC inventory hit zero - potential stockout
                dc_id = txn.get("DCID")
                product_id = txn.get("ProductID")
                event_ts = txn.get("EventTS")
                qty_delta = txn.get("QtyDelta", 0)

                # Last known quantity
                last_known_qty = abs(qty_delta) if qty_delta < 0 else 0

                stockout = self._detect_and_record_stockout(
                    node_type="DC",
                    node_id=dc_id,
                    product_id=product_id,
                    last_known_quantity=last_known_qty,
                    detection_time=event_ts,
                    current_balance=balance,
                )

                if stockout:
                    stockout_records.append(stockout)

        if stockout_records:
            logger.info(
                f"Generated {len(stockout_records)} stockout events "
                "from inventory transactions"
            )

        return stockout_records
