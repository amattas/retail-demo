"""
Field name mapping for database persistence.

Maps generator field names (PascalCase) to database field names (snake_case)
and provides table-to-model resolution and outbox row building.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd

from .base_types import FactGeneratorBase

if TYPE_CHECKING:
    from sqlalchemy.orm import DeclarativeBase

logger = logging.getLogger(__name__)


class FieldMappingMixin(FactGeneratorBase):
    """Field mapping methods for database persistence.

    Handles mapping generator field names to database column names
    and building outbox rows for streaming.
    """

    def _get_model_for_table(self, table_name: str) -> type[DeclarativeBase]:
        """
        Map fact table name to SQLAlchemy model.

        Args:
            table_name: Name of fact table (e.g., "receipts", "dc_inventory_txn")

        Returns:
            SQLAlchemy model class for the table

        Raises:
            ValueError: If table name is unknown
        """
        from retail_datagen.db.models.facts import (
            BLEPing,
            DCInventoryTransaction,
            FootTraffic,
            MarketingImpression,
            OnlineOrderHeader,
            OnlineOrderLine,
            Receipt,
            ReceiptLine,
            StoreInventoryTransaction,
            TruckMove,
        )

        mapping = {
            "dc_inventory_txn": DCInventoryTransaction,
            "truck_moves": TruckMove,
            "store_inventory_txn": StoreInventoryTransaction,
            "receipts": Receipt,
            "receipt_lines": ReceiptLine,
            "foot_traffic": FootTraffic,
            "ble_pings": BLEPing,
            "marketing": MarketingImpression,
            "online_orders": OnlineOrderHeader,
            "online_order_lines": OnlineOrderLine,
        }

        if table_name not in mapping:
            raise ValueError(f"Unknown table: {table_name}")

        return mapping[table_name]

    def _build_outbox_rows_from_df(
        self, table_name: str, df: pd.DataFrame
    ) -> list[dict]:
        """Build streaming_outbox rows for a given fact table batch.

        Each outbox row includes: event_ts, message_type, payload (JSON text),
        partition_key (store/DC if available), and trace_id where appropriate.
        """
        rows: list[dict] = []
        if df is None or df.empty:
            return rows

        # Normalize columns to lower for dictionary extraction safety
        [str(c) for c in df.columns]

        # Helper to get a field ignoring case
        def get_field(rec: dict, name: str):
            if name in rec:
                return rec[name]
            lname = name.lower()
            for k in rec.keys():
                if str(k).lower() == lname:
                    return rec[k]
            return None

        # Event type mapping (default fallbacks handled per-record)
        base_type_map = {
            "receipts": "receipt_created",
            "receipt_lines": "receipt_line_added",
            "dc_inventory_txn": "inventory_updated",
            "store_inventory_txn": "inventory_updated",
            "truck_moves": "truck_arrived",  # may change based on status
            "foot_traffic": "customer_entered",
            "ble_pings": "ble_ping_detected",
            "customer_zone_changes": "customer_zone_changed",
            "marketing": "ad_impression",
            "online_orders": "online_order_created",
            # may change based on timestamps
            "online_order_lines": "online_order_picked",
            "fact_payments": "payment_processed",
            "reorders": "reorder_triggered",
            "store_ops": "store_opened",  # may change based on operation_type
            "stockouts": "stockout_detected",
            "promotions": "promotion_applied",
            # Promo line records don't have separate events
            "promo_lines": "promotion_applied",
        }

        default_type = base_type_map.get(table_name, "receipt_created")

        # Iterate records
        for rec in df.to_dict(orient="records"):
            # Determine message_type and event timestamp
            message_type = default_type
            event_ts = get_field(rec, "event_ts")
            if table_name == "store_ops":
                # Store operations: map operation_type to event_type
                operation_type = (get_field(rec, "operation_type") or "").lower()
                if operation_type == "opened":
                    message_type = "store_opened"
                elif operation_type == "closed":
                    message_type = "store_closed"
            elif table_name == "online_order_lines":
                picked = get_field(rec, "picked_ts")
                shipped = get_field(rec, "shipped_ts")
                delivered = get_field(rec, "delivered_ts")
                if picked:
                    message_type = "online_order_picked"
                    event_ts = picked
                elif shipped:
                    message_type = "online_order_shipped"
                    event_ts = shipped
                elif delivered and not event_ts:
                    # Only use delivered if no other ts present
                    message_type = "online_order_shipped"
                    event_ts = delivered
            elif table_name == "truck_moves":
                status = (get_field(rec, "status") or "").upper()
                if status == "ARRIVED":
                    # Truck arrived at destination (store or DC)
                    message_type = "truck_arrived"
                elif status == "COMPLETED":
                    # Truck departed after completing unloading
                    message_type = "truck_departed"
                # Note: LOADING and IN_TRANSIT are internal states and don't
                # generate separate streaming events - they're tracked in
                # fact table only

            # Partition key preference: store_id -> dc_id
            store_id = get_field(rec, "store_id")
            dc_id = get_field(rec, "dc_id")
            partition_key = str(store_id or dc_id or "")

            # Trace ID preference per type
            trace_id = None
            if table_name == "receipts":
                trace_id = get_field(rec, "receipt_id_ext")
            elif table_name == "receipt_lines":
                rid = get_field(rec, "receipt_id_ext") or get_field(rec, "receipt_id")
                ln = get_field(rec, "line_num")
                trace_id = (
                    f"{rid}-{ln}" if rid is not None and ln is not None else None
                )
            elif table_name == "online_orders":
                trace_id = get_field(rec, "order_id_ext")
            elif table_name == "online_order_lines":
                oid = get_field(rec, "order_id_ext") or get_field(rec, "order_id")
                ln = get_field(rec, "line_num")
                trace_id = (
                    f"{oid}-{ln}" if oid is not None and ln is not None else None
                )
            elif table_name == "marketing":
                trace_id = get_field(rec, "impression_id_ext")
            elif table_name == "promotions":
                rid = get_field(rec, "receipt_id_ext")
                promo = get_field(rec, "promo_code")
                trace_id = (
                    f"{rid}-{promo}" if rid is not None and promo is not None else None
                )
            elif table_name == "promo_lines":
                rid = get_field(rec, "receipt_id_ext")
                ln = get_field(rec, "line_number")
                promo = get_field(rec, "promo_code")
                trace_id = (
                    f"{rid}-{ln}-{promo}"
                    if rid is not None and ln is not None and promo is not None
                    else None
                )

            try:
                payload_json = json.dumps(rec, default=str)
            except (TypeError, ValueError) as e:
                # Fallback: stringify non-serializable values crudely
                logger.debug(f"Failed to JSON serialize record, using fallback: {e}")
                payload_json = json.dumps({k: str(v) for k, v in rec.items()})

            rows.append(
                {
                    "event_ts": event_ts,
                    "message_type": message_type,
                    "payload": payload_json,
                    "partition_key": partition_key,
                    "trace_id": trace_id or "",
                }
            )

        return rows

    def _map_field_names_for_db(self, table_name: str, record: dict) -> dict:
        """
        Map generator field names (PascalCase) to database field names (snake_case).

        Args:
            table_name: Name of fact table
            record: Record dict with generator field names

        Returns:
            New dict with database-compatible field names

        Notes:
            - TraceId field is skipped (not in DB models)
            - balance field (DC/Store inventory) not in generator output - will be NULL
            - Some generator fields don't map to DB (e.g., ReceiptId string,
              OrderId string)
            - Receipts table: discount_amount field not in generator -
              will default to 0.0
        """
        # Define field mappings for each table
        # Generator fields -> Database fields
        common_mappings = {
            # Skip TraceId - not in DB models
            "EventTS": "event_ts",
        }

        table_specific_mappings = {
            "receipts": {
                **common_mappings,
                "StoreID": "store_id",
                "CustomerID": "customer_id",
                # Store external receipt id for linkage with receipt_lines
                "ReceiptId": "receipt_id_ext",
                "ReceiptType": "receipt_type",
                "ReturnForReceiptId": "return_for_receipt_id",
                "ReturnForReceiptIdExt": "return_for_receipt_id_ext",
                # Note: Subtotal field in generator is not stored (can be calculated)
                "DiscountAmount": "discount_amount",
                "Tax": "tax_amount",
                "Total": "total_amount",
                "TenderType": "payment_method",
            },
            "receipt_lines": {
                **common_mappings,
                # ReceiptId will be resolved to numeric FK by lookup before insert
                "ProductID": "product_id",
                "Line": "line_num",
                "Qty": "quantity",
                "UnitPrice": "unit_price",
                "ExtPrice": "ext_price",
                "PromoCode": "promo_code",
            },
            "dc_inventory_txn": {
                **common_mappings,
                "DCID": "dc_id",
                "ProductID": "product_id",
                "QtyDelta": "quantity",
                "Reason": "txn_type",
                "Balance": "balance",
            },
            "truck_moves": {
                **common_mappings,
                "TruckId": "truck_id",
                "DCID": "dc_id",
                "StoreID": "store_id",
                "ProductID": "product_id",
                "Status": "status",
                "ShipmentId": "shipment_id",
                "ETA": "eta",
                "ETD": "etd",
                "DepartureTime": "departure_time",
                "ActualUnloadDuration": "actual_unload_duration",
            },
            "store_inventory_txn": {
                **common_mappings,
                "StoreID": "store_id",
                "ProductID": "product_id",
                "QtyDelta": "quantity",
                "Reason": "txn_type",
                "Source": "source",
                "Balance": "balance",
            },
            "foot_traffic": {
                **common_mappings,
                "StoreID": "store_id",
                "SensorId": "sensor_id",
                "Zone": "zone",
                "Dwell": "dwell_seconds",
                "Count": "count",
            },
            "ble_pings": {
                **common_mappings,
                "StoreID": "store_id",
                "CustomerBLEId": "customer_ble_id",
                "BeaconId": "beacon_id",
                "RSSI": "rssi",
                "Zone": "zone",
                # Note: customer_id field in DB is nullable
                # (requires lookup from BLE ID)
            },
            "marketing": {
                **common_mappings,
                "CampaignId": "campaign_id",
                "CreativeId": "creative_id",
                "ImpressionId": "impression_id_ext",
                "CustomerAdId": "customer_ad_id",
                "Channel": "channel",
                "Device": "device",
                "Cost": "cost",
                # Note: customer_id field in DB is nullable (requires lookup from ad ID)
            },
            "online_orders": {
                **common_mappings,
                # Header shape
                "CustomerID": "customer_id",
                "Subtotal": "subtotal_amount",
                "Tax": "tax_amount",
                "Total": "total_amount",
                "TenderType": "payment_method",
                "CompletedTS": "completed_ts",
                # Optional external order id
                "OrderId": "order_id_ext",
            },
            "online_order_lines": {
                **common_mappings,
                "OrderId": "order_id",
                "ProductID": "product_id",
                "Line": "line_num",
                "Qty": "quantity",
                "UnitPrice": "unit_price",
                "ExtPrice": "ext_price",
                "PromoCode": "promo_code",
                # Per-line lifecycle
                "PickedTS": "picked_ts",
                "ShippedTS": "shipped_ts",
                "DeliveredTS": "delivered_ts",
                "FulfillmentStatus": "fulfillment_status",
                "FulfillmentMode": "fulfillment_mode",
                "NodeType": "node_type",
                "NodeID": "node_id",
            },
            "fact_payments": {
                **common_mappings,
                "ReceiptIdExt": "receipt_id_ext",
                "OrderIdExt": "order_id_ext",
                "PaymentMethod": "payment_method",
                "AmountCents": "amount_cents",
                "Amount": "amount",
                "TransactionId": "transaction_id",
                "ProcessingTimeMs": "processing_time_ms",
                "Status": "status",
                "DeclineReason": "decline_reason",
                "StoreID": "store_id",
                "CustomerID": "customer_id",
            },
            "promotions": {
                **common_mappings,
                "ReceiptId": "receipt_id_ext",
                "PromoCode": "promo_code",
                "DiscountAmount": "discount_amount",
                "DiscountCents": "discount_cents",
                "DiscountType": "discount_type",
                "ProductCount": "product_count",
                "ProductIds": "product_ids",
                "StoreID": "store_id",
                "CustomerID": "customer_id",
            },
            "promo_lines": {
                **common_mappings,
                "ReceiptId": "receipt_id_ext",
                "PromoCode": "promo_code",
                "LineNumber": "line_number",
                "ProductID": "product_id",
                "Qty": "quantity",
                "DiscountAmount": "discount_amount",
                "DiscountCents": "discount_cents",
            },
            "store_ops": {
                # store_ops uses snake_case keys directly
                # Explicit identity mapping for clarity and robustness
                "trace_id": "trace_id",
                "operation_time": "operation_time",
                "store_id": "store_id",
                "operation_type": "operation_type",
            },
            "customer_zone_changes": {
                **common_mappings,
                "StoreID": "store_id",
                "CustomerBLEId": "customer_ble_id",
                "FromZone": "from_zone",
                "ToZone": "to_zone",
            },
            "stockouts": {
                **common_mappings,
                "StoreID": "store_id",
                "DCID": "dc_id",
                "ProductID": "product_id",
                "LastKnownQuantity": "last_known_quantity",
                "DetectionTime": "detection_time",
            },
            "reorders": {
                **common_mappings,
                "StoreID": "store_id",
                "DCID": "dc_id",
                "ProductID": "product_id",
                "CurrentQuantity": "current_quantity",
                "ReorderQuantity": "reorder_quantity",
                "ReorderPoint": "reorder_point",
                "Priority": "priority",
            },
        }

        mapping = table_specific_mappings.get(table_name, common_mappings)
        mapped_record = {}

        for gen_field, value in record.items():
            # Skip TraceId field entirely (not in any DB model)
            if gen_field == "TraceId":
                continue

            db_field = mapping.get(gen_field, gen_field.lower())
            mapped_record[db_field] = value

        # DuckDB fast-path: keep external linking keys on line tables
        # to avoid FK lookups
        if getattr(self, "_use_duckdb", False):
            if table_name == "receipt_lines":
                ext = record.get("ReceiptId")
                if ext is not None and "receipt_id" not in mapped_record:
                    mapped_record["receipt_id_ext"] = ext
            elif table_name == "online_order_lines":
                ext = record.get("OrderId")
                if ext is not None and "order_id" not in mapped_record:
                    mapped_record["order_id_ext"] = ext

        return mapped_record
