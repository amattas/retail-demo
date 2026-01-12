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

    def _transform_db_fields_to_kql_payload(
        self, table_name: str, rec: dict
    ) -> dict:
        """Transform database field names to KQL-expected payload field names.

        The database uses snake_case with _ext suffixes for external IDs,
        but KQL expects specific field names without suffixes.
        """
        # Helper to get field value case-insensitively
        def get_field(field_name: str, default=None):
            # Try exact match first
            if field_name in rec:
                return rec[field_name]
            # Try case-insensitive match
            field_lower = field_name.lower()
            for key in rec.keys():
                if str(key).lower() == field_lower:
                    return rec[key]
            return default

        # Create a clean payload with only KQL-expected fields
        payload = {}

        if table_name == "marketing":
            # ad_impression event
            payload["channel"] = get_field("channel")
            payload["campaign_id"] = get_field("campaign_id")
            payload["creative_id"] = get_field("creative_id")
            payload["customer_ad_id"] = get_field("customer_ad_id")
            # impression_id_ext -> impression_id
            payload["impression_id"] = get_field("impression_id_ext")
            payload["cost"] = get_field("cost")
            # device -> device_type
            payload["device_type"] = get_field("device")

        elif table_name in ("dc_inventory_txn", "store_inventory_txn"):
            # inventory_updated event
            payload["store_id"] = get_field("store_id")
            payload["dc_id"] = get_field("dc_id")
            payload["product_id"] = get_field("product_id")
            # quantity -> quantity_delta
            payload["quantity_delta"] = get_field("quantity")
            # txn_type -> reason
            payload["reason"] = get_field("txn_type")
            # source -> payload_source (renamed to avoid conflict with envelope source)
            payload["payload_source"] = get_field("source", "SYSTEM")

        elif table_name == "truck_moves":
            # truck_arrived or truck_departed event
            status = (get_field("status") or "").upper()
            if status == "ARRIVED":
                # truck_arrived
                payload["truck_id"] = str(get_field("truck_id", ""))
                payload["dc_id"] = get_field("dc_id")
                payload["store_id"] = get_field("store_id")
                payload["shipment_id"] = get_field("shipment_id")
                # eta -> arrival_time
                payload["arrival_time"] = get_field("eta")
                # estimated_unload_duration: calculate from ETA to ETD
                eta = get_field("eta")
                etd = get_field("etd")
                if eta and etd:
                    try:
                        from datetime import datetime
                        if isinstance(eta, str):
                            eta = datetime.fromisoformat(eta)
                        if isinstance(etd, str):
                            etd = datetime.fromisoformat(etd)
                        duration_seconds = int((etd - eta).total_seconds())
                        payload["estimated_unload_duration"] = duration_seconds
                    except Exception:
                        payload["estimated_unload_duration"] = 3600  # default 1 hour
                else:
                    payload["estimated_unload_duration"] = 3600
            elif status == "COMPLETED":
                # truck_departed
                payload["truck_id"] = str(get_field("truck_id", ""))
                payload["dc_id"] = get_field("dc_id")
                payload["store_id"] = get_field("store_id")
                payload["shipment_id"] = get_field("shipment_id")
                # departure_time
                payload["departure_time"] = get_field("departure_time")
                # actual_unload_duration
                payload["actual_unload_duration"] = get_field("actual_unload_duration", 3600)

        elif table_name == "online_orders":
            # online_order_created event
            # order_id_ext -> order_id
            payload["order_id"] = get_field("order_id_ext")
            payload["customer_id"] = get_field("customer_id")
            # Need to get these from online_order_lines or infer defaults
            payload["fulfillment_mode"] = get_field("fulfillment_mode", "SHIP_FROM_DC")
            payload["node_type"] = get_field("node_type", "DC")
            payload["node_id"] = get_field("node_id", 1)
            payload["item_count"] = get_field("item_count", 1)
            # subtotal_amount -> subtotal
            payload["subtotal"] = get_field("subtotal_amount")
            # tax_amount -> tax
            payload["tax"] = get_field("tax_amount")
            # total_amount -> total
            payload["total"] = get_field("total_amount")
            # payment_method -> tender_type
            payload["tender_type"] = get_field("payment_method")

        elif table_name == "fact_payments":
            # payment_processed event
            # receipt_id_ext -> receipt_id
            payload["receipt_id"] = get_field("receipt_id_ext")
            # order_id_ext -> order_id
            payload["order_id"] = get_field("order_id_ext")
            payload["payment_method"] = get_field("payment_method")
            payload["amount"] = get_field("amount")
            payload["amount_cents"] = get_field("amount_cents")
            payload["transaction_id"] = get_field("transaction_id")
            # Add processing_time from event_ts if not present
            payload["processing_time"] = get_field("event_ts")
            payload["processing_time_ms"] = get_field("processing_time_ms")
            payload["status"] = get_field("status")
            payload["decline_reason"] = get_field("decline_reason")
            payload["store_id"] = get_field("store_id")
            payload["customer_id"] = get_field("customer_id")

        elif table_name == "receipts":
            # receipt_created event
            payload["store_id"] = get_field("store_id")
            payload["customer_id"] = get_field("customer_id")
            # receipt_id_ext -> receipt_id
            payload["receipt_id"] = get_field("receipt_id_ext")
            # Map to expected field names - try Subtotal (capitalized) or subtotal_amount
            subtotal = get_field("subtotal_amount") or get_field("subtotal") or get_field("Subtotal")
            payload["subtotal"] = subtotal if subtotal is not None else get_field("total_amount", 0)
            payload["tax"] = get_field("tax_amount", 0)
            payload["total"] = get_field("total_amount")
            # payment_method -> tender_type
            payload["tender_type"] = get_field("payment_method")
            payload["item_count"] = get_field("item_count", 1)
            payload["campaign_id"] = get_field("campaign_id")

        elif table_name == "receipt_lines":
            # receipt_line_added event
            payload["receipt_id"] = get_field("receipt_id_ext")
            payload["line_number"] = get_field("line_num")
            payload["product_id"] = get_field("product_id")
            payload["quantity"] = get_field("quantity")
            payload["unit_price"] = get_field("unit_price")
            payload["extended_price"] = get_field("ext_price")
            payload["promo_code"] = get_field("promo_code")

        elif table_name == "foot_traffic":
            # customer_entered event
            payload["store_id"] = get_field("store_id")
            payload["sensor_id"] = get_field("sensor_id")
            payload["zone"] = get_field("zone")
            payload["customer_count"] = get_field("count")
            # dwell_seconds -> dwell_time
            payload["dwell_time"] = get_field("dwell_seconds")

        elif table_name == "ble_pings":
            # ble_ping_detected event
            payload["store_id"] = get_field("store_id")
            payload["beacon_id"] = get_field("beacon_id")
            payload["customer_ble_id"] = get_field("customer_ble_id")
            payload["rssi"] = get_field("rssi")
            payload["zone"] = get_field("zone")

        elif table_name == "customer_zone_changes":
            # customer_zone_changed event
            payload["store_id"] = get_field("store_id")
            payload["customer_ble_id"] = get_field("customer_ble_id")
            payload["from_zone"] = get_field("from_zone")
            payload["to_zone"] = get_field("to_zone")
            payload["timestamp"] = get_field("event_ts")

        elif table_name == "store_ops":
            # store_opened or store_closed event
            payload["store_id"] = get_field("store_id")
            payload["operation_time"] = get_field("operation_time")
            payload["operation_type"] = get_field("operation_type")

        elif table_name == "reorders":
            # reorder_triggered event
            payload["store_id"] = get_field("store_id")
            payload["dc_id"] = get_field("dc_id")
            payload["product_id"] = get_field("product_id")
            payload["current_quantity"] = get_field("current_quantity")
            payload["reorder_quantity"] = get_field("reorder_quantity")
            payload["reorder_point"] = get_field("reorder_point")
            payload["priority"] = get_field("priority")

        elif table_name == "stockouts":
            # stockout_detected event
            payload["store_id"] = get_field("store_id")
            payload["dc_id"] = get_field("dc_id")
            payload["product_id"] = get_field("product_id")
            payload["last_known_quantity"] = get_field("last_known_quantity")
            payload["detection_time"] = get_field("detection_time")

        elif table_name == "promotions":
            # promotion_applied event
            payload["receipt_id"] = get_field("receipt_id_ext")
            payload["promo_code"] = get_field("promo_code")
            payload["discount_amount"] = get_field("discount_amount")
            payload["discount_cents"] = get_field("discount_cents")
            payload["discount_type"] = get_field("discount_type")
            payload["product_count"] = get_field("product_count")
            payload["product_ids"] = get_field("product_ids")
            payload["store_id"] = get_field("store_id")
            payload["customer_id"] = get_field("customer_id")

        elif table_name == "online_order_lines":
            # online_order_picked or online_order_shipped event
            payload["order_id"] = get_field("order_id_ext")
            payload["node_type"] = get_field("node_type")
            payload["node_id"] = get_field("node_id")
            payload["fulfillment_mode"] = get_field("fulfillment_mode")
            picked = get_field("picked_ts")
            shipped = get_field("shipped_ts")
            if picked:
                payload["picked_time"] = picked
            elif shipped:
                payload["shipped_time"] = shipped
        else:
            # Unknown table - return record as-is
            payload = rec.copy()

        # Remove None values to keep payload clean
        return {k: v for k, v in payload.items() if v is not None}

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
                trace_id = f"{rid}-{ln}" if rid is not None and ln is not None else None
            elif table_name == "online_orders":
                trace_id = get_field(rec, "order_id_ext")
            elif table_name == "online_order_lines":
                oid = get_field(rec, "order_id_ext") or get_field(rec, "order_id")
                ln = get_field(rec, "line_num")
                trace_id = f"{oid}-{ln}" if oid is not None and ln is not None else None
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

            # Transform database fields to KQL-expected payload fields
            try:
                payload_dict = self._transform_db_fields_to_kql_payload(table_name, rec)
                payload_json = json.dumps(payload_dict, default=str)
            except (TypeError, ValueError) as e:
                # Fallback: stringify non-serializable values crudely
                logger.debug(f"Failed to JSON serialize payload, using fallback: {e}")
                payload_dict = self._transform_db_fields_to_kql_payload(table_name, rec)
                payload_json = json.dumps({k: str(v) for k, v in payload_dict.items()})

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
