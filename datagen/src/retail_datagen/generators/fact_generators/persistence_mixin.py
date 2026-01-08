"""
Database persistence and data mapping methods.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import DeclarativeBase

logger = logging.getLogger(__name__)


class PersistenceMixin:
    """Database persistence and data mapping methods."""

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
            "online_order_lines": "online_order_picked",  # may change based on timestamps
            "fact_payments": "payment_processed",
            "reorders": "reorder_triggered",
            "store_ops": "store_opened",  # may change based on operation_type
            "stockouts": "stockout_detected",
            "promotions": "promotion_applied",
            "promo_lines": "promotion_applied",  # Promo line records don't have separate events
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
                # generate separate streaming events - they're tracked in fact table only

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
            - Some generator fields don't map to DB (e.g., ReceiptId string, OrderId string)
            - Receipts table: discount_amount field not in generator - will default to 0.0
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
                # Note: customer_id field in DB is nullable (requires lookup from BLE ID)
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
        }

        mapping = table_specific_mappings.get(table_name, common_mappings)
        mapped_record = {}

        for gen_field, value in record.items():
            # Skip TraceId field entirely (not in any DB model)
            if gen_field == "TraceId":
                continue

            db_field = mapping.get(gen_field, gen_field.lower())
            mapped_record[db_field] = value

        # DuckDB fast-path: keep external linking keys on line tables to avoid FK lookups
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

    async def _capture_and_drop_indexes(
        self, session: AsyncSession, generator_table_names: list[str]
    ) -> list[tuple[str, str]]:
        """
        Capture and drop nonessential indexes for specified tables to speed bulk inserts.

        Returns a list of (index_name, create_sql) to recreate later.
        Keeps indexes that are critical to linkage lookups (receipt_id_ext, order_id_ext).
        """
        from sqlalchemy import text

        captured: list[tuple[str, str]] = []
        try:
            for gen_name in generator_table_names:
                try:
                    model = self._get_model_for_table(gen_name)
                except (KeyError, AttributeError) as e:
                    logger.debug(f"Failed to get model for table {gen_name}: {e}")
                    continue
                tbl = getattr(model, "__tablename__", None)
                if not tbl:
                    continue
                rows = (
                    await session.execute(
                        text(
                            "SELECT name, sql FROM sqlite_master "
                            "WHERE type='index' AND tbl_name=:tbl AND sql IS NOT NULL"
                        ),
                        {"tbl": tbl},
                    )
                ).all()

                for name, sql in rows:
                    if not name or not sql:
                        continue
                    lname = str(name).lower()
                    lsql = str(sql).lower()
                    # Keep ext-id linkage indexes to avoid slow lookups during generation
                    if (
                        ("receipt" in lname and "ext" in lname)
                        or ("order" in lname and "ext" in lname)
                        or ("receipt_id_ext" in lsql)
                        or ("order_id_ext" in lsql)
                    ):
                        continue
                    captured.append((name, sql))
                    try:
                        await session.execute(text(f'DROP INDEX IF EXISTS "{name}"'))
                    except Exception as e:
                        logger.debug(f"Failed to drop index {name} on {tbl}: {e}")
            await session.commit()
        except Exception as e:
            logger.debug(f"Index capture/drop skipped due to error: {e}")
        return captured

    async def _recreate_indexes(
        self, session: AsyncSession, index_defs: list[tuple[str, str]]
    ) -> None:
        """Recreate previously captured indexes after bulk inserts complete."""
        from sqlalchemy import text

        if not index_defs:
            return
        try:
            for name, sql in index_defs:
                try:
                    await session.execute(text(sql))
                except Exception as e:
                    logger.debug(f"Failed to recreate index {name}: {e}")
            await session.commit()
        except Exception as e:
            logger.debug(f"Index recreation encountered an error: {e}")

    async def _insert_hourly_to_db(
        self,
        session: AsyncSession,
        table_name: str,
        data: list[dict] | pd.DataFrame,
        hour: int,
        batch_size: int = 10000,
        commit_every_batches: int = 0,
    ) -> None:
        """
        Insert hourly data batch into the database (DuckDB fast-path).

        Args:
            session: Database session for facts.db
            table_name: Name of fact table (e.g., "receipts")
            data: List of dicts or DataFrame with hourly data
            hour: Hour index (0-23)
            batch_size: Rows per batch insert (default: 10000)

        Note:
            Commits once after all batches are inserted for performance.
            Individual batches are not committed to minimize I/O overhead.
            Field names are automatically mapped from PascalCase to snake_case.

            Falls back to list processing if pandas is unavailable (logged at
            warning level for environment diagnostics).
        """
        # Normalize input into list of dict records
        records: list[dict]
        try:
            import pandas as _pd

            if isinstance(data, _pd.DataFrame):
                if data.empty:
                    logger.debug(f"No data to insert for {table_name} hour {hour}")
                    return
                records = data.to_dict("records")
            else:
                records = list(data or [])
        except (ImportError, AttributeError) as e:
            # If pandas not available, assume list path
            logger.warning(
                f"Failed to process data via pandas for {table_name}, using fallback: {e}"
            )
            records = list(data or [])  # type: ignore[arg-type]
        if not records:
            logger.debug(f"No data to insert for {table_name} hour {hour}")
            return

        # DuckDB fast path
        if (
            getattr(self, "_use_duckdb", False)
            and getattr(self, "_duckdb_conn", None) is not None
        ):
            try:
                import pandas as _pd

                # Struct-of-Arrays build for high-volume tables
                hot_tables = {
                    "receipt_lines",
                    "store_inventory_txn",
                    "online_order_lines",
                    "receipts",
                    "dc_inventory_txn",
                    "truck_moves",
                    "foot_traffic",
                    "ble_pings",
                    "marketing",
                    "online_orders",
                }
                if (
                    table_name in hot_tables
                    and isinstance(records, list)
                    and records
                    and isinstance(records[0], dict)
                ):
                    # Build dict-of-lists for existing keys across a small sample to avoid missing columns
                    sample_keys = set()
                    for r in records[:10]:
                        sample_keys.update(r.keys())
                    cols: dict[str, list] = {k: [] for k in sample_keys}
                    for r in records:
                        for k in cols.keys():
                            cols[k].append(r.get(k))
                    df = _pd.DataFrame(cols)
                else:
                    # Fallback AoS -> DataFrame
                    df = _pd.DataFrame.from_records(records)
                if df.empty:
                    return
                if "TraceId" in df.columns:
                    df = df.drop(columns=["TraceId"])

                # Table-specific rename mapping (generator -> DB)
                common = {"EventTS": "event_ts"}
                mapping_tbl = {
                    "receipts": {
                        **common,
                        "StoreID": "store_id",
                        "CustomerID": "customer_id",
                        "ReceiptId": "receipt_id_ext",
                        "ReceiptType": "receipt_type",
                        "SubtotalCents": "subtotal_cents",
                        "TaxCents": "tax_cents",
                        "TotalCents": "total_cents",
                        "ReturnForReceiptId": "return_for_receipt_id",
                        "ReturnForReceiptIdExt": "return_for_receipt_id_ext",
                        "DiscountAmount": "discount_amount",
                        "Tax": "tax_amount",
                        "Total": "total_amount",
                        "TenderType": "payment_method",
                    },
                    "receipt_lines": {
                        **common,
                        "ProductID": "product_id",
                        "Line": "line_num",
                        "Qty": "quantity",
                        "UnitPrice": "unit_price",
                        "ExtPrice": "ext_price",
                        "UnitCents": "unit_cents",
                        "ExtCents": "ext_cents",
                        "PromoCode": "promo_code",
                    },
                    "dc_inventory_txn": {
                        **common,
                        "DCID": "dc_id",
                        "ProductID": "product_id",
                        "QtyDelta": "quantity",
                        "Reason": "txn_type",
                        "Balance": "balance",
                    },
                    "truck_moves": {
                        **common,
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
                        **common,
                        "StoreID": "store_id",
                        "ProductID": "product_id",
                        "QtyDelta": "quantity",
                        "Reason": "txn_type",
                        "Source": "source",
                        "Balance": "balance",
                    },
                    "foot_traffic": {
                        **common,
                        "StoreID": "store_id",
                        "SensorId": "sensor_id",
                        "Zone": "zone",
                        "Dwell": "dwell_seconds",
                        "Count": "count",
                    },
                    "ble_pings": {
                        **common,
                        "StoreID": "store_id",
                        "CustomerBLEId": "customer_ble_id",
                        "BeaconId": "beacon_id",
                        "RSSI": "rssi",
                        "Zone": "zone",
                    },
                    "marketing": {
                        **common,
                        "CampaignId": "campaign_id",
                        "CreativeId": "creative_id",
                        "ImpressionId": "impression_id_ext",
                        "CustomerAdId": "customer_ad_id",
                        "Channel": "channel",
                        "Device": "device",
                        "Cost": "cost",
                    },
                    "online_orders": {
                        **common,
                        "CustomerID": "customer_id",
                        "Subtotal": "subtotal_amount",
                        "Tax": "tax_amount",
                        "Total": "total_amount",
                        "SubtotalCents": "subtotal_cents",
                        "TaxCents": "tax_cents",
                        "TotalCents": "total_cents",
                        "TenderType": "payment_method",
                        "CompletedTS": "completed_ts",
                        "OrderId": "order_id_ext",
                    },
                    "online_order_lines": {
                        **common,
                        "OrderId": "order_id",
                        "ProductID": "product_id",
                        "Line": "line_num",
                        "Qty": "quantity",
                        "UnitPrice": "unit_price",
                        "ExtPrice": "ext_price",
                        "UnitCents": "unit_cents",
                        "ExtCents": "ext_cents",
                        "PromoCode": "promo_code",
                        "PickedTS": "picked_ts",
                        "ShippedTS": "shipped_ts",
                        "DeliveredTS": "delivered_ts",
                        "FulfillmentStatus": "fulfillment_status",
                        "FulfillmentMode": "fulfillment_mode",
                        "NodeType": "node_type",
                        "NodeID": "node_id",
                    },
                }
                mp = mapping_tbl.get(table_name, common)
                rename_map = {k: v for k, v in mp.items() if k in df.columns and k != v}
                if rename_map:
                    df = df.rename(columns=rename_map)
                # Ensure external IDs on line tables
                if (
                    table_name == "receipt_lines"
                    and "receipt_id_ext" not in df.columns
                    and "ReceiptId" in df.columns
                ):
                    df["receipt_id_ext"] = df["ReceiptId"]
                    df = df.drop(columns=["ReceiptId"])
                if (
                    table_name == "online_order_lines"
                    and "order_id_ext" not in df.columns
                    and "OrderId" in df.columns
                ):
                    df["order_id_ext"] = df["OrderId"]

                duck_table = {
                    "dc_inventory_txn": "fact_dc_inventory_txn",
                    "truck_moves": "fact_truck_moves",
                    "store_inventory_txn": "fact_store_inventory_txn",
                    "receipts": "fact_receipts",
                    "receipt_lines": "fact_receipt_lines",
                    "foot_traffic": "fact_foot_traffic",
                    "ble_pings": "fact_ble_pings",
                    "marketing": "fact_marketing",
                    "online_orders": "fact_online_order_headers",
                    "online_order_lines": "fact_online_order_lines",
                    "fact_payments": "fact_payments",
                    "promotions": "fact_promotions",
                    "promo_lines": "fact_promo_lines",
                }.get(table_name, table_name)
                from retail_datagen.db.duckdb_engine import (
                    insert_dataframe,
                    outbox_insert_records,
                )

                inserted = insert_dataframe(self._duckdb_conn, duck_table, df)
                # Optionally mirror to streaming outbox (only for outbox-driven realtime)
                if getattr(self, "_publish_to_outbox", False):
                    try:
                        outbox_rows = self._build_outbox_rows_from_df(table_name, df)
                        if outbox_rows:
                            outbox_insert_records(self._duckdb_conn, outbox_rows)
                    except Exception as _outbox_exc:
                        logger.debug(
                            f"Outbox insert skipped for {table_name} hour {hour}: {_outbox_exc}"
                        )

                # Update per-table counts and emit progress
                try:
                    if not hasattr(self, "_table_insert_counts"):
                        self._table_insert_counts = {}
                    self._table_insert_counts[table_name] = (
                        self._table_insert_counts.get(table_name, 0) + int(inserted)
                    )

                    tracker_state = self.hourly_tracker.get_current_progress()
                    completed_hours = tracker_state.get("completed_hours", {}).get(
                        table_name, 0
                    )
                    total_days = tracker_state.get("total_days") or 1
                    total_hours_expected = max(1, total_days * 24)
                    per_table_fraction = min(
                        1.0, (completed_hours + 1.0) / total_hours_expected
                    )
                    self._emit_table_progress(
                        table_name,
                        per_table_fraction,
                        f"Writing {table_name.replace('_', ' ')} ({self._table_insert_counts[table_name]:,})",
                        {table_name: self._table_insert_counts[table_name]},
                    )
                except Exception as e:
                    logger.debug(f"Failed to emit progress for {table_name}: {e}")
            except Exception as e:
                logger.error(f"DuckDB insert failed for {table_name}: {e}")
            return

        # Map table name to model (SQLite path)
        try:
            model_class = self._get_model_for_table(table_name)
        except ValueError as e:
            logger.error(f"Cannot insert data: {e}")
            return

        # Special handling: link receipt_lines to receipts by external id
        if table_name == "receipt_lines":
            try:
                # Collect unique external ids
                ext_ids = list(
                    {r.get("ReceiptId") for r in records if r.get("ReceiptId")}
                )
                # Build map from external id -> numeric PK
                receipts_model = self._get_model_for_table("receipts")
                from sqlalchemy import select

                rows = (
                    await session.execute(
                        select(
                            receipts_model.receipt_id, receipts_model.receipt_id_ext
                        ).where(receipts_model.receipt_id_ext.in_(ext_ids))
                    )
                ).all()
                id_map = {ext: pk for (pk, ext) in rows}

                mapped_records = []
                for record in records:
                    mapped = self._map_field_names_for_db(table_name, record)
                    ext = record.get("ReceiptId")
                    pk = id_map.get(ext)
                    if not pk:
                        # No matching receipt yet; skip this line to preserve FK integrity
                        logger.debug(
                            f"Skipping receipt_line with unknown ReceiptId={ext}"
                        )
                        continue
                    mapped["receipt_id"] = int(pk)
                    mapped_records.append(mapped)
            except Exception as e:
                logger.error(f"Failed to resolve receipt_ids for receipt_lines: {e}")
                return
        elif table_name == "online_order_lines":
            try:
                # Collect unique external order ids from raw records
                ext_ids = list({r.get("OrderId") for r in records if r.get("OrderId")})
                if not ext_ids:
                    logger.debug("No online order external IDs to resolve for lines")
                    return
                # Map external order id -> header PK
                headers_model = self._get_model_for_table("online_orders")
                from sqlalchemy import select

                rows = (
                    await session.execute(
                        select(
                            headers_model.order_id, headers_model.order_id_ext
                        ).where(headers_model.order_id_ext.in_(ext_ids))
                    )
                ).all()
                id_map = {ext: pk for (pk, ext) in rows}

                mapped_records = []
                for record in records:
                    mapped = self._map_field_names_for_db(table_name, record)
                    ext = record.get("OrderId")
                    pk = id_map.get(ext)
                    if not pk:
                        logger.debug(
                            f"Skipping online_order_line with unknown OrderId={ext}"
                        )
                        continue
                    mapped["order_id"] = int(pk)
                    mapped_records.append(mapped)
            except Exception as e:
                logger.error(f"Failed to resolve order_ids for online_order_lines: {e}")
                return
        else:
            # Default mapping path
            mapped_records = [
                self._map_field_names_for_db(table_name, record) for record in records
            ]

        # Normalize pandas NaT/NaN values to None for DB serialization
        # Pandas can produce NaT (datetime) and NaN (float) which fail for
        # INTEGER/DATETIME columns during bulk insert. Convert any pd.isna(x)
        # values to plain None so NULLs are written instead.
        try:
            import pandas as _pd  # local import to avoid hard dependency elsewhere

            normalized: list[dict] = []
            for rec in mapped_records:
                clean: dict = {}
                for k, v in rec.items():
                    # Convert pandas/NumPy missing values to None
                    if _pd.isna(v):  # True for NaN/NaT
                        clean[k] = None
                    else:
                        clean[k] = v
                normalized.append(clean)
            mapped_records = normalized
        except (ImportError, AttributeError) as e:
            # If pandas isn't available or any issue occurs, proceed without normalization
            logger.debug(f"Failed to normalize pandas NA values for {table_name}: {e}")

        # Filter out any keys that are not actual columns in the target table
        try:
            allowed_cols = {col.name for col in model_class.__table__.columns}
            filtered_records = []
            for rec in mapped_records:
                filtered = {k: v for k, v in rec.items() if k in allowed_cols}
                filtered_records.append(filtered)
            mapped_records = filtered_records
        except (AttributeError, TypeError) as e:
            # Defensive: if column introspection fails, proceed without filtering
            logger.debug(f"Failed to filter columns for {table_name}: {e}")

        # Batch insert using bulk operations
        try:
            total_hour_rows = len(mapped_records)
            batch_index = 0
            for i in range(0, total_hour_rows, batch_size):
                batch = mapped_records[i : i + batch_size]

                # Use bulk insert for performance
                # Note: This doesn't populate auto-increment IDs back to Python objects
                await session.execute(model_class.__table__.insert(), batch)
                # Flush to DB
                await session.flush()

                logger.debug(
                    f"Inserted batch {i // batch_size + 1} for {table_name} hour {hour}: "
                    f"{len(batch)} rows"
                )

                # Live per-table progress and counts (master-style tiles)
                try:
                    # Update cumulative DB-written counts for this table
                    if not hasattr(self, "_table_insert_counts"):
                        self._table_insert_counts = {}
                    self._table_insert_counts[table_name] = (
                        self._table_insert_counts.get(table_name, 0) + len(batch)
                    )

                    # Compute fractional progress across the whole range using hourly tracker state
                    tracker_state = self.hourly_tracker.get_current_progress()
                    completed_hours = tracker_state.get("completed_hours", {}).get(
                        table_name, 0
                    )
                    total_days = tracker_state.get("total_days") or 1
                    total_hours_expected = max(1, total_days * 24)
                    # Partial hour progress within this hour based on batch position
                    partial_hour = (
                        (i + len(batch)) / total_hour_rows
                        if total_hour_rows > 0
                        else 1.0
                    )
                    per_table_fraction = min(
                        1.0, (completed_hours + partial_hour) / total_hours_expected
                    )

                    # Emit per-table progress callback (router merges counts and recomputes overall)
                    self._emit_table_progress(
                        table_name,
                        per_table_fraction,
                        f"Writing {table_name.replace('_', ' ')} ({self._table_insert_counts[table_name]:,})",
                        {table_name: self._table_insert_counts[table_name]},
                    )
                except Exception as _:
                    # Non-fatal: progress updates should not break inserts
                    pass

                # Periodic commit for durability
                batch_index += 1
                if commit_every_batches > 0 and (
                    batch_index % commit_every_batches == 0
                ):
                    try:
                        await session.commit()
                        logger.debug(
                            f"Committed {len(batch)} rows for {table_name} hour {hour}, batch {batch_index}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Interim commit failed for {table_name} hour {hour}: {e}"
                        )

            # Commit once after all batches (not per batch for performance)
            await session.commit()
            logger.info(
                f"Inserted {len(mapped_records)} rows for {table_name} hour {hour}"
            )

            # Optional verification: compare DB count delta (throttled by hour)
            try:
                if hour % 6 == 0:  # verify every 6 hours to reduce overhead
                    from sqlalchemy import func, select

                    total_db = (
                        await session.execute(
                            select(func.count()).select_from(model_class)
                        )
                    ).scalar() or 0
                    prev = 0
                    if not hasattr(self, "_fact_db_counts"):
                        self._fact_db_counts = {}
                    prev = int(self._fact_db_counts.get(table_name, 0))
                    added = int(total_db) - prev
                    self._fact_db_counts[table_name] = int(total_db)
                    logger.info(
                        f"{table_name} verification (hour {hour}): added={added}, expected={len(mapped_records)}, db_total={total_db}"
                    )
            except Exception as e:
                logger.debug(f"Verification skipped for {table_name} hour {hour}: {e}")

        except Exception as e:
            logger.error(
                f"Failed to insert hourly data for {table_name} hour {hour}: {e}"
            )
            await session.rollback()
            raise

    async def _update_watermarks_after_generation(
        self,
        start_date: datetime,
        end_date: datetime,
        active_tables: list[str],
    ) -> None:
        """
        Update watermarks for all generated fact tables.

        Marks the generated date range as unpublished so streaming knows
        where to start publishing from.

        Args:
            start_date: Start of generated data range
            end_date: End of generated data range
            active_tables: List of table names that were generated

        Note:
            Called after generation completes successfully.
        """
        if not self._session:
            return

        from retail_datagen.db.purge import mark_data_unpublished

        # Map generator table names to database table names
        table_name_mapping = {
            "dc_inventory_txn": "fact_dc_inventory_txn",
            "truck_moves": "fact_truck_moves",
            "store_inventory_txn": "fact_store_inventory_txn",
            "receipts": "fact_receipts",
            "receipt_lines": "fact_receipt_lines",
            "foot_traffic": "fact_foot_traffic",
            "ble_pings": "fact_ble_pings",
            "marketing": "fact_marketing",
            "online_orders": "fact_online_orders",
            "fact_payments": "fact_payments",
        }

        for table_name in active_tables:
            db_table_name = table_name_mapping.get(table_name)
            if db_table_name:
                try:
                    await mark_data_unpublished(
                        self._session, db_table_name, start_date, end_date
                    )
                    logger.debug(
                        f"Marked {db_table_name} as unpublished: "
                        f"{start_date} to {end_date}"
                    )
                except Exception as e:
                    logger.error(f"Failed to update watermark for {db_table_name}: {e}")
                    # Don't fail generation if watermark update fails
                    continue
