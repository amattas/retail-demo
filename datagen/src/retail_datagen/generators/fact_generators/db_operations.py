"""
Database operations for fact data persistence.

Handles index management, bulk inserts, and watermark updates.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd

from .field_mapping import FieldMappingMixin

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class DbOperationsMixin(FieldMappingMixin):
    """Database operations for fact data persistence.

    Handles index management, bulk insert operations, and watermark updates.
    Inherits field mapping capabilities from FieldMappingMixin.
    """

    async def _capture_and_drop_indexes(
        self, session: AsyncSession, generator_table_names: list[str]
    ) -> list[tuple[str, str]]:
        """
        Capture and drop nonessential indexes for tables to speed bulk inserts.

        Returns a list of (index_name, create_sql) to recreate later.
        Keeps indexes that are critical to linkage lookups
        (receipt_id_ext, order_id_ext).
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
                    # Keep ext-id linkage indexes to avoid slow lookups
                    # during generation
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
                f"Failed to process data via pandas for {table_name}, "
                f"using fallback: {e}"
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
            await self._insert_via_duckdb(table_name, records, hour)
            return

        # SQLite fallback path
        await self._insert_via_sqlite(
            session, table_name, records, hour, batch_size, commit_every_batches
        )

    async def _insert_via_duckdb(
        self, table_name: str, records: list[dict], hour: int
    ) -> None:
        """Insert records via DuckDB fast path."""
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
                # Build dict-of-lists for existing keys across a small
                # sample to avoid missing columns
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

            # Apply column renaming
            df = self._apply_duckdb_column_rename(table_name, df)

            # Ensure string-type columns are explicitly typed (fix for OrderIdExt)
            if table_name == "fact_payments" and "order_id_ext" in df.columns:
                df["order_id_ext"] = df["order_id_ext"].astype(str)
                # Replace 'None' strings with actual None
                df.loc[df["order_id_ext"] == "None", "order_id_ext"] = None

            duck_table = self._get_duckdb_table_name(table_name)
            from retail_datagen.db.duckdb_engine import (
                insert_dataframe,
                outbox_insert_records,
            )

            inserted = insert_dataframe(self._duckdb_conn, duck_table, df)
            # Optionally mirror to streaming outbox
            # (only for outbox-driven realtime)
            if getattr(self, "_publish_to_outbox", False):
                try:
                    outbox_rows = self._build_outbox_rows_from_df(table_name, df)
                    if outbox_rows:
                        outbox_insert_records(self._duckdb_conn, outbox_rows)
                except Exception as _outbox_exc:
                    logger.debug(
                        f"Outbox insert skipped for {table_name} "
                        f"hour {hour}: {_outbox_exc}"
                    )

            # Update per-table counts and emit progress
            self._update_insert_progress(table_name, int(inserted))

        except Exception as e:
            logger.error(f"DuckDB insert failed for {table_name}: {e}")

    def _apply_duckdb_column_rename(
        self, table_name: str, df: pd.DataFrame
    ) -> pd.DataFrame:
        """Apply DuckDB-specific column renaming for a table."""
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
            "truck_inventory": {
                **common,
                "TruckId": "truck_id",
                "ShipmentId": "shipment_id",
                "ProductID": "product_id",
                "Quantity": "quantity",
                "Action": "action",
                "LocationID": "location_id",
                "LocationType": "location_type",
            },
            "reorders": {
                **common,
                "StoreID": "store_id",
                "DCID": "dc_id",
                "ProductID": "product_id",
                "CurrentQuantity": "current_quantity",
                "ReorderQuantity": "reorder_quantity",
                "ReorderPoint": "reorder_point",
                "Priority": "priority",
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

        return df

    def _get_duckdb_table_name(self, table_name: str) -> str:
        """Map generator table name to DuckDB table name."""
        return {
            "dc_inventory_txn": "fact_dc_inventory_txn",
            "truck_moves": "fact_truck_moves",
            "truck_inventory": "fact_truck_inventory",
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
            "store_ops": "fact_store_ops",
            "customer_zone_changes": "fact_customer_zone_changes",
            "stockouts": "fact_stockouts",
            "reorders": "fact_reorders",
        }.get(table_name, table_name)

    def _update_insert_progress(self, table_name: str, inserted: int) -> None:
        """Update per-table insert counts and emit progress."""
        try:
            if not hasattr(self, "_table_insert_counts"):
                self._table_insert_counts = {}
            self._table_insert_counts[table_name] = (
                self._table_insert_counts.get(table_name, 0) + inserted
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
            count = self._table_insert_counts[table_name]
            msg = f"Writing {table_name.replace('_', ' ')} ({count:,})"
            self._emit_table_progress(
                table_name,
                per_table_fraction,
                msg,
                {table_name: count},
            )
        except Exception as e:
            logger.debug(f"Failed to emit progress for {table_name}: {e}")

    async def _insert_via_sqlite(
        self,
        session: AsyncSession,
        table_name: str,
        records: list[dict],
        hour: int,
        batch_size: int,
        commit_every_batches: int,
    ) -> None:
        """Insert records via SQLite fallback path."""
        # Map table name to model (SQLite path)
        try:
            model_class = self._get_model_for_table(table_name)
        except ValueError as e:
            logger.error(f"Cannot insert data: {e}")
            return

        # Special handling: link receipt_lines to receipts by external id
        if table_name == "receipt_lines":
            mapped_records = await self._resolve_receipt_line_ids(
                session, table_name, records
            )
            if mapped_records is None:
                return
        elif table_name == "online_order_lines":
            mapped_records = await self._resolve_online_order_line_ids(
                session, table_name, records
            )
            if mapped_records is None:
                return
        else:
            # Default mapping path
            mapped_records = [
                self._map_field_names_for_db(table_name, record) for record in records
            ]

        # Normalize pandas NaT/NaN values to None for DB serialization
        mapped_records = self._normalize_pandas_values(table_name, mapped_records)

        # Filter out any keys that are not actual columns in the target table
        mapped_records = self._filter_to_allowed_columns(
            table_name, model_class, mapped_records
        )

        # Batch insert using bulk operations
        await self._execute_batch_insert(
            session,
            model_class,
            table_name,
            mapped_records,
            hour,
            batch_size,
            commit_every_batches,
        )

    async def _resolve_receipt_line_ids(
        self, session: AsyncSession, table_name: str, records: list[dict]
    ) -> list[dict] | None:
        """Resolve receipt line external IDs to numeric PKs."""
        try:
            # Collect unique external ids
            ext_ids = list({r.get("ReceiptId") for r in records if r.get("ReceiptId")})
            # Build map from external id -> numeric PK
            receipts_model = self._get_model_for_table("receipts")
            from sqlalchemy import select

            # SQLAlchemy ORM columns are dynamically defined; mypy can't see them
            rows = (
                await session.execute(
                    select(
                        receipts_model.receipt_id,  # type: ignore[attr-defined]
                        receipts_model.receipt_id_ext,  # type: ignore[attr-defined]
                    ).where(
                        receipts_model.receipt_id_ext.in_(ext_ids)  # type: ignore[attr-defined]
                    )
                )
            ).all()
            id_map = {ext: pk for (pk, ext) in rows}

            mapped_records = []
            for record in records:
                mapped = self._map_field_names_for_db(table_name, record)
                ext = record.get("ReceiptId")
                pk = id_map.get(ext)
                if not pk:
                    # No matching receipt yet; skip this line to
                    # preserve FK integrity
                    logger.debug(f"Skipping receipt_line with unknown ReceiptId={ext}")
                    continue
                mapped["receipt_id"] = int(pk)
                mapped_records.append(mapped)
            return mapped_records
        except Exception as e:
            logger.error(f"Failed to resolve receipt_ids for receipt_lines: {e}")
            return None

    async def _resolve_online_order_line_ids(
        self, session: AsyncSession, table_name: str, records: list[dict]
    ) -> list[dict] | None:
        """Resolve online order line external IDs to numeric PKs."""
        try:
            # Collect unique external order ids from raw records
            ext_ids = list({r.get("OrderId") for r in records if r.get("OrderId")})
            if not ext_ids:
                logger.debug("No online order external IDs to resolve for lines")
                return None
            # Map external order id -> header PK
            headers_model = self._get_model_for_table("online_orders")
            from sqlalchemy import select

            # SQLAlchemy ORM columns are dynamically defined; mypy can't see them
            rows = (
                await session.execute(
                    select(
                        headers_model.order_id,  # type: ignore[attr-defined]
                        headers_model.order_id_ext,  # type: ignore[attr-defined]
                    ).where(
                        headers_model.order_id_ext.in_(ext_ids)  # type: ignore[attr-defined]
                    )
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
            return mapped_records
        except Exception as e:
            logger.error(f"Failed to resolve order_ids for online_order_lines: {e}")
            return None

    def _normalize_pandas_values(
        self, table_name: str, mapped_records: list[dict]
    ) -> list[dict]:
        """Normalize pandas NaT/NaN values to None for DB serialization."""
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
            return normalized
        except (ImportError, AttributeError) as e:
            # If pandas isn't available or any issue occurs, proceed
            # without normalization
            logger.debug(f"Failed to normalize pandas NA values for {table_name}: {e}")
            return mapped_records

    def _filter_to_allowed_columns(
        self, table_name: str, model_class, mapped_records: list[dict]
    ) -> list[dict]:
        """Filter out any keys that are not actual columns in the target table."""
        try:
            allowed_cols = {col.name for col in model_class.__table__.columns}
            filtered_records = []
            for rec in mapped_records:
                filtered = {k: v for k, v in rec.items() if k in allowed_cols}
                filtered_records.append(filtered)
            return filtered_records
        except (AttributeError, TypeError) as e:
            # Defensive: if column introspection fails, proceed without filtering
            logger.debug(f"Failed to filter columns for {table_name}: {e}")
            return mapped_records

    async def _execute_batch_insert(
        self,
        session: AsyncSession,
        model_class,
        table_name: str,
        mapped_records: list[dict],
        hour: int,
        batch_size: int,
        commit_every_batches: int,
    ) -> None:
        """Execute batch insert operations for SQLite."""
        try:
            total_hour_rows = len(mapped_records)
            batch_index = 0
            for i in range(0, total_hour_rows, batch_size):
                batch = mapped_records[i : i + batch_size]

                # Use bulk insert for performance
                # Note: This doesn't populate auto-increment IDs back to Python objects
                # __table__ is typed as FromClause but is actually Table at runtime
                stmt = model_class.__table__.insert()  # type: ignore[attr-defined]
                await session.execute(stmt, batch)
                # Flush to DB
                await session.flush()

                batch_num = i // batch_size + 1
                logger.debug(
                    f"Inserted batch {batch_num} for {table_name} "
                    f"hour {hour}: {len(batch)} rows"
                )

                # Live per-table progress and counts (master-style tiles)
                self._update_batch_progress(
                    table_name, batch, i, total_hour_rows, len(batch)
                )

                # Periodic commit for durability
                batch_index += 1
                if commit_every_batches > 0 and (
                    batch_index % commit_every_batches == 0
                ):
                    try:
                        await session.commit()
                        logger.debug(
                            f"Committed {len(batch)} rows for {table_name} "
                            f"hour {hour}, batch {batch_index}"
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
            await self._verify_insert_counts(
                session, model_class, table_name, hour, mapped_records
            )

        except Exception as e:
            logger.error(
                f"Failed to insert hourly data for {table_name} hour {hour}: {e}"
            )
            await session.rollback()
            raise

    def _update_batch_progress(
        self,
        table_name: str,
        batch: list[dict],
        batch_start: int,
        total_hour_rows: int,
        batch_len: int,
    ) -> None:
        """Update progress during batch insert."""
        try:
            # Update cumulative DB-written counts for this table
            if not hasattr(self, "_table_insert_counts"):
                self._table_insert_counts = {}
            self._table_insert_counts[table_name] = (
                self._table_insert_counts.get(table_name, 0) + batch_len
            )

            # Compute fractional progress across the whole range
            # using hourly tracker state
            tracker_state = self.hourly_tracker.get_current_progress()
            completed_hours = tracker_state.get("completed_hours", {}).get(
                table_name, 0
            )
            total_days = tracker_state.get("total_days") or 1
            total_hours_expected = max(1, total_days * 24)
            # Partial hour progress within this hour based on batch position
            partial_hour = (
                (batch_start + batch_len) / total_hour_rows
                if total_hour_rows > 0
                else 1.0
            )
            per_table_fraction = min(
                1.0, (completed_hours + partial_hour) / total_hours_expected
            )

            # Emit per-table progress callback
            # (router merges counts and recomputes overall)
            count = self._table_insert_counts[table_name]
            msg = f"Writing {table_name.replace('_', ' ')} ({count:,})"
            self._emit_table_progress(
                table_name,
                per_table_fraction,
                msg,
                {table_name: count},
            )
        except Exception:
            # Non-fatal: progress updates should not break inserts
            pass

    async def _verify_insert_counts(
        self,
        session: AsyncSession,
        model_class,
        table_name: str,
        hour: int,
        mapped_records: list[dict],
    ) -> None:
        """Verify insert counts periodically (every 6 hours)."""
        try:
            if hour % 6 == 0:  # verify every 6 hours to reduce overhead
                from sqlalchemy import func, select

                total_db = (
                    await session.execute(select(func.count()).select_from(model_class))
                ).scalar() or 0
                prev = 0
                if not hasattr(self, "_fact_db_counts"):
                    self._fact_db_counts = {}
                prev = int(self._fact_db_counts.get(table_name, 0))
                added = int(total_db) - prev
                self._fact_db_counts[table_name] = int(total_db)
                expected = len(mapped_records)
                logger.info(
                    f"{table_name} verification (hour {hour}): "
                    f"added={added}, expected={expected}, "
                    f"db_total={total_db}"
                )
        except Exception as e:
            logger.debug(f"Verification skipped for {table_name} hour {hour}: {e}")

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
            "promotions": "fact_promotions",
            "promo_lines": "fact_promo_lines",
            "store_ops": "fact_store_ops",
            "customer_zone_changes": "fact_customer_zone_changes",
            "stockouts": "fact_stockouts",
            "reorders": "fact_reorders",
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
