"""
Batch streaming operations for DuckDB and legacy SQLite.

This module handles batch streaming of historical events from
databases to Azure Event Hub.
"""

import random
from datetime import UTC, datetime

from ..azure_client import AzureEventHubClient
from ..schemas import EventEnvelope, EventType


class BatchStreamingManager:
    """Manages batch streaming from databases."""

    def __init__(self, log, session_id: str, streaming_config):
        """
        Initialize batch streaming manager.

        Args:
            log: Structured logger instance
            session_id: Session identifier for logging
            streaming_config: StreamingConfig instance
        """
        self.log = log
        self._session_id = session_id
        self.streaming_config = streaming_config

    async def start_batch_streaming_duckdb(
        self, duckdb_conn, azure_connection_string: str | None
    ) -> bool:
        """
        Start batch streaming from DuckDB database.

        Reads unpublished data from data/retail.duckdb and streams to Azure Event Hub,
        updating watermarks after successful publication.

        Args:
            duckdb_conn: DuckDB connection
            azure_connection_string: Azure Event Hub connection string

        Returns:
            bool: True if streaming completed successfully, False otherwise
        """
        if duckdb_conn is None:
            self.log.error(
                "DuckDB connection not available", session_id=self._session_id
            )
            return False

        self.log.info(
            "Starting batch streaming from DuckDB",
            session_id=self._session_id,
        )

        try:
            # Initialize Azure client
            azure_client = await self._init_azure_client(azure_connection_string)
            if azure_client is None:
                return False

            # Get streaming window from watermarks
            from retail_datagen.db.duck_watermarks import (
                get_unpublished_data_range,
                update_publication_watermark,
            )

            try:
                # Compute global window across all tables: earliest min, latest max
                earliest = None
                latest = None
                for tbl in self._get_fact_tables_duck():
                    start, end = get_unpublished_data_range(duckdb_conn, tbl)
                    if start and (earliest is None or start < earliest):
                        earliest = start
                    if end and (latest is None or end > latest):
                        latest = end
                if not earliest:
                    raise ValueError("No unpublished data found")
                if not latest:
                    latest = datetime.now(UTC)
                start_ts, end_ts = earliest, latest
                self.log.info(
                    f"Streaming data from {start_ts} to {end_ts} (DuckDB)",
                    session_id=self._session_id,
                )
            except ValueError as e:
                self.log.warning(str(e), session_id=self._session_id)
                await azure_client.disconnect()
                return True

            # Stream events per table
            total_published = 0
            for duck_table in self._get_fact_tables_duck():
                try:
                    events = self._load_unpublished_events_from_duck(
                        duckdb_conn, duck_table, start_ts, end_ts, batch_size=100000
                    )
                    if not events:
                        self.log.debug(
                            f"No unpublished events in {duck_table}",
                            session_id=self._session_id,
                        )
                        continue

                    logical_table = self._map_duck_table_to_logical(duck_table)
                    envelopes = self._convert_db_events_to_envelopes(
                        events, logical_table
                    )
                    if envelopes:
                        success = await azure_client.send_events(envelopes)
                        if success:
                            total_published += len(envelopes)
                            update_publication_watermark(
                                duckdb_conn, duck_table, end_ts
                            )
                            self.log.info(
                                f"Published {len(envelopes)} events from {duck_table}",
                                session_id=self._session_id,
                            )
                        else:
                            self.log.error(
                                f"Failed to publish events from {duck_table}",
                                session_id=self._session_id,
                            )
                except Exception as e:
                    self.log.error(
                        f"Error streaming {duck_table}: {e}",
                        session_id=self._session_id,
                        error_type=type(e).__name__,
                    )

            self.log.info(
                f"DuckDB batch streaming complete: {total_published} events published",
                session_id=self._session_id,
            )
            return True

        except Exception as e:
            self.log.error(
                "DuckDB batch streaming failed",
                session_id=self._session_id,
                error=str(e),
                error_type=type(e).__name__,
            )
            return False
        finally:
            if azure_client:
                await azure_client.disconnect()

    async def start_batch_streaming_sqlite(
        self, session, azure_connection_string: str | None
    ) -> bool:
        """
        Legacy: Start batch streaming from SQLite database (deprecated).

        Reads unpublished data from facts.db and streams to Azure Event Hub,
        updating watermarks after successful publication.

        Args:
            session: SQLAlchemy session
            azure_connection_string: Azure Event Hub connection string

        Returns:
            bool: True if streaming completed successfully, False otherwise
        """
        if not session:
            self.log.error(
                "Cannot start batch streaming without database session",
                session_id=self._session_id,
            )
            return False

        self.log.info(
            "Starting batch streaming from legacy SQLite (deprecated)",
            session_id=self._session_id,
        )

        try:
            # Initialize Azure client
            azure_client = await self._init_azure_client(azure_connection_string)
            if azure_client is None:
                return False

            # Get streaming window from watermarks
            try:
                start_ts, end_ts = await self._get_streaming_window_from_watermarks(
                    session
                )
                self.log.info(
                    f"Streaming data from {start_ts} to {end_ts}",
                    session_id=self._session_id,
                )
            except ValueError as e:
                self.log.warning(str(e), session_id=self._session_id)
                await azure_client.disconnect()
                return True  # No data to stream is not an error

            # Stream events from each table
            total_published = 0
            from retail_datagen.db.duck_watermarks import update_publication_watermark

            for table_name in self._get_fact_tables():
                fact_table_name = f"fact_{table_name}"
                try:
                    # Load unpublished events
                    events = await self._load_unpublished_events_from_db(
                        session, fact_table_name, start_ts, end_ts
                    )

                    if not events:
                        self.log.debug(
                            f"No unpublished events in {fact_table_name}",
                            session_id=self._session_id,
                        )
                        continue

                    self.log.info(
                        f"Loaded {len(events)} events from {fact_table_name}",
                        session_id=self._session_id,
                    )

                    # Convert to EventEnvelope format
                    envelopes = self._convert_db_events_to_envelopes(events, table_name)

                    # Publish events
                    if envelopes:
                        success = await azure_client.send_events(envelopes)

                        if success:
                            total_published += len(envelopes)

                            # Update watermark after successful publication
                            await update_publication_watermark(
                                session, fact_table_name, end_ts
                            )

                            self.log.info(
                                f"Published {len(envelopes)} events from {fact_table_name}",
                                session_id=self._session_id,
                            )
                        else:
                            self.log.error(
                                f"Failed to publish events from {fact_table_name}",
                                session_id=self._session_id,
                            )

                except Exception as e:
                    self.log.error(
                        f"Error streaming {fact_table_name}: {e}",
                        session_id=self._session_id,
                        error_type=type(e).__name__,
                    )
                    # Continue with next table

            self.log.info(
                f"Batch streaming complete: {total_published} events published",
                session_id=self._session_id,
            )
            return True

        except Exception as e:
            self.log.error(
                "Batch streaming failed",
                session_id=self._session_id,
                error=str(e),
                error_type=type(e).__name__,
            )
            return False
        finally:
            if azure_client:
                await azure_client.disconnect()

    async def _init_azure_client(
        self, azure_connection_string: str | None
    ) -> AzureEventHubClient | None:
        """Initialize Azure Event Hub client."""
        if azure_connection_string:
            azure_client = AzureEventHubClient(
                connection_string=azure_connection_string,
                hub_name=self.streaming_config.hub_name,
                max_batch_size=self.streaming_config.max_batch_size,
                batch_timeout_ms=self.streaming_config.batch_timeout_ms,
                retry_attempts=self.streaming_config.retry_attempts,
                backoff_multiplier=self.streaming_config.backoff_multiplier,
                circuit_breaker_enabled=self.streaming_config.circuit_breaker_enabled,
            )

            if not await azure_client.connect():
                self.log.error(
                    "Failed to connect to Azure Event Hub",
                    session_id=self._session_id,
                )
                return None
            return azure_client
        else:
            self.log.warning(
                "No Azure connection string - events will not be sent",
                session_id=self._session_id,
            )
            return AzureEventHubClient("", self.streaming_config.hub_name)

    def _get_fact_tables_duck(self) -> list[str]:
        """Get list of DuckDB fact table names."""
        return [
            "fact_receipts",
            "fact_receipt_lines",
            "fact_dc_inventory_txn",
            "fact_store_inventory_txn",
            "fact_truck_moves",
            "fact_foot_traffic",
            "fact_ble_pings",
            "fact_marketing",
            "fact_online_order_headers",
            "fact_online_order_lines",
        ]

    def _get_fact_tables(self) -> list[str]:
        """Get list of fact table names (without 'fact_' prefix)."""
        return [
            "receipts",
            "receipt_lines",
            "dc_inventory_txn",
            "store_inventory_txn",
            "truck_moves",
            "foot_traffic",
            "ble_pings",
            "marketing",
            "online_orders",
        ]

    def _map_duck_table_to_logical(self, duck_table: str) -> str:
        """Map DuckDB table name to logical table name."""
        mapping = {
            "fact_receipts": "receipts",
            "fact_receipt_lines": "receipt_lines",
            "fact_dc_inventory_txn": "dc_inventory_txn",
            "fact_store_inventory_txn": "store_inventory_txn",
            "fact_truck_moves": "truck_moves",
            "fact_foot_traffic": "foot_traffic",
            "fact_ble_pings": "ble_pings",
            "fact_marketing": "marketing",
            "fact_online_order_headers": "online_orders",
            "fact_online_order_lines": "online_orders",  # treat as order events
        }
        return mapping.get(duck_table, duck_table)

    def _load_unpublished_events_from_duck(
        self,
        duckdb_conn,
        duck_table: str,
        start_ts: datetime,
        end_ts: datetime,
        batch_size: int,
    ) -> list[dict]:
        """Load unpublished events from DuckDB."""
        if duckdb_conn is None:
            return []
        # Pull rows in window
        q = f"SELECT * FROM {duck_table} WHERE event_ts >= ? AND event_ts < ? ORDER BY event_ts LIMIT ?"
        cur = duckdb_conn.execute(q, [start_ts, end_ts, batch_size])
        rows = cur.fetchall()
        cols = [d[0] for d in (cur.description or [])]
        # Convert tuples to dicts
        events: list[dict] = []
        for tup in rows:
            rec = {cols[i]: tup[i] for i in range(len(cols))}
            events.append(rec)
        return events

    async def _load_unpublished_events_from_db(
        self,
        session,
        table_name: str,
        start_ts: datetime,
        end_ts: datetime,
        batch_size: int = 1000,
    ) -> list[dict]:
        """
        Load unpublished events from legacy SQLite facts.db (deprecated).

        Args:
            session: SQLAlchemy session
            table_name: Fact table name (e.g., "fact_receipts")
            start_ts: Start timestamp (from watermark)
            end_ts: End timestamp (current time or batch end)
            batch_size: Maximum events to return

        Returns:
            List of event records as dicts
        """
        if not session:
            raise ValueError(
                "No database session provided - cannot read from legacy SQLite"
            )

        # Import here to avoid circular dependencies
        from sqlalchemy import select

        from retail_datagen.db.models.facts import (
            BLEPing,
            DCInventoryTransaction,
            FootTraffic,
            MarketingImpression,
            OnlineOrder,
            Receipt,
            ReceiptLine,
            StoreInventoryTransaction,
            TruckMove,
        )

        # Map table name to model
        model_map = {
            "fact_receipts": Receipt,
            "fact_receipt_lines": ReceiptLine,
            "fact_dc_inventory_txn": DCInventoryTransaction,
            "fact_store_inventory_txn": StoreInventoryTransaction,
            "fact_truck_moves": TruckMove,
            "fact_foot_traffic": FootTraffic,
            "fact_ble_pings": BLEPing,
            "fact_marketing": MarketingImpression,
            "fact_online_orders": OnlineOrder,
        }

        model_class = model_map.get(table_name)
        if not model_class:
            raise ValueError(f"Unknown table: {table_name}")

        # Query unpublished data
        query = (
            select(model_class)
            .where(model_class.event_ts >= start_ts)
            .where(model_class.event_ts < end_ts)
            .order_by(model_class.event_ts)
            .limit(batch_size)
        )

        result = await session.execute(query)
        rows = result.scalars().all()

        # Convert to dicts
        events = []
        for row in rows:
            event_dict = {
                column.name: getattr(row, column.name)
                for column in row.__table__.columns
            }
            events.append(event_dict)

        return events

    async def _get_streaming_window_from_watermarks(
        self, session
    ) -> tuple[datetime, datetime]:
        """Get time window of unpublished data from watermarks."""
        from retail_datagen.db.purge import get_unpublished_data_range

        # Get earliest unpublished across all tables
        earliest = None
        latest = None

        for table in self._get_fact_tables():
            start, end = await get_unpublished_data_range(session, f"fact_{table}")
            if start:
                if not earliest or start < earliest:
                    earliest = start
            if end:
                if not latest or end > latest:
                    latest = end

        if not earliest:
            raise ValueError("No unpublished data found")

        if not latest:
            latest = datetime.now(UTC)

        return earliest, latest

    def _convert_db_events_to_envelopes(
        self, events: list[dict], table_name: str
    ) -> list[EventEnvelope]:
        """
        Convert database records to EventEnvelope format for streaming.

        Args:
            events: List of event records from database
            table_name: Table name (without 'fact_' prefix)

        Returns:
            List of EventEnvelope objects ready for streaming
        """
        # Map table names to event types
        event_type_map = {
            "receipts": EventType.RECEIPT_CREATED,
            "receipt_lines": EventType.RECEIPT_LINE_ADDED,
            "dc_inventory_txn": EventType.INVENTORY_UPDATED,
            "store_inventory_txn": EventType.INVENTORY_UPDATED,
            "truck_moves": EventType.TRUCK_ARRIVED,
            "foot_traffic": EventType.CUSTOMER_ENTERED,
            "ble_pings": EventType.BLE_PING_DETECTED,
            "marketing": EventType.AD_IMPRESSION,
            "online_orders": EventType.ONLINE_ORDER_CREATED,
        }

        event_type = event_type_map.get(table_name, EventType.RECEIPT_CREATED)

        envelopes = []
        for event_data in events:
            # Extract timestamp field (event_ts)
            timestamp = event_data.get("event_ts", datetime.now(UTC))
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)

            # Generate trace ID
            trace_id = f"TR_{int(timestamp.timestamp())}_{random.randint(10000, 99999)}"

            # Create envelope
            envelope = EventEnvelope(
                event_type=event_type,
                payload=event_data,
                trace_id=trace_id,
                ingest_timestamp=timestamp,
                schema_version="1.0",
                source="retail-datagen-batch",
            )
            envelopes.append(envelope)

        return envelopes

    async def _stream_pending_shipments(
        self, duckdb_conn, azure_client: AzureEventHubClient, up_to_time: datetime
    ) -> tuple[int, list[int]]:
        """
        Stream pending shipments from staging table that are ready for processing.

        Pending shipments are truck movements that were scheduled beyond the historical
        generation end date. They are stored in _staging_pending_shipments and picked up
        when streaming catches up to their departure time.

        Args:
            duckdb_conn: DuckDB connection
            azure_client: Azure Event Hub client for sending events
            up_to_time: Process shipments with departure_time <= this time

        Returns:
            Tuple of (events_published_count, list_of_staging_ids_processed)
        """
        from retail_datagen.db.duckdb_engine import (
            pending_shipments_delete,
            pending_shipments_get_ready,
        )

        # Get pending shipments ready for processing
        ready_shipments = pending_shipments_get_ready(duckdb_conn, up_to_time)

        if not ready_shipments:
            self.log.debug(
                "No pending shipments ready for streaming",
                session_id=self._session_id,
            )
            return 0, []

        self.log.info(
            f"Found {len(ready_shipments)} pending shipments ready for streaming",
            session_id=self._session_id,
        )

        # Convert to event envelopes
        envelopes = self._convert_db_events_to_envelopes(ready_shipments, "truck_moves")

        if not envelopes:
            return 0, []

        # Send events
        success = await azure_client.send_events(envelopes)

        if success:
            # Get staging IDs for deletion
            staging_ids = [
                s.get("_staging_id") for s in ready_shipments if s.get("_staging_id")
            ]

            # Delete processed shipments from staging
            if staging_ids:
                deleted = pending_shipments_delete(duckdb_conn, staging_ids)
                self.log.info(
                    f"Published {len(envelopes)} pending shipments, removed {deleted} from staging",
                    session_id=self._session_id,
                )

            return len(envelopes), staging_ids
        else:
            self.log.error(
                "Failed to publish pending shipments",
                session_id=self._session_id,
            )
            return 0, []
