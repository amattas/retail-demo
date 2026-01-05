"""
Main real-time event streaming engine.

This module provides the central EventStreamer class that orchestrates
event generation and streaming to Azure Event Hub with monitoring,
error handling, and performance optimization.
"""

import asyncio
import os
import signal
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import wraps
from typing import Any

from ..config.models import RetailConfig
from ..shared.logging_utils import get_structured_logger
from ..shared.metrics import event_generation_duration_seconds, metrics_collector
from ..shared.models import Customer, DistributionCenter, ProductMaster, Store
from .azure_client import AzureEventHubClient
from .errors import ErrorSeverity, classify_error
from .event_factory import EventFactory
from .schemas import EventEnvelope, EventType


def event_generation_pipeline(method: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to measure and post-process event generation."""

    @wraps(method)
    async def wrapper(self: "EventStreamer", *args: Any, **kwargs: Any):
        try:
            with event_generation_duration_seconds.time():
                events: list[EventEnvelope] = await method(self, *args, **kwargs)
        except Exception as exc:  # pragma: no cover - defensive logging
            self.log.error(
                "Error generating event burst",
                session_id=self._session_id,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return []

        self._process_generated_events(events)
        return events

    return wrapper


@dataclass
class StreamingStatistics:
    """Statistics and metrics for event streaming."""

    events_generated: int = 0
    events_sent_successfully: int = 0
    events_failed: int = 0
    batches_sent: int = 0
    total_streaming_time: float = 0.0
    events_per_second: float = 0.0
    bytes_sent: int = 0
    last_event_time: datetime | None = None
    event_type_counts: dict[EventType, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    error_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    connection_failures: int = 0
    circuit_breaker_trips: int = 0


@dataclass
class DLQEntry:
    """Dead letter queue entry with metadata."""

    event: EventEnvelope
    error_message: str
    error_category: str
    error_severity: str
    timestamp: str
    retry_count: int = 0
    last_retry_timestamp: str | None = None


@dataclass
class StreamingConfig:
    """Extended configuration for streaming operations."""

    emit_interval_ms: int = 500
    burst: int = 100
    azure_connection_string: str | None = None
    hub_name: str = "retail-events"
    max_batch_size: int = 256
    batch_timeout_ms: int = 1000
    retry_attempts: int = 3
    backoff_multiplier: float = 2.0
    circuit_breaker_enabled: bool = True
    monitoring_interval: int = 30  # seconds
    max_buffer_size: int = 10000
    enable_dead_letter_queue: bool = True
    dlq_max_size: int = 10000
    dlq_retry_enabled: bool = True
    dlq_retry_max_attempts: int = 3

    @classmethod
    def from_retail_config(cls, config: RetailConfig) -> "StreamingConfig":
        """Create streaming config from main retail config."""
        streaming_config = cls()
        streaming_config.emit_interval_ms = config.realtime.emit_interval_ms
        streaming_config.burst = config.realtime.burst
        streaming_config.hub_name = config.stream.hub

        # Get connection string using secure method (env var, Key Vault, or config)
        if hasattr(config.realtime, "get_connection_string"):
            streaming_config.azure_connection_string = (
                config.realtime.get_connection_string()
            )
        elif hasattr(config.realtime, "azure_connection_string"):
            streaming_config.azure_connection_string = (
                config.realtime.azure_connection_string
            )

        if hasattr(config.realtime, "max_batch_size"):
            streaming_config.max_batch_size = config.realtime.max_batch_size
        if hasattr(config.realtime, "batch_timeout_ms"):
            streaming_config.batch_timeout_ms = config.realtime.batch_timeout_ms
        if hasattr(config.realtime, "retry_attempts"):
            streaming_config.retry_attempts = config.realtime.retry_attempts
        if hasattr(config.realtime, "backoff_multiplier"):
            streaming_config.backoff_multiplier = config.realtime.backoff_multiplier
        if hasattr(config.realtime, "monitoring_interval"):
            streaming_config.monitoring_interval = config.realtime.monitoring_interval

        # DLQ configuration
        if hasattr(config.realtime, "dlq_max_size"):
            streaming_config.dlq_max_size = config.realtime.dlq_max_size
        if hasattr(config.realtime, "dlq_retry_enabled"):
            streaming_config.dlq_retry_enabled = config.realtime.dlq_retry_enabled
        if hasattr(config.realtime, "dlq_retry_max_attempts"):
            streaming_config.dlq_retry_max_attempts = (
                config.realtime.dlq_retry_max_attempts
            )

        return streaming_config


class EventStreamer:
    """
    Main real-time event streaming engine.

    Orchestrates the generation and streaming of retail events to Azure Event Hub
    with comprehensive monitoring, error handling, and performance optimization.

    Features:
    - Configurable event generation patterns
    - Robust Azure Event Hub integration
    - Real-time monitoring and statistics
    - Graceful shutdown handling
    - Dead letter queue for failed events
    - Circuit breaker for resilience
    - Performance metrics and health checks
    """

    def __init__(
        self,
        config: RetailConfig,
        stores: list[Store] | None = None,
        customers: list[Customer] | None = None,
        products: list[ProductMaster] | None = None,
        distribution_centers: list[DistributionCenter] | None = None,
        azure_connection_string: str | None = None,
    ):
        """
        Initialize the event streaming engine.

        Args:
            config: Main retail configuration
            stores: List of store master records (loaded if None)
            customers: List of customer master records (loaded if None)
            products: List of product master records (loaded if None)
            distribution_centers: List of DC master records (loaded if None)
            azure_connection_string: Azure Event Hub connection string
            session: Deprecated (DuckDB-only runtime)
        """
        self.config = config
        self.streaming_config = StreamingConfig.from_retail_config(config)

        # Override connection string if provided
        if azure_connection_string:
            self.streaming_config.azure_connection_string = azure_connection_string

        # Master data
        self._stores = stores
        self._customers = customers
        self._products = products
        self._distribution_centers = distribution_centers

        # DuckDB fast path for batch streaming (disabled in test mode)
        self._use_duckdb = True
        self._duckdb_conn = None
        try:
            from retail_datagen.db.duckdb_engine import get_duckdb_conn

            self._duckdb_conn = get_duckdb_conn()
        except Exception:
            self._use_duckdb = False

        # In unit-test mode, force in-memory streaming loop (no DuckDB batch path)
        if os.getenv("RETAIL_DATAGEN_TEST_MODE", "").strip().lower() in {"1", "true", "yes"}:
            self._use_duckdb = False
            self._duckdb_conn = None

        # Components
        self._azure_client: AzureEventHubClient | None = None
        self._event_factory: EventFactory | None = None

        # State management
        self._is_streaming = False
        self._is_shutdown = False
        self._streaming_task: asyncio.Task | None = None
        self._monitoring_task: asyncio.Task | None = None
        self._dlq_retry_task: asyncio.Task | None = None
        self._statistics = StreamingStatistics()
        self._event_buffer: list[EventEnvelope] = []

        # Enhanced DLQ with metadata
        self._dlq: list[DLQEntry] = []
        self._dlq_enabled = self.streaming_config.enable_dead_letter_queue
        self._dlq_max_size = self.streaming_config.dlq_max_size
        self._dlq_retry_enabled = self.streaming_config.dlq_retry_enabled
        self._dlq_retry_max_attempts = self.streaming_config.dlq_retry_max_attempts

        # Synchronization
        self._buffer_lock = asyncio.Lock()
        self._stats_lock = asyncio.Lock()
        self._dlq_lock = asyncio.Lock()

        # Pause/Resume state
        self._is_paused = False
        self._pause_lock = asyncio.Lock()
        self._pause_event = asyncio.Event()
        self._pause_event.set()  # Not paused initially

        # Pause statistics
        self._pause_count = 0

        # Session attribute (deprecated - kept for backward compatibility)
        self._session = None
        self._total_pause_duration = 0.0
        self._last_pause_time: float | None = None

        # Optional filter for allowed event types (set by API router)
        self._allowed_event_types: set[EventType] | None = None

        # Event hooks for extensibility (initialize empty by default)
        self._event_generated_hooks: list[Callable[[EventEnvelope], None]] = []
        self._event_sent_hooks: list[Callable[[EventEnvelope], None]] = []
        self._batch_sent_hooks: list[Callable[[list[EventEnvelope]], None]] = []
        self._error_hooks: list[Callable[[Exception, str], None]] = []

        # Structured logger with session tracking
        self.log = get_structured_logger(__name__)
        self._session_id = self.log.generate_correlation_id()
        self.log.set_correlation_id(self._session_id)

        # Metrics collector
        self.metrics = metrics_collector

        # Daily target tracking for event pacing
        self._daily_targets: dict[EventType, int] = {}
        self._daily_counts: dict[EventType, int] = {}
        self._current_day: str | None = None

    def _compute_daily_targets(self):
        """Compute rough daily targets for key event types based on config and master sizes."""
        stores = len(self._stores) if self._stores else 1
        dcs = len(self._distribution_centers) if self._distribution_centers else 1
        cpd = getattr(self.config.volume, "customers_per_day", 20000) or 20000
        ood = getattr(self.config.volume, "online_orders_per_day", 2500) or 2500
        targets = {
            EventType.RECEIPT_CREATED: cpd,
            EventType.CUSTOMER_ENTERED: stores * 100,
            EventType.BLE_PING_DETECTED: stores * 500,
            EventType.INVENTORY_UPDATED: stores * 20 + dcs * 50,
            EventType.TRUCK_ARRIVED: 10,
            EventType.AD_IMPRESSION: 10000,  # marketing can be high volume
            EventType.ONLINE_ORDER_CREATED: ood,
        }
        self._daily_targets = targets
        self._daily_counts = {et: 0 for et in targets.keys()}
        self._current_day = datetime.now(UTC).strftime("%Y-%m-%d")

    def _reset_daily_if_needed(self, ts: datetime):
        day = ts.strftime("%Y-%m-%d")
        if self._current_day != day:
            self._compute_daily_targets()

    def _build_event_weights(self, ts: datetime) -> dict[EventType, float]:
        """Build event weights biased by remaining quota for the current day."""
        self._reset_daily_if_needed(ts)
        weights: dict[EventType, float] = {}
        # Base tiny weight for all known types to keep variety
        base = 0.01
        for et, target in self._daily_targets.items():
            remaining = max(target - self._daily_counts.get(et, 0), 0)
            # Weight proportional to remaining, with base floor
            weights[et] = base + remaining / max(target, 1)
        return weights

    def set_allowed_event_types(self, event_type_names: list[str] | None):
        """Optionally restrict emitted events to a subset of EventType names."""
        if not event_type_names:
            self._allowed_event_types = None
            return
        allowed = set()
        for name in event_type_names:
            try:
                allowed.add(EventType(name))
            except Exception:
                # Ignore invalid names; router should validate
                pass
        self._allowed_event_types = allowed if allowed else None

        # Setup signal handling for graceful shutdown
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.log.info(
            "Received shutdown signal",
            session_id=self._session_id,
            signal=signum,
        )
        asyncio.create_task(self.stop())

    async def initialize(self) -> bool:
        """
        Initialize all streaming components.

        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            # Load master data if not provided
            await self._ensure_master_data_loaded()

            # Initialize Azure Event Hub client
            if self.streaming_config.azure_connection_string:
                self._azure_client = AzureEventHubClient(
                    connection_string=self.streaming_config.azure_connection_string,
                    hub_name=self.streaming_config.hub_name,
                    max_batch_size=self.streaming_config.max_batch_size,
                    batch_timeout_ms=self.streaming_config.batch_timeout_ms,
                    retry_attempts=self.streaming_config.retry_attempts,
                    backoff_multiplier=self.streaming_config.backoff_multiplier,
                    circuit_breaker_enabled=self.streaming_config.circuit_breaker_enabled,
                )

                # Test connection
                if not await self._azure_client.connect():
                    self.log.error(
                        "Failed to connect to Azure Event Hub",
                        session_id=self._session_id,
                    )
                    return False

                self.log.info(
                    "Azure Event Hub client initialized and connected",
                    session_id=self._session_id,
                )
            else:
                self.log.warning(
                    "No Azure connection string provided - events will be generated but not sent",
                    session_id=self._session_id,
                )
                # Create mock client for testing
                self._azure_client = AzureEventHubClient(
                    "", self.streaming_config.hub_name
                )

            # Initialize event factory
            self._event_factory = EventFactory(
                stores=(
                    list(self._stores.values())
                    if isinstance(self._stores, dict)
                    else self._stores
                ),
                customers=(
                    list(self._customers.values())
                    if isinstance(self._customers, dict)
                    else self._customers
                ),
                products=(
                    list(self._products.values())
                    if isinstance(self._products, dict)
                    else self._products
                ),
                distribution_centers=(
                    list(self._distribution_centers.values())
                    if isinstance(self._distribution_centers, dict)
                    else self._distribution_centers
                ),
                seed=self.config.seed,
            )

            # Compute daily targets for pacing after data load
            self._compute_daily_targets()

            self.log.info(
                "Event streaming engine initialized successfully",
                session_id=self._session_id,
            )
            return True

        except Exception as e:
            self.log.error(
                "Failed to initialize streaming engine",
                session_id=self._session_id,
                error=str(e),
                error_type=type(e).__name__,
            )
            return False

    async def _ensure_master_data_loaded(self):
        """Ensure all required master data is loaded from DuckDB."""
        if all([self._stores, self._customers, self._products, self._distribution_centers]):
            return
        self.log.info("Loading master data for streaming from DuckDB", session_id=self._session_id)
        try:
            from retail_datagen.db.duck_master_reader import (
                read_customers,
                read_distribution_centers,
                read_products,
                read_stores,
            )

            if not self._stores:
                self._stores = read_stores()
            if not self._customers:
                self._customers = read_customers()
            if not self._products:
                self._products = read_products()
            if not self._distribution_centers:
                self._distribution_centers = read_distribution_centers()
        except Exception as e:
            # Fallback: synthesize a minimal set of master data to allow simulation/tests
            self.log.error(
                "Failed to load master data from DuckDB",
                session_id=self._session_id,
                error=str(e),
                error_type=type(e).__name__,
            )
            self.log.warning(
                "Falling back to minimal in-memory master data for streaming",
                session_id=self._session_id,
            )
            from decimal import Decimal

            from retail_datagen.shared.models import (
                Customer,
                DistributionCenter,
                ProductMaster,
                Store,
            )
            now = datetime.now(UTC)
            if not self._stores:
                self._stores = [
                    Store(ID=1, StoreNumber="ST001", Address="123 Main St", GeographyID=1),
                    Store(ID=2, StoreNumber="ST002", Address="456 Oak Ave", GeographyID=2),
                ]
            if not self._customers:
                self._customers = [
                    Customer(
                        ID=1,
                        FirstName="Alex",
                        LastName="Anderson",
                        Address="789 Pine St",
                        GeographyID=1,
                        LoyaltyCard="LC000001",
                        Phone="(555) 111-1111",
                        BLEId="BLE001",
                        AdId="AD001",
                    ),
                    Customer(
                        ID=2,
                        FirstName="Blake",
                        LastName="Brightwell",
                        Address="321 Elm St",
                        GeographyID=2,
                        LoyaltyCard="LC000002",
                        Phone="(555) 222-2222",
                        BLEId="BLE002",
                        AdId="AD002",
                    ),
                ]
            if not self._products:
                self._products = [
                    ProductMaster(
                        ID=1,
                        ProductName="Widget Pro",
                        Brand="TestBrand",
                        Company="TestCo",
                        Department="Electronics",
                        Category="Gadgets",
                        Subcategory="Widgets",
                        Cost=Decimal("10.00"),
                        MSRP=Decimal("20.00"),
                        SalePrice=Decimal("18.00"),
                        RequiresRefrigeration=False,
                        LaunchDate=now,
                    ),
                    ProductMaster(
                        ID=2,
                        ProductName="Gadget Plus",
                        Brand="TestBrand",
                        Company="TestCo",
                        Department="Electronics",
                        Category="Gadgets",
                        Subcategory="Gadgets",
                        Cost=Decimal("15.00"),
                        MSRP=Decimal("25.00"),
                        SalePrice=Decimal("22.00"),
                        RequiresRefrigeration=False,
                        LaunchDate=now,
                    ),
                ]
            if not self._distribution_centers:
                self._distribution_centers = [
                    DistributionCenter(ID=1, DCNumber="DC001", Address="999 Industrial Blvd", GeographyID=1),
                    DistributionCenter(ID=2, DCNumber="DC002", Address="888 Warehouse Way", GeographyID=2),
                ]
            # Minimal fallback prepared; do not raise

    async def start(self, duration: timedelta | None = None) -> bool:
        """
        Start the event streaming process.

        Args:
            duration: Optional duration to stream for. If None, streams indefinitely.

        Returns:
            bool: True if streaming started successfully, False otherwise
        """
        if self._is_streaming:
            self.log.warning("Streaming is already active", session_id=self._session_id)
            return False

        # Check if using database mode (batch streaming)
        if self._use_duckdb:
            return await self.start_batch_streaming_duckdb()

        if not await self.initialize():
            self.log.error(
                "Failed to initialize streaming engine",
                session_id=self._session_id,
            )
            return False

        self._is_streaming = True
        self._is_shutdown = False
        start_time = datetime.now(UTC)
        end_time = start_time + duration if duration else None

        # Record streaming start in metrics
        self.metrics.start_streaming()

        self.log.info(
            "Starting streaming session",
            session_id=self._session_id,
            duration=str(duration) if duration else "indefinite",
            emit_interval_ms=self.streaming_config.emit_interval_ms,
            burst_size=self.streaming_config.burst,
            start_time=start_time.isoformat(),
        )

        try:
            # Start monitoring task
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())

            # Start DLQ retry task if enabled
            if self._dlq_retry_enabled:
                self._dlq_retry_task = asyncio.create_task(self._dlq_retry_loop())

            # Start main streaming task
            self._streaming_task = asyncio.create_task(
                self._streaming_loop(start_time, end_time)
            )

            # Wait for streaming to complete
            await self._streaming_task

            self.log.info(
                "Event streaming completed successfully",
                session_id=self._session_id,
            )
            return True

        except Exception as exc:
            self.log.error(
                "Error during streaming",
                session_id=self._session_id,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return False
        finally:
            await self._cleanup()

    async def start_batch_streaming(self) -> bool:
        """
        Legacy: Start batch streaming from SQLite database (deprecated).

        Reads unpublished data from facts.db and streams to Azure Event Hub,
        updating watermarks after successful publication.

        Returns:
            bool: True if streaming completed successfully, False otherwise
        """
        if not self._session:
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
            if self.streaming_config.azure_connection_string:
                self._azure_client = AzureEventHubClient(
                    connection_string=self.streaming_config.azure_connection_string,
                    hub_name=self.streaming_config.hub_name,
                    max_batch_size=self.streaming_config.max_batch_size,
                    batch_timeout_ms=self.streaming_config.batch_timeout_ms,
                    retry_attempts=self.streaming_config.retry_attempts,
                    backoff_multiplier=self.streaming_config.backoff_multiplier,
                    circuit_breaker_enabled=self.streaming_config.circuit_breaker_enabled,
                )

                if not await self._azure_client.connect():
                    self.log.error(
                        "Failed to connect to Azure Event Hub",
                        session_id=self._session_id,
                    )
                    return False
            else:
                self.log.warning(
                    "No Azure connection string - events will not be sent",
                    session_id=self._session_id,
                )
                self._azure_client = AzureEventHubClient(
                    "", self.streaming_config.hub_name
                )

            # Get streaming window from watermarks
            try:
                start_ts, end_ts = await self._get_streaming_window_from_watermarks()
                self.log.info(
                    f"Streaming data from {start_ts} to {end_ts}",
                    session_id=self._session_id,
                )
            except ValueError as e:
                self.log.warning(str(e), session_id=self._session_id)
                return True  # No data to stream is not an error

            # Stream events from each table
            total_published = 0
            from ..db.duck_watermarks import update_publication_watermark

            for table_name in self._get_fact_tables():
                fact_table_name = f"fact_{table_name}"
                try:
                    # Load unpublished events
                    events = await self._load_unpublished_events_from_db(
                        fact_table_name, start_ts, end_ts
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
                        success = await self._azure_client.send_events(envelopes)

                        if success:
                            total_published += len(envelopes)

                            # Update watermark after successful publication
                            await update_publication_watermark(
                                self._session, fact_table_name, end_ts
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
            if self._azure_client:
                await self._azure_client.disconnect()

    async def start_batch_streaming_duckdb(self) -> bool:
        """
        Start batch streaming from DuckDB database.

        Reads unpublished data from data/retail.duckdb and streams to Azure Event Hub,
        updating watermarks after successful publication.

        Returns:
            bool: True if streaming completed successfully, False otherwise
        """
        if not (self._use_duckdb and self._duckdb_conn is not None):
            self.log.error("DuckDB connection not available", session_id=self._session_id)
            return False

        self.log.info(
            "Starting batch streaming from DuckDB",
            session_id=self._session_id,
        )

        try:
            # Initialize Azure client
            if self.streaming_config.azure_connection_string:
                self._azure_client = AzureEventHubClient(
                    connection_string=self.streaming_config.azure_connection_string,
                    hub_name=self.streaming_config.hub_name,
                    max_batch_size=self.streaming_config.max_batch_size,
                    batch_timeout_ms=self.streaming_config.batch_timeout_ms,
                    retry_attempts=self.streaming_config.retry_attempts,
                    backoff_multiplier=self.streaming_config.backoff_multiplier,
                    circuit_breaker_enabled=self.streaming_config.circuit_breaker_enabled,
                )

                if not await self._azure_client.connect():
                    self.log.error(
                        "Failed to connect to Azure Event Hub",
                        session_id=self._session_id,
                    )
                    return False
            else:
                self.log.warning(
                    "No Azure connection string - events will not be sent",
                    session_id=self._session_id,
                )
                self._azure_client = AzureEventHubClient(
                    "", self.streaming_config.hub_name
                )

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
                    start, end = get_unpublished_data_range(self._duckdb_conn, tbl)
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
                return True

            # Stream events per table
            total_published = 0
            for duck_table in self._get_fact_tables_duck():
                try:
                    events = self._load_unpublished_events_from_duck(
                        duck_table, start_ts, end_ts, batch_size=100000
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
                        success = await self._azure_client.send_events(envelopes)
                        if success:
                            total_published += len(envelopes)
                            update_publication_watermark(
                                self._duckdb_conn, duck_table, end_ts
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
            if self._azure_client:
                await self._azure_client.disconnect()

    def _get_fact_tables_duck(self) -> list[str]:
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

    def _map_duck_table_to_logical(self, duck_table: str) -> str:
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
        self, duck_table: str, start_ts: datetime, end_ts: datetime, batch_size: int
    ) -> list[dict]:
        if not (self._use_duckdb and self._duckdb_conn is not None):
            return []
        # Pull rows in window
        q = (
            f"SELECT * FROM {duck_table} WHERE event_ts >= ? AND event_ts < ? ORDER BY event_ts LIMIT ?"
        )
        cur = self._duckdb_conn.execute(q, [start_ts, end_ts, batch_size])
        rows = cur.fetchall()
        cols = [d[0] for d in (cur.description or [])]
        # Convert tuples to dicts
        events: list[dict] = []
        for tup in rows:
            rec = {cols[i]: tup[i] for i in range(len(cols))}
            events.append(rec)
        return events

    async def stop(self) -> bool:
        """
        Stop the event streaming process gracefully.

        Returns:
            bool: True if stopped successfully, False otherwise
        """
        if not self._is_streaming:
            self.log.info("Streaming is not active", session_id=self._session_id)
            return True

        self.log.info("Stopping event streaming", session_id=self._session_id)
        self._is_shutdown = True

        try:
            # Cancel tasks
            if self._streaming_task and not self._streaming_task.done():
                self._streaming_task.cancel()
                try:
                    await self._streaming_task
                except asyncio.CancelledError:
                    pass

            if self._monitoring_task and not self._monitoring_task.done():
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass

            if self._dlq_retry_task and not self._dlq_retry_task.done():
                self._dlq_retry_task.cancel()
                try:
                    await self._dlq_retry_task
                except asyncio.CancelledError:
                    pass

            # Flush any remaining events
            await self._flush_remaining_events()

            await self._cleanup()

            # Record streaming stop in metrics
            self.metrics.stop_streaming()

            self.log.info(
                "Event streaming stopped successfully",
                session_id=self._session_id,
            )
            return True

        except Exception as e:
            self.log.error(
                "Error stopping streaming",
                session_id=self._session_id,
                error=str(e),
                error_type=type(e).__name__,
            )
            return False

    async def _streaming_loop(self, start_time: datetime, end_time: datetime | None):
        """Main streaming loop that generates and sends events."""
        next_burst_time = start_time
        batch_count = 0

        while not self._is_shutdown:
            # Wait if paused
            await self._pause_event.wait()

            # Check if still streaming (might have stopped during pause)
            if not self._is_streaming or self._is_shutdown:
                break

            current_time = datetime.now(UTC)

            # Check if we've reached the end time
            if end_time and current_time >= end_time:
                self.log.info(
                    "Streaming duration completed", session_id=self._session_id
                )
                break

            # Check if it's time for the next burst
            if current_time >= next_burst_time:
                try:
                    batch_id = self.log.generate_correlation_id()
                    self.log.debug(
                        "Generating event batch",
                        batch_id=batch_id,
                        batch_number=batch_count,
                        target_size=self.streaming_config.burst,
                        session_id=self._session_id,
                    )

                    # Generate event burst
                    events = await self._generate_event_burst(current_time)

                    if events:
                        # Enrich events with correlation and session identifiers
                        for event in events:
                            if not event.correlation_id:
                                event.correlation_id = batch_id
                            event.session_id = self._session_id

                        self.log.info(
                            "Event batch generated",
                            batch_id=batch_id,
                            event_count=len(events),
                            event_types=[str(e.event_type) for e in events[:5]],
                            session_id=self._session_id,
                        )

                        # Buffer events
                        async with self._buffer_lock:
                            self._event_buffer.extend(events)

                            # Update statistics
                            async with self._stats_lock:
                                self._statistics.events_generated += len(events)
                                for event in events:
                                    self._statistics.event_type_counts[
                                        event.event_type
                                    ] += 1

                        # Send events if buffer is large enough
                        if (
                            len(self._event_buffer)
                            >= self.streaming_config.max_batch_size
                        ):
                            await self._flush_event_buffer()

                    # Calculate next burst time
                    next_burst_time = current_time + timedelta(
                        milliseconds=self.streaming_config.emit_interval_ms
                    )
                    batch_count += 1

                except Exception as exc:
                    self.log.error(
                        "Streaming loop error",
                        error=str(exc),
                        error_type=type(exc).__name__,
                        batch_number=batch_count,
                        session_id=self._session_id,
                    )
                    async with self._stats_lock:
                        self._statistics.error_counts["streaming_loop_errors"] += 1

                    # Call error hooks
                    self._run_hooks_once(self._error_hooks, exc, "streaming_loop")

            # Sleep for a short interval to avoid busy waiting
            await asyncio.sleep(0.1)

        self._is_streaming = False

    def _process_generated_events(self, events: list[EventEnvelope]) -> None:
        """Record metrics and run hooks for generated events."""
        if not events:
            return

        for event in events:
            self.metrics.record_event_generated(event.event_type)

        self._run_event_hooks(events, self._event_generated_hooks)

    @staticmethod
    def _run_hooks_once(hooks: list[Callable[..., None]], *args: Any) -> None:
        for hook in hooks:
            try:
                hook(*args)
            except Exception:
                pass

    def _run_event_hooks(
        self,
        events: list[EventEnvelope],
        hooks: list[Callable[[EventEnvelope], None]],
    ) -> None:
        if not events or not hooks:
            return

        for event in events:
            self._run_hooks_once(hooks, event)

    @event_generation_pipeline
    async def _generate_event_burst(self, timestamp: datetime) -> list[EventEnvelope]:
        """
        Generate a burst of mixed events.

        Args:
            timestamp: Base timestamp for event generation

        Returns:
            List of generated events
        """
        # Build weights based on daily targets
        all_weights = self._build_event_weights(timestamp)
        if self._allowed_event_types:
            weights = {
                et: w
                for et, w in all_weights.items()
                if et in self._allowed_event_types
            }
            if not weights:
                weights = {et: 1.0 for et in self._allowed_event_types}
        else:
            weights = all_weights

        events = self._event_factory.generate_mixed_events(
            count=self.streaming_config.burst,
            timestamp=timestamp,
            event_weights=weights,
        )

        # Update daily counts for pacing (protected by stats lock)
        async with self._stats_lock:
            for ev in events:
                if ev.event_type in self._daily_targets:
                    self._daily_counts[ev.event_type] = (
                        self._daily_counts.get(ev.event_type, 0) + 1
                    )

        return events

    async def _flush_event_buffer(self):
        """Flush events from buffer to Azure Event Hub."""
        if not self._event_buffer:
            return

        async with self._buffer_lock:
            events_to_send = self._event_buffer.copy()
            self._event_buffer.clear()

        batch_id = events_to_send[0].correlation_id if events_to_send else "unknown"

        self.log.debug(
            "Flushing event buffer",
            batch_id=batch_id,
            event_count=len(events_to_send),
            session_id=self._session_id,
        )

        try:
            # Track batch send timing
            batch_start_time = time.time()
            success = await self._azure_client.send_events(events_to_send)
            batch_duration = time.time() - batch_start_time

            async with self._stats_lock:
                if success:
                    self._statistics.events_sent_successfully += len(events_to_send)
                    self._statistics.batches_sent += 1
                    self._statistics.last_event_time = datetime.now(UTC)

                    # Estimate bytes sent (rough calculation)
                    estimated_bytes = sum(
                        len(str(event.payload)) + 200
                        for event in events_to_send  # +200 for envelope
                    )
                    self._statistics.bytes_sent += estimated_bytes

                    # Record metrics for successful batch
                    self.metrics.record_batch_sent(estimated_bytes, batch_duration)
                    for event in events_to_send:
                        self.metrics.record_event_sent(event.event_type)

                    self.log.info(
                        "Event batch sent",
                        batch_id=batch_id,
                        event_count=len(events_to_send),
                        duration_seconds=batch_duration,
                        session_id=self._session_id,
                    )
                else:
                    # Send failed - record metrics
                    self.metrics.record_batch_failed("send_failed")
                    for event in events_to_send:
                        self.metrics.record_event_failed(
                            event.event_type, "send_failed"
                        )

                    # Send failed - add to DLQ with metadata
                    await self._handle_send_failure(
                        events_to_send, Exception("Send failed")
                    )

                    self.log.warning(
                        "Event batch send failed",
                        batch_id=batch_id,
                        event_count=len(events_to_send),
                        session_id=self._session_id,
                    )

            # Call batch sent hooks
            if success:
                self._run_hooks_once(self._batch_sent_hooks, events_to_send)

                # Call individual event sent hooks
                self._run_event_hooks(events_to_send, self._event_sent_hooks)

        except Exception as e:
            # Record metrics for exception
            error_type = type(e).__name__
            self.metrics.record_batch_failed(error_type)
            for event in events_to_send:
                self.metrics.record_event_failed(event.event_type, error_type)

            # Handle send failure with error classification
            await self._handle_send_failure(events_to_send, e)
            async with self._stats_lock:
                self._statistics.error_counts["flush_errors"] += 1

            self.log.error(
                "Error flushing event buffer",
                batch_id=batch_id,
                error=str(e),
                error_type=error_type,
                session_id=self._session_id,
            )

    async def _flush_remaining_events(self):
        """Flush any remaining events in buffer during shutdown."""
        if self._event_buffer:
            self.log.info(
                "Flushing remaining events",
                event_count=len(self._event_buffer),
                session_id=self._session_id,
            )
            await self._flush_event_buffer()

    async def _monitoring_loop(self):
        """Background monitoring loop for statistics and health checks."""
        while not self._is_shutdown:
            try:
                # Update Prometheus metrics
                self.metrics.update_uptime()
                self.metrics.update_dlq_size(len(self._dlq))

                # Update performance metrics
                async with self._stats_lock:
                    if self._statistics.events_generated > 0:
                        elapsed_time = (
                            datetime.now(UTC)
                            - (self._statistics.last_event_time or datetime.now(UTC))
                        ).total_seconds()

                        if elapsed_time > 0:
                            self._statistics.events_per_second = (
                                self._statistics.events_sent_successfully
                                / max(elapsed_time, 1)
                            )

                # Log statistics periodically
                if (
                    self._statistics.events_generated % 1000 == 0
                    and self._statistics.events_generated > 0
                ):
                    stats = await self.get_statistics()
                    self.log.info(
                        "Streaming stats",
                        session_id=self._session_id,
                        events_generated=stats["events_generated"],
                        events_sent=stats["events_sent_successfully"],
                        events_per_second=stats["events_per_second"],
                    )

                # Perform health check
                if self._azure_client:
                    health = await self._azure_client.health_check()
                    if not health.get("healthy", False):
                        self.log.warning(
                            "Event Hub health check failed",
                            session_id=self._session_id,
                            health=health,
                        )
                        async with self._stats_lock:
                            self._statistics.connection_failures += 1

                await asyncio.sleep(self.streaming_config.monitoring_interval)

            except Exception as exc:
                self.log.error(
                    "Error in monitoring loop",
                    session_id=self._session_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                await asyncio.sleep(self.streaming_config.monitoring_interval)

    async def _cleanup(self):
        """Clean up resources."""
        if self._azure_client:
            await self._azure_client.disconnect()

        self._is_streaming = False

    async def pause(self) -> dict:
        """
        Pause streaming without stopping completely.

        Events will not be generated or sent until resumed.
        State is maintained so streaming can continue seamlessly.

        Returns:
            {
                "success": bool,
                "message": str,
                "paused_at": str (ISO timestamp),
                "events_sent_before_pause": int
            }
        """
        async with self._pause_lock:
            if not self._is_streaming:
                return {
                    "success": False,
                    "message": "Cannot pause: streaming is not active",
                    "paused_at": None,
                    "events_sent_before_pause": 0,
                }

            if self._is_paused:
                return {
                    "success": False,
                    "message": "Already paused",
                    "paused_at": (
                        datetime.fromtimestamp(self._last_pause_time, UTC).isoformat()
                        if self._last_pause_time
                        else None
                    ),
                    "events_sent_before_pause": self._statistics.events_sent_successfully,
                }

            # Set pause state
            self._is_paused = True
            self._pause_event.clear()  # Block streaming loop
            self._last_pause_time = time.time()
            self._pause_count += 1

            # Record pause in metrics
            self.metrics.pause_streaming()

            self.log.info("Streaming paused", session_id=self._session_id)

            return {
                "success": True,
                "message": "Streaming paused successfully",
                "paused_at": datetime.now(UTC).isoformat(),
                "events_sent_before_pause": self._statistics.events_sent_successfully,
            }

    async def resume(self) -> dict:
        """
        Resume streaming after pause.

        Continues from where it left off with no event loss.

        Returns:
            {
                "success": bool,
                "message": str,
                "resumed_at": str (ISO timestamp),
                "pause_duration_seconds": float,
                "total_pause_count": int
            }
        """
        async with self._pause_lock:
            if not self._is_streaming:
                return {
                    "success": False,
                    "message": "Cannot resume: streaming is not active",
                    "resumed_at": None,
                    "pause_duration_seconds": 0,
                }

            if not self._is_paused:
                return {
                    "success": False,
                    "message": "Not paused",
                    "resumed_at": None,
                    "pause_duration_seconds": 0,
                }

            # Calculate pause duration
            pause_duration = 0.0
            if self._last_pause_time:
                pause_duration = time.time() - self._last_pause_time
                self._total_pause_duration += pause_duration

            # Resume streaming
            self._is_paused = False
            self._pause_event.set()  # Unblock streaming loop

            # Record resume in metrics
            self.metrics.resume_streaming()

            self.log.info(
                "Streaming resumed",
                session_id=self._session_id,
                pause_duration_seconds=pause_duration,
            )

            return {
                "success": True,
                "message": "Streaming resumed successfully",
                "resumed_at": datetime.now(UTC).isoformat(),
                "pause_duration_seconds": pause_duration,
                "total_pause_count": self._pause_count,
            }

    def get_pause_statistics(self) -> dict:
        """Get pause/resume statistics."""
        return {
            "is_paused": self._is_paused,
            "pause_count": self._pause_count,
            "total_pause_duration_seconds": self._total_pause_duration,
            "currently_paused_duration": (
                time.time() - self._last_pause_time
                if self._is_paused and self._last_pause_time
                else 0.0
            ),
        }

    async def _handle_send_failure(
        self, events: list[EventEnvelope], exception: Exception
    ):
        """Handle send failure with error classification."""
        # Classify error
        error = classify_error(exception)

        self.log.error(
            "Event send failed",
            session_id=self._session_id,
            error_message=error.message,
            severity=error.severity.value,
            category=error.category.value,
            retryable=error.retryable,
            event_count=len(events),
        )

        # Update statistics
        async with self._stats_lock:
            self._statistics.events_failed += len(events)

        # Add to DLQ if enabled
        if self._dlq_enabled:
            async with self._dlq_lock:
                for event in events:
                    dlq_entry = DLQEntry(
                        event=event,
                        error_message=error.message,
                        error_category=error.category.value,
                        error_severity=error.severity.value,
                        timestamp=datetime.now(UTC).isoformat(),
                        retry_count=0,
                    )

                    self._dlq.append(dlq_entry)

                # Trim DLQ if too large
                if len(self._dlq) > self._dlq_max_size:
                    removed = len(self._dlq) - self._dlq_max_size
                    self._dlq = self._dlq[-self._dlq_max_size :]
                    self.log.warning(
                        "DLQ size exceeded",
                        session_id=self._session_id,
                        removed=removed,
                        dlq_size=len(self._dlq),
                        max_size=self._dlq_max_size,
                    )

        # Stop streaming if critical error
        if error.severity == ErrorSeverity.CRITICAL:
            self.log.critical(
                "Critical error detected, stopping streaming",
                session_id=self._session_id,
            )
            await self.stop()

    async def retry_dlq_events(self, max_retries: int | None = None) -> dict:
        """
        Retry events from DLQ.

        Args:
            max_retries: Maximum retry attempts per event (default from config)

        Returns:
            {
                "total_attempted": int,
                "succeeded": int,
                "failed": int,
                "still_in_dlq": int
            }
        """
        max_retries = max_retries or self._dlq_retry_max_attempts

        async with self._dlq_lock:
            if not self._dlq:
                return {
                    "total_attempted": 0,
                    "succeeded": 0,
                    "failed": 0,
                    "still_in_dlq": 0,
                }

            total = len(self._dlq)
            succeeded = 0
            failed = 0

            # Copy DLQ for retry
            retry_queue = self._dlq.copy()
            self._dlq = []

            for entry in retry_queue:
                # Skip if max retries exceeded
                if entry.retry_count >= max_retries:
                    self._dlq.append(entry)
                    failed += 1
                    continue

                # Retry send
                try:
                    result = await self._azure_client.send_events([entry.event])

                    if result:
                        succeeded += 1
                        self.log.info(
                            "DLQ event retry succeeded",
                            session_id=self._session_id,
                            trace_id=entry.event.trace_id,
                        )
                    else:
                        # Update retry count and re-add to DLQ
                        entry.retry_count += 1
                        entry.last_retry_timestamp = datetime.now(UTC).isoformat()
                        self._dlq.append(entry)
                        failed += 1

                except Exception as e:
                    # Update retry count and re-add to DLQ
                    entry.retry_count += 1
                    entry.last_retry_timestamp = datetime.now(UTC).isoformat()
                    entry.error_message = (
                        f"{entry.error_message} | Retry failed: {str(e)}"
                    )
                    self._dlq.append(entry)
                    failed += 1

            return {
                "total_attempted": total,
                "succeeded": succeeded,
                "failed": failed,
                "still_in_dlq": len(self._dlq),
            }

    def get_dlq_summary(self) -> dict:
        """Get DLQ summary statistics."""
        if not self._dlq:
            return {
                "size": 0,
                "by_category": {},
                "by_severity": {},
                "oldest_entry": None,
                "newest_entry": None,
                "max_size": self._dlq_max_size,
            }

        from collections import Counter

        categories = Counter(entry.error_category for entry in self._dlq)
        severities = Counter(entry.error_severity for entry in self._dlq)

        return {
            "size": len(self._dlq),
            "by_category": dict(categories),
            "by_severity": dict(severities),
            "oldest_entry": self._dlq[0].timestamp if self._dlq else None,
            "newest_entry": self._dlq[-1].timestamp if self._dlq else None,
            "max_size": self._dlq_max_size,
        }

    async def _dlq_retry_loop(self):
        """Background loop to retry DLQ events periodically."""
        retry_interval = 300  # 5 minutes

        while self._is_streaming and not self._is_shutdown:
            await asyncio.sleep(retry_interval)

            if self._dlq_retry_enabled and self._dlq:
                self.log.info(
                    "Auto-retrying DLQ events",
                    session_id=self._session_id,
                    dlq_size=len(self._dlq),
                )
                result = await self.retry_dlq_events()
                self.log.info(
                    "DLQ retry complete",
                    session_id=self._session_id,
                    succeeded=result["succeeded"],
                    failed=result["failed"],
                )

    # Legacy SQLite database integration methods (deprecated)

    async def _load_unpublished_events_from_db(
        self,
        table_name: str,
        start_ts: datetime,
        end_ts: datetime,
        batch_size: int = 1000,
    ) -> list[dict]:
        """
        Load unpublished events from legacy SQLite facts.db (deprecated).

        Args:
            table_name: Fact table name (e.g., "fact_receipts")
            start_ts: Start timestamp (from watermark)
            end_ts: End timestamp (current time or batch end)
            batch_size: Maximum events to return

        Returns:
            List of event records as dicts
        """
        if not self._session:
            raise ValueError("No database session provided - cannot read from legacy SQLite")

        # Import here to avoid circular dependencies
        from ..db.models.facts import (
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

        result = await self._session.execute(query)
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

    async def _get_streaming_window_from_watermarks(self) -> tuple[datetime, datetime]:
        """Get time window of unpublished data from watermarks."""
        from ..db.purge import get_unpublished_data_range

        # Get earliest unpublished across all tables
        earliest = None
        latest = None

        for table in self._get_fact_tables():
            start, end = await get_unpublished_data_range(
                self._session, f"fact_{table}"
            )
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
        import random

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

    # Public API methods

    async def get_statistics(self) -> dict[str, Any]:
        """
        Get current streaming statistics.

        Returns:
            dict: Current statistics and performance metrics
        """
        async with self._stats_lock:
            stats = {
                "events_generated": self._statistics.events_generated,
                "events_sent_successfully": self._statistics.events_sent_successfully,
                "events_failed": self._statistics.events_failed,
                "batches_sent": self._statistics.batches_sent,
                "events_per_second": self._statistics.events_per_second,
                "bytes_sent": self._statistics.bytes_sent,
                "last_event_time": (
                    self._statistics.last_event_time.isoformat()
                    if self._statistics.last_event_time
                    else None
                ),
                "event_type_counts": dict(self._statistics.event_type_counts),
                "error_counts": dict(self._statistics.error_counts),
                "connection_failures": self._statistics.connection_failures,
                "circuit_breaker_trips": self._statistics.circuit_breaker_trips,
                "buffer_size": len(self._event_buffer),
                "dead_letter_queue_size": len(self._dlq),
                "is_streaming": self._is_streaming,
                "pause_statistics": self.get_pause_statistics(),
                "dlq_summary": self.get_dlq_summary(),
            }

            # Add Azure client statistics if available
            if self._azure_client:
                azure_stats = self._azure_client.get_statistics()
                stats.update({"azure_client": azure_stats})

            return stats

    async def get_health_status(self) -> dict[str, Any]:
        """
        Get comprehensive health status of the streaming system.

        Returns:
            dict: Health status information
        """
        health = {
            "overall_healthy": True,
            "streaming_active": self._is_streaming,
            "components": {},
            "last_updated": datetime.now(UTC).isoformat(),
        }

        # Check Azure client health
        if self._azure_client:
            azure_health = await self._azure_client.health_check()
            health["components"]["azure_event_hub"] = azure_health
            if not azure_health.get("healthy", False):
                health["overall_healthy"] = False

        # Check event factory health
        health["components"]["event_factory"] = {
            "healthy": self._event_factory is not None,
            "master_data_loaded": all(
                [
                    self._stores,
                    self._customers,
                    self._products,
                    self._distribution_centers,
                ]
            ),
        }

        # Check buffer health
        buffer_healthy = len(self._event_buffer) < self.streaming_config.max_buffer_size
        health["components"]["event_buffer"] = {
            "healthy": buffer_healthy,
            "size": len(self._event_buffer),
            "max_size": self.streaming_config.max_buffer_size,
        }
        if not buffer_healthy:
            health["overall_healthy"] = False

        # Check error rates
        stats = await self.get_statistics()
        total_events = stats["events_generated"]
        failed_events = stats["events_failed"]

        if total_events > 0:
            error_rate = failed_events / total_events
            error_healthy = error_rate < 0.05  # Less than 5% error rate
            health["components"]["error_rate"] = {
                "healthy": error_healthy,
                "rate": error_rate,
                "threshold": 0.05,
            }
            if not error_healthy:
                health["overall_healthy"] = False

        return health

    def add_event_generated_hook(self, hook: Callable[[EventEnvelope], None]):
        """Add hook called when events are generated."""
        self._event_generated_hooks.append(hook)

    def add_event_sent_hook(self, hook: Callable[[EventEnvelope], None]):
        """Add hook called when events are successfully sent."""
        self._event_sent_hooks.append(hook)

    def add_batch_sent_hook(self, hook: Callable[[list[EventEnvelope]], None]):
        """Add hook called when event batches are successfully sent."""
        self._batch_sent_hooks.append(hook)

    def add_error_hook(self, hook: Callable[[Exception, str], None]):
        """Add hook called when errors occur."""
        self._error_hooks.append(hook)

    @asynccontextmanager
    async def streaming_session(self, duration: timedelta | None = None):
        """
        Async context manager for streaming sessions.

        Usage:
            async with streamer.streaming_session(duration=timedelta(minutes=5)):
                # Streaming runs in background
                await some_other_work()
        """
        streaming_task = asyncio.create_task(self.start(duration))
        try:
            # Allow some time for streaming to start
            await asyncio.sleep(1)
            yield self
        finally:
            await self.stop()
            try:
                await streaming_task
            except asyncio.CancelledError:
                pass
