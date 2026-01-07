"""
Main EventStreamer orchestration class.

This module provides the central EventStreamer class that coordinates
all streaming components and provides the public API.
"""

import asyncio
import os
import signal
import threading
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from ...config.models import RetailConfig
from ...shared.logging_utils import get_structured_logger
from ...shared.metrics import metrics_collector
from ...shared.models import Customer, DistributionCenter, ProductMaster, Store
from ..azure_client import AzureEventHubClient
from ..event_factory import EventFactory
from .batch_streaming import BatchStreamingManager
from .config import StreamingConfig, StreamingStatistics
from .core import StreamingCore
from .dlq import DLQManager
from .monitoring import MonitoringManager


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
        if os.getenv("RETAIL_DATAGEN_TEST_MODE", "").strip().lower() in {
            "1",
            "true",
            "yes",
        }:
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

        # Session attribute (deprecated - kept for backward compatibility)
        self._session = None

        # Structured logger with session tracking
        self.log = get_structured_logger(__name__)
        self._session_id = self.log.generate_correlation_id()
        self.log.set_correlation_id(self._session_id)

        # Metrics collector
        self.metrics = metrics_collector

        # Initialize component managers
        self._dlq_manager = DLQManager(
            max_size=self.streaming_config.dlq_max_size,
            retry_enabled=self.streaming_config.dlq_retry_enabled,
            retry_max_attempts=self.streaming_config.dlq_retry_max_attempts,
            log=self.log,
            session_id=self._session_id,
        )

        self._monitoring_manager = MonitoringManager(
            log=self.log,
            session_id=self._session_id,
            streaming_config=self.streaming_config,
            statistics=self._statistics,
            metrics=self.metrics,
            azure_client=self._azure_client,
        )

        self._batch_streaming_manager = BatchStreamingManager(
            log=self.log,
            session_id=self._session_id,
            streaming_config=self.streaming_config,
        )

        # Initialize streaming core with placeholder values (will be updated in initialize())
        self._streaming_core = StreamingCore(
            log=self.log,
            session_id=self._session_id,
            streaming_config=self.streaming_config,
            event_factory=None,  # Will be set in initialize()
            azure_client=None,  # Will be set in initialize()
            statistics=self._statistics,
            metrics=self.metrics,
        )

        # Setup signal handling for graceful shutdown
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        if threading.current_thread() is threading.main_thread():
            # Store original handlers to restore later
            self._original_sigint = signal.getsignal(signal.SIGINT)
            self._original_sigterm = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)

    def _restore_signal_handlers(self):
        """Restore original signal handlers."""
        if threading.current_thread() is threading.main_thread():
            if hasattr(self, "_original_sigint") and self._original_sigint:
                signal.signal(signal.SIGINT, self._original_sigint)
            if hasattr(self, "_original_sigterm") and self._original_sigterm:
                signal.signal(signal.SIGTERM, self._original_sigterm)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.log.info(
            "Received shutdown signal",
            session_id=self._session_id,
            signal=signum,
        )
        # Set shutdown flag - the streaming loop will check this and exit
        self._is_shutdown = True

        # Try to schedule stop() on the event loop if one is running
        try:
            loop = asyncio.get_running_loop()
            loop.call_soon_threadsafe(lambda: asyncio.create_task(self.stop()))
        except RuntimeError:
            # No running event loop - just set the flag, which we already did
            pass

        # Restore original handlers so a second Ctrl+C will force exit
        self._restore_signal_handlers()

        # Re-raise signal with original handler for proper exit
        if signum == signal.SIGINT and hasattr(self, "_original_sigint"):
            if callable(self._original_sigint) and self._original_sigint not in (
                signal.SIG_IGN,
                signal.SIG_DFL,
            ):
                self._original_sigint(signum, frame)
            elif self._original_sigint == signal.SIG_DFL:
                # Default behavior - raise KeyboardInterrupt
                raise KeyboardInterrupt

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
                    circuit_breaker_failure_threshold=self.streaming_config.circuit_breaker_failure_threshold,
                    circuit_breaker_recovery_timeout=self.streaming_config.circuit_breaker_recovery_timeout,
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

            # Update streaming core with initialized components
            self._streaming_core._event_factory = self._event_factory
            self._streaming_core._azure_client = self._azure_client

            # Compute daily targets for pacing after data load
            stores_count = len(self._stores) if self._stores else 1
            dcs_count = (
                len(self._distribution_centers) if self._distribution_centers else 1
            )
            self._streaming_core.compute_daily_targets(
                stores_count, dcs_count, self.config.volume
            )
            # Store config volume for daily resets
            self._streaming_core._config_volume = self.config.volume
            self._streaming_core._stores = self._stores
            self._streaming_core._distribution_centers = self._distribution_centers

            # Update monitoring manager with azure client
            self._monitoring_manager._azure_client = self._azure_client

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
        if all(
            [self._stores, self._customers, self._products, self._distribution_centers]
        ):
            return
        self.log.info(
            "Loading master data for streaming from DuckDB", session_id=self._session_id
        )
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
            self._load_fallback_master_data()

    def _load_fallback_master_data(self):
        """Load minimal fallback master data for testing."""
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
                DistributionCenter(
                    ID=1, DCNumber="DC001", Address="999 Industrial Blvd", GeographyID=1
                ),
                DistributionCenter(
                    ID=2, DCNumber="DC002", Address="888 Warehouse Way", GeographyID=2
                ),
            ]

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
            self._monitoring_task = asyncio.create_task(
                self._monitoring_manager.monitoring_loop(
                    dlq_size_func=self._dlq_manager.get_size,
                    is_shutdown_func=lambda: self._is_shutdown,
                )
            )

            # Start DLQ retry task if enabled
            if self.streaming_config.dlq_retry_enabled:
                self._dlq_retry_task = asyncio.create_task(
                    self._dlq_manager.retry_loop(
                        is_streaming_func=lambda: self._is_streaming,
                        is_shutdown_func=lambda: self._is_shutdown,
                        azure_client=self._azure_client,
                    )
                )

            # Start main streaming task
            self._streaming_task = asyncio.create_task(
                self._streaming_core.streaming_loop(
                    start_time,
                    end_time,
                    pause_event=self._monitoring_manager.get_pause_event(),
                    is_shutdown_func=lambda: self._is_shutdown,
                )
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

        Returns:
            bool: True if streaming completed successfully, False otherwise
        """
        return await self._batch_streaming_manager.start_batch_streaming_sqlite(
            self._session, self.streaming_config.azure_connection_string
        )

    async def start_batch_streaming_duckdb(self) -> bool:
        """
        Start batch streaming from DuckDB database.

        Returns:
            bool: True if streaming completed successfully, False otherwise
        """
        return await self._batch_streaming_manager.start_batch_streaming_duckdb(
            self._duckdb_conn, self.streaming_config.azure_connection_string
        )

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
            if self._streaming_core:
                await self._streaming_core.flush_remaining_events(self._dlq_manager)

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

    async def _cleanup(self):
        """Clean up resources."""
        if self._azure_client:
            await self._azure_client.disconnect()

        self._is_streaming = False

        # Restore original signal handlers
        self._restore_signal_handlers()

    # Delegate methods to component managers

    async def pause(self) -> dict:
        """Pause streaming without stopping completely."""
        return await self._monitoring_manager.pause(lambda: self._is_streaming)

    async def resume(self) -> dict:
        """Resume streaming after pause."""
        return await self._monitoring_manager.resume(lambda: self._is_streaming)

    def get_pause_statistics(self) -> dict:
        """Get pause/resume statistics."""
        return self._monitoring_manager.get_pause_statistics()

    async def retry_dlq_events(self, max_retries: int | None = None) -> dict:
        """Retry events from DLQ."""
        return await self._dlq_manager.retry_events(self._azure_client, max_retries)

    def get_dlq_summary(self) -> dict:
        """Get DLQ summary statistics."""
        return self._dlq_manager.get_summary()

    async def get_statistics(self) -> dict:
        """Get current streaming statistics."""
        buffer_size = (
            self._streaming_core.get_buffer_size() if self._streaming_core else 0
        )
        dlq_size = self._dlq_manager.get_size()
        stats = await self._monitoring_manager.get_statistics(
            buffer_size, dlq_size, self._azure_client
        )
        stats["is_streaming"] = self._is_streaming
        stats["dlq_summary"] = self.get_dlq_summary()
        return stats

    async def get_health_status(self) -> dict:
        """Get comprehensive health status of the streaming system."""
        master_data_loaded = all(
            [
                self._stores,
                self._customers,
                self._products,
                self._distribution_centers,
            ]
        )
        buffer_size = (
            self._streaming_core.get_buffer_size() if self._streaming_core else 0
        )
        return await self._monitoring_manager.get_health_status(
            is_streaming=self._is_streaming,
            azure_client=self._azure_client,
            event_factory=self._event_factory,
            master_data_loaded=master_data_loaded,
            buffer_size=buffer_size,
        )

    def set_allowed_event_types(self, event_type_names: list[str] | None):
        """Optionally restrict emitted events to a subset of EventType names."""
        if self._streaming_core:
            self._streaming_core.set_allowed_event_types(event_type_names)

    def add_event_generated_hook(self, hook):
        """Add hook called when events are generated."""
        if self._streaming_core:
            self._streaming_core.add_event_generated_hook(hook)

    def add_event_sent_hook(self, hook):
        """Add hook called when events are successfully sent."""
        if self._streaming_core:
            self._streaming_core.add_event_sent_hook(hook)

    def add_batch_sent_hook(self, hook):
        """Add hook called when event batches are successfully sent."""
        if self._streaming_core:
            self._streaming_core.add_batch_sent_hook(hook)

    def add_error_hook(self, hook):
        """Add hook called when errors occur."""
        if self._streaming_core:
            self._streaming_core.add_error_hook(hook)

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

    # Backward compatibility properties for tests
    @property
    def _event_buffer(self):
        """Backward compatibility: access event buffer."""
        return self._streaming_core._event_buffer if self._streaming_core else []

    @_event_buffer.setter
    def _event_buffer(self, value):
        """Backward compatibility: set event buffer."""
        if self._streaming_core:
            self._streaming_core._event_buffer = value

    @property
    def _dlq(self):
        """Backward compatibility: access DLQ."""
        return self._dlq_manager._dlq

    @property
    def _buffer_lock(self):
        """Backward compatibility: access buffer lock."""
        return self._streaming_core._buffer_lock if self._streaming_core else None

    @property
    def _stats_lock(self):
        """Backward compatibility: access stats lock."""
        return (
            self._monitoring_manager._stats_lock if self._monitoring_manager else None
        )

    @property
    def _allowed_event_types(self):
        """Backward compatibility: access allowed event types."""
        return (
            self._streaming_core._allowed_event_types if self._streaming_core else None
        )

    @property
    def _pause_event(self):
        """Backward compatibility: access pause event."""
        return (
            self._monitoring_manager.get_pause_event()
            if self._monitoring_manager
            else None
        )

    @property
    def _is_paused(self):
        """Backward compatibility: check if paused."""
        return (
            self._monitoring_manager.is_paused() if self._monitoring_manager else False
        )

    async def _generate_event_burst(self, timestamp):
        """Backward compatibility: delegate to streaming core."""
        if self._streaming_core:
            return await self._streaming_core._generate_event_burst(timestamp)
        return []

    async def _flush_event_buffer(self):
        """Backward compatibility: delegate to streaming core."""
        if self._streaming_core:
            await self._streaming_core.flush_event_buffer(self._dlq_manager)
