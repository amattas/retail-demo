"""
Azure Event Hub client wrapper for real-time streaming.

This module provides a wrapper around the Azure Event Hub producer client
with proper error handling, retry logic, and batch management.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

try:
    from azure.core.exceptions import AzureError
    from azure.eventhub import EventData, EventHubProducerClient
    from azure.eventhub.exceptions import EventHubError

    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

    # Mock classes for development without Azure SDK
    class EventData:
        def __init__(self, body: str):
            self.body = body

    class EventHubProducerClient:
        def __init__(self, *args, **kwargs):
            pass

    class EventHubError(Exception):
        pass

    class AzureError(Exception):
        pass


from ..shared.credential_utils import sanitize_connection_string
from ..shared.logging_utils import get_structured_logger
from ..shared.metrics import metrics_collector
from .schemas import EventEnvelope

logger = logging.getLogger(__name__)


class CircuitBreaker:
    """Simple circuit breaker implementation for Azure Event Hub failures."""

    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.metrics = metrics_collector

        # Initialize metric
        self.metrics.update_circuit_breaker_state(self.state)

    def call(self, func):
        """Execute function with circuit breaker protection."""
        if self.state == "OPEN":
            if self._should_attempt_reset():
                self.state = "HALF_OPEN"
                self.metrics.update_circuit_breaker_state(self.state)
            else:
                raise EventHubError("Circuit breaker is OPEN")

        try:
            result = func()
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e

    async def call_async(self, func):
        """Execute coroutine function with circuit breaker protection."""
        if self.state == "OPEN":
            if self._should_attempt_reset():
                self.state = "HALF_OPEN"
                self.metrics.update_circuit_breaker_state(self.state)
            else:
                raise EventHubError("Circuit breaker is OPEN")

        try:
            result = await func()
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True

        time_since_failure = (
            datetime.now(UTC) - self.last_failure_time
        ).total_seconds()
        return time_since_failure >= self.recovery_timeout

    def _on_success(self):
        """Reset circuit breaker on successful operation."""
        self.failure_count = 0
        if self.state != "CLOSED":
            self.state = "CLOSED"
            self.metrics.update_circuit_breaker_state(self.state)

    def _on_failure(self):
        """Handle failure and potentially open circuit."""
        self.failure_count += 1
        self.last_failure_time = datetime.now(UTC)

        # Record failure metric
        self.metrics.record_circuit_breaker_failure()

        if self.failure_count >= self.failure_threshold:
            if self.state != "OPEN":
                self.state = "OPEN"
                self.metrics.update_circuit_breaker_state(self.state)
                self.metrics.record_circuit_breaker_trip()


class AzureEventHubClient:
    """
    Azure Event Hub producer client wrapper with error handling and batching.

    Provides robust Event Hub integration with:
    - Automatic retry with exponential backoff
    - Circuit breaker pattern for failure handling
    - Optimized batch processing
    - Connection pooling and resource management
    - Comprehensive error handling and logging
    """

    def __init__(
        self,
        connection_string: str,
        hub_name: str = "",
        max_batch_size: int = 256,
        batch_timeout_ms: int = 1000,
        retry_attempts: int = 3,
        backoff_multiplier: float = 2.0,
        circuit_breaker_enabled: bool = True,
    ):
        """
        Initialize Azure Event Hub client.

        Args:
            connection_string: Azure Event Hub connection string
            hub_name: Name of the Event Hub (can be empty if EntityPath is in connection string)
            max_batch_size: Maximum events per batch
            batch_timeout_ms: Maximum time to wait for batch completion
            retry_attempts: Number of retry attempts on failures
            backoff_multiplier: Multiplier for exponential backoff
            circuit_breaker_enabled: Whether to use circuit breaker pattern
        """
        if not AZURE_AVAILABLE:
            logger.warning("Azure Event Hub SDK not available - using mock client")

        self.connection_string = connection_string
        self.hub_name = hub_name
        self.max_batch_size = max_batch_size
        self.batch_timeout_ms = batch_timeout_ms
        self.retry_attempts = retry_attempts
        self.backoff_multiplier = backoff_multiplier

        self._client: EventHubProducerClient | None = None
        self._is_connected = False
        self._event_buffer: list[EventEnvelope] = []
        self._statistics = {
            "events_sent": 0,
            "events_failed": 0,
            "batches_sent": 0,
            "connection_failures": 0,
            "last_send_time": None,
        }

        # Circuit breaker for failure handling
        self.circuit_breaker = CircuitBreaker() if circuit_breaker_enabled else None

        # Structured logger
        self.log = get_structured_logger(__name__)
        self.log.info(
            "Initializing Azure Event Hub client",
            hub_name=hub_name or "from_connection_string",
            max_batch_size=max_batch_size,
        )

        # Determine mock mode: either Azure SDK missing or explicit mock scheme
        self._is_mock = (not AZURE_AVAILABLE) or (
            isinstance(connection_string, str) and connection_string.startswith("mock://")
        )

        # Initialize client only for real (non-mock) connections
        if (not self._is_mock) and connection_string and hub_name:
            self._initialize_client()

    def _initialize_client(self):
        """Initialize the Azure Event Hub producer client."""
        try:
            self._client = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string, eventhub_name=self.hub_name
            )
            # Log sanitized connection string to avoid exposing keys
            sanitized = sanitize_connection_string(self.connection_string)
            logger.info(
                f"Azure Event Hub client initialized for hub: {self.hub_name} "
                f"(connection configured: {sanitized})"
            )
        except Exception as e:
            logger.error(f"Failed to initialize Event Hub client: {e}")
            self._statistics["connection_failures"] += 1
            raise

    async def connect(self) -> bool:
        """
        Establish connection to Azure Event Hub.

        Returns:
            bool: True if connection successful, False otherwise
        """
        if self._is_mock:
            logger.info("Mock Azure client - connection simulated")
            self._is_connected = True
            metrics_collector.update_connection_status(True)
            return True

        try:
            if not self._client:
                self._initialize_client()

            # Test connection by getting partition information
            partition_props = await self._client.get_partition_properties("0")
            if partition_props:
                self._is_connected = True
                metrics_collector.update_connection_status(True)
                logger.info("Successfully connected to Azure Event Hub")
                return True
        except Exception as e:
            logger.error(f"Failed to connect to Azure Event Hub: {e}")
            self._statistics["connection_failures"] += 1
            self._is_connected = False
            metrics_collector.update_connection_status(False)
            metrics_collector.record_connection_failure()

        return False

    async def disconnect(self):
        """Close connection to Azure Event Hub."""
        if self._client and AZURE_AVAILABLE:
            try:
                await self._client.close()
                logger.info("Azure Event Hub client disconnected")
            except Exception as e:
                logger.error(f"Error during disconnect: {e}")

        self._is_connected = False
        metrics_collector.update_connection_status(False)

    def is_connected(self) -> bool:
        """Check if client is connected to Event Hub."""
        return self._is_connected

    async def send_event(self, event: EventEnvelope) -> bool:
        """
        Send a single event to Event Hub.

        Args:
            event: Event envelope to send

        Returns:
            bool: True if sent successfully, False otherwise
        """
        return await self.send_events([event])

    async def send_events(self, events: list[EventEnvelope]) -> bool:
        """
        Send multiple events to Event Hub with batching and retry logic.

        Args:
            events: List of event envelopes to send

        Returns:
            bool: True if all events sent successfully, False otherwise
        """
        if not events:
            return True

        if not self._is_connected:
            self.log.error("Client not connected to Event Hub", event_count=len(events))
            return False

        batch_id = events[0].correlation_id if events else "unknown"

        self.log.debug(
            "Sending event batch",
            batch_id=batch_id,
            event_count=len(events),
            batch_size_kb=self._estimate_batch_size(events),
        )

        # Process events in batches
        success = True
        for i in range(0, len(events), self.max_batch_size):
            batch = events[i : i + self.max_batch_size]
            batch_success = await self._send_batch(batch)
            if not batch_success:
                success = False

        if success:
            self.log.info(
                "Batch sent successfully",
                batch_id=batch_id,
                event_count=len(events),
                total_sent=self._statistics["events_sent"],
            )
        else:
            self.log.error(
                "Batch send failed",
                batch_id=batch_id,
                event_count=len(events),
                total_failed=self._statistics["events_failed"],
            )

        return success

    def _estimate_batch_size(self, events: list[EventEnvelope]) -> float:
        """Estimate batch size in KB."""
        import sys

        total_size = sum(sys.getsizeof(e.model_dump_json()) for e in events)
        return total_size / 1024

    async def _send_batch(self, events: list[EventEnvelope]) -> bool:
        """
        Send a batch of events with retry logic.

        Args:
            events: Batch of events to send

        Returns:
            bool: True if batch sent successfully, False otherwise
        """
        attempt = 0
        while attempt < self.retry_attempts:
            try:
                if self.circuit_breaker:
                    success = await self.circuit_breaker.call_async(
                        lambda: self._send_batch_direct(events)
                    )
                else:
                    success = await self._send_batch_direct(events)

                if success:
                    self._statistics["events_sent"] += len(events)
                    self._statistics["batches_sent"] += 1
                    self._statistics["last_send_time"] = datetime.now(UTC)
                    logger.debug(f"Successfully sent batch of {len(events)} events")
                    return True

            except Exception as e:
                attempt += 1
                self._statistics["events_failed"] += len(events)
                logger.warning(
                    f"Batch send attempt {attempt}/{self.retry_attempts} failed: {e}"
                )

                if attempt < self.retry_attempts:
                    # Exponential backoff
                    wait_time = self.backoff_multiplier ** (attempt - 1)
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"Failed to send batch after {self.retry_attempts} attempts"
                    )

        return False

    async def _send_batch_direct(self, events: list[EventEnvelope]) -> bool:
        """
        Direct batch send to Event Hub without retry logic.

        Args:
            events: Events to send

        Returns:
            bool: True if successful, False otherwise
        """
        if not AZURE_AVAILABLE:
            # Mock implementation for testing
            logger.debug(f"Mock send: {len(events)} events")
            return True

        try:
            # Convert events to Event Hub format
            event_data_batch = []
            for event in events:
                event_json = event.model_dump_json()
                event_data = EventData(event_json)

                # Set partition key if provided
                if event.partition_key:
                    event_data.partition_key = event.partition_key

                event_data_batch.append(event_data)

            # Send batch to Event Hub
            async with self._client:
                await self._client.send_batch(event_data_batch)

            return True

        except EventHubError as e:
            logger.error(f"Event Hub specific error: {e}")
            raise
        except AzureError as e:
            logger.error(f"Azure service error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error sending batch: {e}")
            raise

    def add_to_buffer(self, event: EventEnvelope) -> bool:
        """
        Add event to internal buffer for batching.

        Args:
            event: Event to buffer

        Returns:
            bool: True if buffer needs flushing (reached max size), False otherwise
        """
        self._event_buffer.append(event)

        # Check if buffer needs flushing
        needs_flush = len(self._event_buffer) >= self.max_batch_size

        # Auto-flush if buffer reaches max size and we're in an async context
        if needs_flush:
            try:
                loop = asyncio.get_running_loop()
                loop.call_soon(lambda: asyncio.create_task(self.flush_buffer()))
            except RuntimeError:
                # No running event loop - caller should handle flush manually
                pass

        return needs_flush

    async def flush_buffer(self) -> bool:
        """
        Flush all buffered events to Event Hub.

        Returns:
            bool: True if all events sent successfully, False otherwise
        """
        if not self._event_buffer:
            return True

        events_to_send = self._event_buffer.copy()
        self._event_buffer.clear()

        return await self.send_events(events_to_send)

    def get_statistics(self) -> dict[str, Any]:
        """
        Get client statistics and performance metrics.

        Returns:
            dict: Statistics about client performance
        """
        stats = self._statistics.copy()
        stats.update(
            {
                "is_connected": self._is_connected,
                "buffer_size": len(self._event_buffer),
                "circuit_breaker_state": (
                    self.circuit_breaker.state if self.circuit_breaker else "DISABLED"
                ),
                "hub_name": self.hub_name,
            }
        )
        return stats

    async def health_check(self) -> dict[str, Any]:
        """
        Perform health check of the Event Hub connection.

        Returns:
            dict: Health status and diagnostic information
        """
        health_status = {
            "healthy": False,
            "connection_status": "disconnected",
            "last_send_time": self._statistics["last_send_time"],
            "total_events_sent": self._statistics["events_sent"],
            "total_failures": self._statistics["events_failed"],
            "circuit_breaker_state": (
                self.circuit_breaker.state if self.circuit_breaker else "DISABLED"
            ),
        }

        if self._is_connected:
            try:
                if AZURE_AVAILABLE and self._client:
                    # Test connection with a simple operation
                    hub_props = await self._client.get_eventhub_properties()
                    if hub_props:
                        health_status["healthy"] = True
                        health_status["connection_status"] = "connected"
                        health_status["partition_count"] = len(hub_props.partition_ids)
                else:
                    # Mock client is always healthy when connected
                    health_status["healthy"] = True
                    health_status["connection_status"] = "connected (mock)"

            except Exception as e:
                logger.error(f"Health check failed: {e}")
                health_status["connection_status"] = f"unhealthy: {str(e)}"

        return health_status

    @asynccontextmanager
    async def managed_connection(self):
        """
        Async context manager for automatic connection management.

        Usage:
            async with client.managed_connection():
                await client.send_event(event)
        """
        try:
            await self.connect()
            yield self
        finally:
            await self.disconnect()

    def _parse_connection_string(self, conn_str: str) -> dict:
        """
        Parse Event Hub connection string to extract metadata.

        Args:
            conn_str: Connection string to parse

        Returns:
            dict: Metadata extracted from connection string with keys:
                - endpoint: Event Hub namespace endpoint (e.g., 'sb://xxx.servicebus.windows.net')
                - entity_path: Event Hub name (if present in connection string)
                - namespace: Namespace name extracted from endpoint
                - key_name: SharedAccessKeyName value
                - is_fabric_rti: Boolean indicating if this is a Fabric RTI connection
        """
        metadata = {}

        try:
            parts = conn_str.split(";")
            for part in parts:
                if "=" in part:
                    key, value = part.split("=", 1)
                    if key == "Endpoint":
                        metadata["endpoint"] = value
                        # Extract namespace from endpoint
                        if "sb://" in value:
                            namespace = value.split("sb://")[1].split(".")[0]
                            metadata["namespace"] = namespace
                    elif key == "EntityPath":
                        metadata["entity_path"] = value
                    elif key == "SharedAccessKeyName":
                        metadata["key_name"] = value

            # Detect Fabric RTI (namespace starts with "eventstream-")
            namespace = metadata.get("namespace", "")
            if namespace.startswith("eventstream-"):
                metadata["is_fabric_rti"] = True
            else:
                metadata["is_fabric_rti"] = False

        except Exception as e:
            logger.warning(f"Error parsing connection string: {e}")

        return metadata

    async def test_connection(self) -> tuple[bool, str, dict]:
        """
        Test connection to Event Hub without sending events.

        This method validates the connection by creating a producer client
        and fetching Event Hub properties, which proves the connection works
        without actually sending any data.

        Returns:
            tuple: (success, message, metadata)
                - success: True if connection test passed
                - message: Descriptive message about the test result
                - metadata: Dictionary with connection details (endpoint, partition info, etc.)

        Example:
            success, msg, metadata = await client.test_connection()
            if success:
                print(f"Connected! Hub: {metadata['entity_path']}")
        """
        if not AZURE_AVAILABLE:
            # Return mock success for development without Azure SDK
            return await self._test_connection_mock()

        try:
            # Parse connection string to extract metadata
            metadata = self._parse_connection_string(self.connection_string)

            # Determine hub name (from constructor or EntityPath in connection string)
            hub_name = self.hub_name or metadata.get("entity_path")
            if not hub_name:
                return (
                    False,
                    "Hub name not specified and EntityPath not found in connection string",
                    {},
                )

            # Create a temporary producer client with short timeout
            logger.info(f"Testing connection to Event Hub: {hub_name}")

            async with EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=hub_name,
                logging_enable=False,
            ) as producer:
                # Get Event Hub metadata (proves connection works)
                properties = await producer.get_eventhub_properties()

                # Add partition information to metadata
                metadata.update(
                    {
                        "hub_name": hub_name,
                        "partition_count": len(properties.partition_ids),
                        "partition_ids": list(properties.partition_ids),
                        "created_at": (
                            properties.created_at.isoformat()
                            if properties.created_at
                            else None
                        ),
                    }
                )

                logger.info(
                    f"Successfully connected to Event Hub '{hub_name}' with {len(properties.partition_ids)} partitions"
                )

                return True, "Connection successful", metadata

        except EventHubError as e:
            error_msg = f"Event Hub error: {str(e)}"
            logger.error(error_msg)
            return False, error_msg, {}
        except Exception as e:
            error_msg = f"Connection failed: {str(e)}"
            logger.error(error_msg)
            return False, error_msg, {}

    async def _test_connection_mock(self) -> tuple[bool, str, dict]:
        """Mock connection test for development without Azure SDK."""
        metadata = {
            "endpoint": "mock://localhost",
            "entity_path": "mock-hub",
            "namespace": "mock",
            "is_fabric_rti": False,
            "hub_name": self.hub_name or "mock-hub",
            "partition_count": 4,
            "partition_ids": ["0", "1", "2", "3"],
        }
        logger.info("Mock connection test (Azure SDK not installed)")
        return True, "Mock connection successful (Azure SDK not installed)", metadata
