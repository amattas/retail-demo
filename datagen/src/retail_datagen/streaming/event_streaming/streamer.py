"""
Main EventStreamer orchestration class.

This module provides the central EventStreamer class that coordinates
batch streaming from DuckDB to Azure Event Hub.
"""

from ...config.models import RetailConfig
from ...shared.logging_utils import get_structured_logger
from .batch_streaming import BatchStreamingManager
from .config import StreamingConfig


class EventStreamer:
    """
    Batch event streaming engine.

    Orchestrates batch streaming of retail events from DuckDB fact tables
    to Azure Event Hub.

    Features:
    - Batch streaming from DuckDB fact tables
    - Robust Azure Event Hub integration
    - Watermark-based incremental processing
    - Graceful error handling
    """

    def __init__(
        self,
        config: RetailConfig,
        azure_connection_string: str | None = None,
    ):
        """
        Initialize the event streaming engine.

        Args:
            config: Main retail configuration
            azure_connection_string: Azure Event Hub connection string
        """
        self.config = config
        self.streaming_config = StreamingConfig.from_retail_config(config)

        # Override connection string if provided
        if azure_connection_string:
            self.streaming_config.azure_connection_string = azure_connection_string

        # DuckDB connection for batch streaming
        self._duckdb_conn = None
        try:
            from retail_datagen.db.duckdb_engine import get_duckdb_conn

            self._duckdb_conn = get_duckdb_conn()
        except Exception:
            pass

        # Structured logger with session tracking
        self.log = get_structured_logger(__name__)
        self._session_id = self.log.generate_correlation_id()
        self.log.set_correlation_id(self._session_id)

        # Initialize batch streaming manager
        self._batch_streaming_manager = BatchStreamingManager(
            log=self.log,
            session_id=self._session_id,
            streaming_config=self.streaming_config,
        )

    async def start(self) -> bool:
        """
        Start batch streaming from DuckDB.

        Returns:
            bool: True if streaming completed successfully, False otherwise
        """
        return await self.start_batch_streaming_duckdb()

    async def start_batch_streaming_duckdb(self) -> bool:
        """
        Start batch streaming from DuckDB database.

        Returns:
            bool: True if streaming completed successfully, False otherwise
        """
        return await self._batch_streaming_manager.start_batch_streaming_duckdb(
            self._duckdb_conn, self.streaming_config.azure_connection_string
        )

    # Stub methods for API router compatibility.
    # Removed during batch-only simplification but routers still reference them.

    def get_dlq_summary(self) -> dict:
        """Get dead letter queue summary (stub - DLQ not supported in batch mode)."""
        return {"message": "DLQ not supported in batch-only mode", "entries": []}

    def retry_dlq_events(self, max_retries: int = 3) -> dict:
        """Retry DLQ events (stub - DLQ not supported in batch mode)."""
        return {"message": "DLQ not supported in batch-only mode", "retried": 0}

    @property
    def _dlq(self) -> list:
        """DLQ entries (stub - returns empty list)."""
        return []

    def pause(self) -> bool:
        """Pause streaming (stub - not supported in batch mode)."""
        self.log.warning("pause() not supported in batch-only mode")
        return False

    def resume(self) -> bool:
        """Resume streaming (stub - not supported in batch mode)."""
        self.log.warning("resume() not supported in batch-only mode")
        return False

    def get_pause_statistics(self) -> dict:
        """Get pause statistics (stub - not supported in batch mode)."""
        return {"message": "Pause not supported in batch-only mode"}
