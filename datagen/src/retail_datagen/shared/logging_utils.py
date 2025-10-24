"""Structured logging utilities for streaming."""
import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any, Optional


class StructuredLogger:
    """Structured logger with correlation ID support."""

    def __init__(self, logger_name: str):
        self.logger = logging.getLogger(logger_name)
        self._correlation_id: Optional[str] = None

    def set_correlation_id(self, correlation_id: str):
        """Set correlation ID for current context."""
        self._correlation_id = correlation_id

    def clear_correlation_id(self):
        """Clear correlation ID."""
        self._correlation_id = None

    def generate_correlation_id(self) -> str:
        """Generate new correlation ID."""
        return f"CORR_{uuid.uuid4().hex[:12]}"

    def _format_message(self, level: str, message: str, **kwargs) -> dict:
        """Format log message with structured data."""
        log_entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": level,
            "message": message,
            "correlation_id": self._correlation_id or "none",
        }

        # Add additional context
        if kwargs:
            log_entry["context"] = kwargs

        return log_entry

    def info(self, message: str, **kwargs):
        """Log info with structured data."""
        entry = self._format_message("INFO", message, **kwargs)
        self.logger.info(json.dumps(entry))

    def warning(self, message: str, **kwargs):
        """Log warning with structured data."""
        entry = self._format_message("WARNING", message, **kwargs)
        self.logger.warning(json.dumps(entry))

    def error(self, message: str, **kwargs):
        """Log error with structured data."""
        entry = self._format_message("ERROR", message, **kwargs)
        self.logger.error(json.dumps(entry))

    def debug(self, message: str, **kwargs):
        """Log debug with structured data."""
        entry = self._format_message("DEBUG", message, **kwargs)
        self.logger.debug(json.dumps(entry))


def get_structured_logger(name: str) -> StructuredLogger:
    """Get or create structured logger."""
    return StructuredLogger(name)
