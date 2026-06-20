"""Logging configuration for structured logging."""

import logging
import sys


def configure_structured_logging(level: str = "INFO"):
    """Configure structured logging for the application."""
    log_level = getattr(logging, level.upper())

    logging.basicConfig(
        level=log_level,
        format="%(message)s",  # JSON already formatted
        stream=sys.stdout,
    )

    # Explicitly set root logger level (basicConfig may be a no-op if handlers exist)
    logging.getLogger().setLevel(log_level)

    # Disable excessive third-party logging
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
