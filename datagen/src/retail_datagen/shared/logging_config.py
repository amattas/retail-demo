"""Logging configuration for structured logging."""
import logging
import sys


def configure_structured_logging(level: str = "INFO"):
    """Configure structured logging for the application."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(message)s",  # JSON already formatted
        stream=sys.stdout,
    )

    # Disable excessive third-party logging
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
