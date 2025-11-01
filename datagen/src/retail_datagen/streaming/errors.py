"""Error classification for streaming system."""

from enum import Enum


class ErrorSeverity(Enum):
    """Error severity levels."""

    TRANSIENT = "transient"  # Retry automatically
    PERMANENT = "permanent"  # Don't retry, log and move on
    CRITICAL = "critical"  # Stop streaming, alert


class ErrorCategory(Enum):
    """Error categories for classification."""

    NETWORK = "network"
    AUTHENTICATION = "authentication"
    THROTTLING = "throttling"
    SERIALIZATION = "serialization"
    VALIDATION = "validation"
    UNKNOWN = "unknown"


class StreamingError(Exception):
    """Base exception for streaming errors."""

    def __init__(
        self,
        message: str,
        severity: ErrorSeverity,
        category: ErrorCategory,
        retryable: bool = True,
        original_exception: Exception | None = None,
    ):
        super().__init__(message)
        self.severity = severity
        self.category = category
        self.retryable = retryable
        self.original_exception = original_exception


def classify_error(exception: Exception) -> StreamingError:
    """Classify an exception into a StreamingError."""
    error_message = str(exception)
    error_type = type(exception).__name__

    # Network errors (transient, retryable)
    if any(
        keyword in error_type.lower()
        for keyword in ["timeout", "connection", "network"]
    ):
        return StreamingError(
            message=f"Network error: {error_message}",
            severity=ErrorSeverity.TRANSIENT,
            category=ErrorCategory.NETWORK,
            retryable=True,
            original_exception=exception,
        )

    # Authentication errors (permanent, not retryable)
    if any(
        keyword in error_type.lower()
        for keyword in ["auth", "unauthorized", "forbidden"]
    ):
        return StreamingError(
            message=f"Authentication error: {error_message}",
            severity=ErrorSeverity.PERMANENT,
            category=ErrorCategory.AUTHENTICATION,
            retryable=False,
            original_exception=exception,
        )

    # Throttling errors (transient, retryable with backoff)
    if any(
        keyword in error_message.lower()
        for keyword in ["throttl", "rate limit", "quota"]
    ):
        return StreamingError(
            message=f"Throttling error: {error_message}",
            severity=ErrorSeverity.TRANSIENT,
            category=ErrorCategory.THROTTLING,
            retryable=True,
            original_exception=exception,
        )

    # Serialization errors (permanent)
    if any(
        keyword in error_type.lower()
        for keyword in ["json", "serialization", "encoding"]
    ):
        return StreamingError(
            message=f"Serialization error: {error_message}",
            severity=ErrorSeverity.PERMANENT,
            category=ErrorCategory.SERIALIZATION,
            retryable=False,
            original_exception=exception,
        )

    # Default: unknown error (transient, retryable)
    return StreamingError(
        message=f"Unknown error: {error_message}",
        severity=ErrorSeverity.TRANSIENT,
        category=ErrorCategory.UNKNOWN,
        retryable=True,
        original_exception=exception,
    )
