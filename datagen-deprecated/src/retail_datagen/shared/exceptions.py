"""
Custom exceptions for the retail data generator.

This module contains specialized exception classes for handling various
error conditions in dictionary loading, data validation, and file processing.
"""

from pathlib import Path
from typing import Any


class RetailDataGenException(Exception):
    """Base exception for all retail data generator errors."""

    pass


class DictionaryLoadError(RetailDataGenException):
    """Exception raised when dictionary files cannot be loaded."""

    def __init__(
        self,
        message: str,
        file_path: Path | None = None,
        original_error: Exception | None = None,
    ):
        self.file_path = file_path
        self.original_error = original_error

        if file_path:
            message = f"Error loading dictionary file '{file_path}': {message}"

        if original_error:
            message = f"{message} (Original error: {original_error})"

        super().__init__(message)


class DictionaryValidationError(RetailDataGenException):
    """Exception raised when dictionary data fails validation."""

    def __init__(
        self,
        message: str,
        file_path: Path | None = None,
        row_number: int | None = None,
        column_name: str | None = None,
        invalid_value: Any | None = None,
        validation_errors: list[str] | None = None,
    ):
        self.file_path = file_path
        self.row_number = row_number
        self.column_name = column_name
        self.invalid_value = invalid_value
        self.validation_errors = validation_errors or []

        # Build detailed error message
        error_parts = [message]

        if file_path:
            error_parts.append(f"File: {file_path}")

        if row_number is not None:
            error_parts.append(f"Row: {row_number}")

        if column_name:
            error_parts.append(f"Column: {column_name}")

        if invalid_value is not None:
            error_parts.append(f"Value: {invalid_value}")

        if validation_errors:
            error_parts.extend(
                [f"Validation error: {error}" for error in validation_errors]
            )

        super().__init__(" | ".join(error_parts))


class DictionaryConsistencyError(RetailDataGenException):
    """Exception raised when data consistency checks fail across dictionaries."""

    def __init__(self, message: str, inconsistent_data: dict[str, Any] | None = None):
        self.inconsistent_data = inconsistent_data or {}

        if inconsistent_data:
            details = ", ".join(
                [f"{key}: {value}" for key, value in inconsistent_data.items()]
            )
            message = f"{message} (Inconsistent data: {details})"

        super().__init__(message)


class DictionaryFileNotFoundError(DictionaryLoadError):
    """Exception raised when required dictionary files are missing."""

    def __init__(self, file_path: Path, searched_paths: list[Path] | None = None):
        self.searched_paths = searched_paths or []

        message = f"Required dictionary file not found: {file_path}"

        if searched_paths:
            searched_str = ", ".join([str(p) for p in searched_paths])
            message = f"{message}. Searched in: {searched_str}"

        super().__init__(message, file_path)


class DictionaryParsingError(DictionaryLoadError):
    """Exception raised when CSV parsing fails."""

    def __init__(
        self,
        file_path: Path,
        message: str = "CSV parsing failed",
        row_number: int | None = None,
        original_error: Exception | None = None,
    ):
        self.row_number = row_number

        if row_number is not None:
            message = f"{message} at row {row_number}"

        super().__init__(message, file_path, original_error)


class DictionaryEncodingError(DictionaryLoadError):
    """Exception raised when file encoding issues prevent reading."""

    def __init__(
        self,
        file_path: Path,
        attempted_encodings: list[str],
        original_error: Exception | None = None,
    ):
        self.attempted_encodings = attempted_encodings

        encodings_str = ", ".join(attempted_encodings)
        message = f"Unable to read file with encodings: {encodings_str}"

        super().__init__(message, file_path, original_error)


class DictionarySchemaError(DictionaryValidationError):
    """Exception raised when CSV schema doesn't match expected format."""

    def __init__(
        self, file_path: Path, expected_columns: list[str], actual_columns: list[str]
    ):
        self.expected_columns = expected_columns
        self.actual_columns = actual_columns

        message = (
            f"Schema mismatch - Expected columns: {expected_columns}, "
            f"Found: {actual_columns}"
        )

        super().__init__(message, file_path)


class DictionaryCacheError(RetailDataGenException):
    """Exception raised when dictionary caching operations fail."""

    def __init__(
        self,
        message: str,
        cache_key: str | None = None,
        original_error: Exception | None = None,
    ):
        self.cache_key = cache_key
        self.original_error = original_error

        if cache_key:
            message = f"Cache error for key '{cache_key}': {message}"

        if original_error:
            message = f"{message} (Original error: {original_error})"

        super().__init__(message)


class DictionaryMemoryError(RetailDataGenException):
    """Exception raised when dictionary loading exceeds memory limits."""

    def __init__(
        self,
        message: str = "Dictionary loading exceeded memory limits",
        file_path: Path | None = None,
        file_size_mb: float | None = None,
    ):
        self.file_path = file_path
        self.file_size_mb = file_size_mb

        if file_path:
            message = f"{message} for file: {file_path}"

        if file_size_mb:
            message = f"{message} (File size: {file_size_mb:.2f} MB)"

        super().__init__(message)
