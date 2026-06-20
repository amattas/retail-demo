"""
Database persistence and data mapping methods.

This module provides the combined persistence capabilities for fact generation,
inheriting from focused mixin classes for field mapping and database operations.

The module has been modularized for maintainability while preserving
backward compatibility through mixin inheritance.
"""

from __future__ import annotations

from .db_operations import DbOperationsMixin

# Re-export FieldMappingMixin for any code that imports it directly
from .field_mapping import FieldMappingMixin

__all__ = ["PersistenceMixin", "FieldMappingMixin", "DbOperationsMixin"]


class PersistenceMixin(DbOperationsMixin):
    """Database persistence and data mapping methods.

    Combined mixin providing all persistence capabilities for fact generation.

    Inherits from:
        DbOperationsMixin: Index management, bulk inserts, watermark updates
            - Inherits from FieldMappingMixin: Field mapping, model resolution
    """

    pass
