"""
SQLAlchemy declarative base for all ORM models.

This module provides the base class that all SQLAlchemy models inherit from,
enabling ORM functionality and table metadata tracking.

NOTE: This is a legacy file. The actual Base class is defined in
retail_datagen.db.models.base. This file re-exports it for backward compatibility.
"""

from retail_datagen.db.models.base import Base

__all__ = ["Base"]
