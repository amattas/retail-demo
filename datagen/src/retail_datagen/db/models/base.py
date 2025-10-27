"""
Base class for all SQLAlchemy ORM models.

Provides common functionality and configuration for both master and fact tables.
"""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """
    Base class for all SQLAlchemy ORM models.

    Uses SQLAlchemy 2.0 declarative base pattern. All models inherit from this
    class to gain common functionality and metadata management.

    Note: We use separate databases (master.db and facts.db) but share the same
    metadata/Base class for consistency. Foreign key relationships will be
    defined but not enforced at the database level due to cross-database references.
    """
    pass
