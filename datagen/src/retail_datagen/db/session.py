"""
Database session management and context managers.

Provides async session factories and context managers for automatic
transaction handling with both master and facts databases.
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from retail_datagen.db.engine import get_facts_engine, get_master_engine

logger = logging.getLogger(__name__)

# Module-level session maker cache
_master_session_maker: async_sessionmaker[AsyncSession] | None = None
_facts_session_maker: async_sessionmaker[AsyncSession] | None = None


def master_session_maker() -> async_sessionmaker[AsyncSession]:
    """
    Get or create session maker for master database.

    The session maker is a factory for creating AsyncSession instances.
    It's configured with appropriate defaults for the master database.

    Returns:
        async_sessionmaker configured for master database

    Example:
        >>> SessionMaker = master_session_maker()
        >>> async with SessionMaker() as session:
        ...     result = await session.execute(select(Store))
    """
    global _master_session_maker

    if _master_session_maker is None:
        engine = get_master_engine()
        _master_session_maker = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            # Auto-flush before queries to ensure all pending changes are visible
            autoflush=True,
            # Don't auto-commit; we want explicit transaction control
            autocommit=False,
            # Expire objects after commit to ensure fresh data on next access
            expire_on_commit=True,
        )
        logger.debug("Created master database session maker")

    return _master_session_maker


def facts_session_maker() -> async_sessionmaker[AsyncSession]:
    """
    Get or create session maker for facts database.

    The session maker is a factory for creating AsyncSession instances.
    It's configured with appropriate defaults for the facts database.

    Returns:
        async_sessionmaker configured for facts database

    Example:
        >>> SessionMaker = facts_session_maker()
        >>> async with SessionMaker() as session:
        ...     result = await session.execute(select(Receipt))
    """
    global _facts_session_maker

    if _facts_session_maker is None:
        engine = get_facts_engine()
        _facts_session_maker = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            autoflush=True,
            autocommit=False,
            expire_on_commit=True,
        )
        logger.debug("Created facts database session maker")

    return _facts_session_maker


@asynccontextmanager
async def get_master_session() -> AsyncIterator[AsyncSession]:
    """
    Context manager for master database sessions with automatic transaction handling.

    This context manager:
    - Creates a new session from the master session maker
    - Automatically commits on successful completion
    - Automatically rolls back on exception
    - Ensures proper cleanup

    Yields:
        AsyncSession for master database operations

    Example:
        >>> async with get_master_session() as session:
        ...     store = Store(name="Store 1")
        ...     session.add(store)
        ...     # Automatically committed on exit

    Raises:
        Any exception from database operations (after rollback)
    """
    SessionMaker = master_session_maker()
    async with SessionMaker() as session:
        try:
            yield session
            await session.commit()
            logger.debug("Master session committed successfully")
        except Exception as e:
            await session.rollback()
            logger.error(f"Master session rolled back due to error: {e}")
            raise
        finally:
            await session.close()


@asynccontextmanager
async def get_facts_session() -> AsyncIterator[AsyncSession]:
    """
    Context manager for facts database sessions with automatic transaction handling.

    This context manager:
    - Creates a new session from the facts session maker
    - Automatically commits on successful completion
    - Automatically rolls back on exception
    - Ensures proper cleanup

    Yields:
        AsyncSession for facts database operations

    Example:
        >>> async with get_facts_session() as session:
        ...     receipt = Receipt(transaction_id="TXN001")
        ...     session.add(receipt)
        ...     # Automatically committed on exit

    Raises:
        Any exception from database operations (after rollback)
    """
    SessionMaker = facts_session_maker()
    async with SessionMaker() as session:
        try:
            yield session
            await session.commit()
            logger.debug("Facts session committed successfully")
        except Exception as e:
            await session.rollback()
            logger.error(f"Facts session rolled back due to error: {e}")
            raise
        finally:
            await session.close()


@asynccontextmanager
async def get_session(database: str = "master") -> AsyncIterator[AsyncSession]:
    """
    Generic context manager that routes to the appropriate database session.

    Args:
        database: Either "master" or "facts" to specify target database

    Yields:
        AsyncSession for specified database

    Raises:
        ValueError: If database parameter is not "master" or "facts"

    Example:
        >>> async with get_session("master") as session:
        ...     result = await session.execute(select(Store))
    """
    if database == "master":
        async with get_master_session() as session:
            yield session
    elif database == "facts":
        async with get_facts_session() as session:
            yield session
    else:
        raise ValueError(f"Invalid database: {database}. Must be 'master' or 'facts'")


class SessionContext:
    """
    Explicit transaction context for advanced session management.

    This class provides fine-grained control over transactions when
    automatic commit/rollback via context managers is not appropriate.

    Example:
        >>> ctx = SessionContext("master")
        >>> await ctx.begin()
        >>> try:
        ...     session = ctx.session
        ...     session.add(Store(name="Test"))
        ...     await ctx.commit()
        ... except Exception:
        ...     await ctx.rollback()
        ... finally:
        ...     await ctx.close()
    """

    def __init__(self, database: str = "master"):
        """
        Initialize session context for specified database.

        Args:
            database: Either "master" or "facts"
        """
        if database == "master":
            self._session_maker = master_session_maker()
        elif database == "facts":
            self._session_maker = facts_session_maker()
        else:
            raise ValueError(f"Invalid database: {database}")

        self.database = database
        self.session: AsyncSession | None = None

    async def begin(self) -> AsyncSession:
        """
        Start a new session.

        Returns:
            AsyncSession instance
        """
        if self.session is not None:
            raise RuntimeError("Session already active")

        self.session = self._session_maker()
        return self.session

    async def commit(self) -> None:
        """Commit the current transaction."""
        if self.session is None:
            raise RuntimeError("No active session")

        await self.session.commit()
        logger.debug(f"{self.database.capitalize()} session committed")

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        if self.session is None:
            raise RuntimeError("No active session")

        await self.session.rollback()
        logger.debug(f"{self.database.capitalize()} session rolled back")

    async def close(self) -> None:
        """Close the session."""
        if self.session is not None:
            await self.session.close()
            self.session = None
            logger.debug(f"{self.database.capitalize()} session closed")

    async def __aenter__(self) -> AsyncSession:
        """Support async context manager protocol."""
        return await self.begin()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Support async context manager protocol with automatic cleanup."""
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()
        await self.close()
