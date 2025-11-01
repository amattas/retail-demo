"""
Database engine creation and management.

Provides async SQLAlchemy engines with proper SQLite configuration,
pragma enforcement, and connection event handling.
"""

import logging

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import StaticPool

from retail_datagen.db.config import DatabaseConfig

logger = logging.getLogger(__name__)

# Module-level engine cache
_master_engine: AsyncEngine | None = None
_facts_engine: AsyncEngine | None = None
_retail_engine: AsyncEngine | None = None


def create_engine(
    db_path: str,
    pragmas: dict[str, str | int] | None = None,
    echo: bool = False,
) -> AsyncEngine:
    """
    Create an async SQLAlchemy engine for SQLite with proper configuration.

    This function creates an engine with:
    - aiosqlite async driver
    - StaticPool (appropriate for SQLite's single-file nature)
    - PRAGMA enforcement via connection event listeners
    - Optional SQL query logging

    Args:
        db_path: Path to SQLite database file
        pragmas: Dictionary of PRAGMA settings to apply on each connection.
                If None, uses DatabaseConfig.SQLITE_PRAGMAS
        echo: If True, log all SQL queries (useful for debugging)

    Returns:
        Configured AsyncEngine instance

    Example:
        >>> engine = create_engine("data/master.db")
        >>> async with engine.begin() as conn:
        ...     await conn.execute(text("SELECT 1"))
    """
    if pragmas is None:
        pragmas = DatabaseConfig.SQLITE_PRAGMAS

    # Create async engine with SQLite-appropriate settings
    db_url = f"sqlite+aiosqlite:///{db_path}"
    engine = create_async_engine(
        db_url,
        # StaticPool maintains a single connection that is reused
        # This is appropriate for SQLite which doesn't support
        # traditional connection pooling
        poolclass=StaticPool,
        echo=echo,
        # Future-proofing for SQLAlchemy 2.0+ compatibility
        future=True,
    )

    # Register event listener to set PRAGMAs on each connection
    # This ensures optimal SQLite performance and behavior
    @event.listens_for(engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        """
        Set SQLite PRAGMAs on each new connection.

        This event handler runs whenever a new connection is established,
        ensuring consistent database behavior across all operations.
        """
        cursor = dbapi_connection.cursor()
        try:
            for pragma, value in pragmas.items():
                # Format PRAGMA command based on value type
                if isinstance(value, str):
                    cursor.execute(f"PRAGMA {pragma}={value}")
                else:
                    cursor.execute(f"PRAGMA {pragma}={value}")
            logger.debug(f"Applied {len(pragmas)} PRAGMAs to connection for {db_path}")
        except Exception as e:
            logger.error(f"Failed to apply PRAGMAs to {db_path}: {e}")
            raise
        finally:
            cursor.close()

    logger.info(f"Created async engine for database: {db_path}")
    return engine


def get_master_engine() -> AsyncEngine:
    """
    Get or create the master database engine (singleton).

    The master database stores permanent dimension tables:
    - Geographies, Stores, Distribution Centers, Trucks
    - Customers, Products
    - DC and Store inventory snapshots

    Returns:
        AsyncEngine for master database

    Note:
        This function implements lazy initialization and caching.
        The engine is created only once and reused across the application.
    """
    global _master_engine

    if _master_engine is None:
        _master_engine = create_engine(
            db_path=DatabaseConfig.MASTER_DB_PATH,
            echo=DatabaseConfig.ECHO_SQL,
        )
        logger.info("Initialized master database engine")

    return _master_engine


def get_facts_engine() -> AsyncEngine:
    """
    Get or create the facts database engine (singleton).

    The facts database stores temporary fact tables:
    - Receipt transactions and line items
    - Inventory movements (DC, truck, store)
    - Foot traffic and BLE pings
    - Marketing campaigns
    - Online orders and logistics

    This database is purged after events are published to Azure Event Hub.

    Returns:
        AsyncEngine for facts database

    Note:
        This function implements lazy initialization and caching.
        The engine is created only once and reused across the application.
    """
    global _facts_engine

    if _facts_engine is None:
        _facts_engine = create_engine(
            db_path=DatabaseConfig.FACTS_DB_PATH,
            echo=DatabaseConfig.ECHO_SQL,
        )
        logger.info("Initialized facts database engine")

    return _facts_engine


def get_retail_engine() -> AsyncEngine:
    """
    Get or create the unified retail database engine (singleton).

    The retail database stores all tables in a single unified database:
    - Master data: Geographies, Stores, Distribution Centers, Trucks,
      Customers, Products, DC and Store inventory snapshots
    - Fact data: Receipt transactions, inventory movements, foot traffic,
      BLE pings, marketing campaigns, online orders and logistics

    This is the preferred engine for all new development. The separate
    master and facts engines are maintained for backward compatibility.

    Returns:
        AsyncEngine for unified retail database

    Note:
        This function implements lazy initialization and caching.
        The engine is created only once and reused across the application.
    """
    global _retail_engine

    if _retail_engine is None:
        _retail_engine = create_engine(
            db_path=DatabaseConfig.RETAIL_DB_PATH,
            echo=DatabaseConfig.ECHO_SQL,
        )
        logger.info("Initialized unified retail database engine")

    return _retail_engine


async def dispose_engines() -> None:
    """
    Dispose of all database engines and close connections.

    This function should be called during application shutdown to ensure
    clean closure of all database connections and resources.

    Example:
        >>> await dispose_engines()
    """
    global _master_engine, _facts_engine, _retail_engine

    if _master_engine is not None:
        await _master_engine.dispose()
        logger.info("Disposed master database engine")
        _master_engine = None

    if _facts_engine is not None:
        await _facts_engine.dispose()
        logger.info("Disposed facts database engine")
        _facts_engine = None

    if _retail_engine is not None:
        await _retail_engine.dispose()
        logger.info("Disposed unified retail database engine")
        _retail_engine = None


async def check_engine_health(engine: AsyncEngine) -> bool:
    """
    Check if a database engine is healthy and can execute queries.

    Args:
        engine: AsyncEngine to check

    Returns:
        True if engine is healthy, False otherwise
    """
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT 1"))
            result.scalar()
        return True
    except Exception as e:
        logger.error(f"Engine health check failed: {e}")
        return False


# Import text for health check
from sqlalchemy import text
