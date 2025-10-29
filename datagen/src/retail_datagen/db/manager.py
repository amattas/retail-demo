"""
Database connection lifecycle manager.

Provides singleton manager for coordinating database engines, sessions,
and lifecycle events (startup/shutdown).

Supports both unified retail database (preferred) and legacy split databases
(master.db + facts.db) for backward compatibility.
"""

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from retail_datagen.db.config import DatabaseConfig
from retail_datagen.db.engine import (
    dispose_engines,
    get_retail_engine,
    get_facts_engine,
    get_master_engine,
)
from retail_datagen.db.init import (
    check_database_integrity,
    ensure_database_directories,
    get_database_info,
    init_databases,
)
from retail_datagen.db.session import (
    retail_session_maker,
    facts_session_maker,
    get_retail_session,
    get_facts_session,
    get_master_session,
    master_session_maker,
)

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Singleton manager for database connections and lifecycle.

    This class coordinates:
    - Engine initialization and disposal
    - Session factory access
    - Health checks and monitoring
    - Startup and shutdown hooks

    Supports both unified retail.db (preferred) and legacy split databases
    (master.db + facts.db) for backward compatibility.

    Example:
        >>> manager = DatabaseManager()
        >>> await manager.startup()
        >>> # Use unified retail database
        >>> async with manager.get_retail_session() as session:
        ...     # Work with data
        >>> await manager.shutdown()
    """

    _instance: "DatabaseManager | None" = None
    _initialized: bool = False

    def __new__(cls) -> "DatabaseManager":
        """Ensure singleton pattern - only one instance exists."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize manager state (only once)."""
        # Prevent re-initialization
        if DatabaseManager._initialized:
            return

        self._retail_engine: AsyncEngine | None = None
        self._master_engine: AsyncEngine | None = None
        self._facts_engine: AsyncEngine | None = None
        self._is_running: bool = False

        DatabaseManager._initialized = True
        logger.debug("DatabaseManager initialized (singleton)")

    async def startup(self) -> None:
        """
        Initialize databases and prepare for operations.

        This method should be called during application startup.
        It ensures directories exist, creates database files,
        and verifies connectivity.

        Uses unified retail.db database (preferred approach).

        Raises:
            RuntimeError: If manager is already running
            Exception: If initialization fails
        """
        if self._is_running:
            logger.warning("DatabaseManager already running")
            return

        logger.info("Starting DatabaseManager...")

        try:
            # Ensure directories exist
            ensure_database_directories()

            # Initialize databases (creates files, applies pragmas, handles migration)
            await init_databases()

            # Cache retail engine reference (primary)
            self._retail_engine = get_retail_engine()

            # Cache legacy engine references for backward compatibility
            self._master_engine = get_master_engine()
            self._facts_engine = get_facts_engine()

            # Verify health of retail database
            retail_healthy = await self.check_retail_health()

            if not retail_healthy:
                raise RuntimeError("Retail database health check failed during startup")

            self._is_running = True
            logger.info("DatabaseManager startup complete (using retail.db)")

        except Exception as e:
            logger.error(f"DatabaseManager startup failed: {e}")
            await self.shutdown()  # Clean up partial initialization
            raise

    async def shutdown(self) -> None:
        """
        Gracefully shut down database connections.

        This method should be called during application shutdown.
        It disposes of all engines and closes connections.
        """
        if not self._is_running:
            logger.warning("DatabaseManager not running")
            return

        logger.info("Shutting down DatabaseManager...")

        try:
            # Dispose of all engines and close connections
            await dispose_engines()

            # Clear cached references
            self._retail_engine = None
            self._master_engine = None
            self._facts_engine = None
            self._is_running = False

            logger.info("DatabaseManager shutdown complete")

        except Exception as e:
            logger.error(f"Error during DatabaseManager shutdown: {e}")
            raise

    @property
    def is_running(self) -> bool:
        """Check if manager is running."""
        return self._is_running

    @property
    def retail_engine(self) -> AsyncEngine:
        """Get unified retail database engine (preferred)."""
        if self._retail_engine is None:
            self._retail_engine = get_retail_engine()
        return self._retail_engine

    @property
    def master_engine(self) -> AsyncEngine:
        """Get master database engine (legacy, for backward compatibility)."""
        if self._master_engine is None:
            self._master_engine = get_master_engine()
        return self._master_engine

    @property
    def facts_engine(self) -> AsyncEngine:
        """Get facts database engine (legacy, for backward compatibility)."""
        if self._facts_engine is None:
            self._facts_engine = get_facts_engine()
        return self._facts_engine

    async def get_retail_session(self):
        """Get retail database session context manager (preferred)."""
        return get_retail_session()

    async def get_master_session(self):
        """Get master database session context manager (legacy)."""
        return get_master_session()

    async def get_facts_session(self):
        """Get facts database session context manager (legacy)."""
        return get_facts_session()

    def retail_session_factory(self) -> Any:
        """Get retail database session maker (preferred)."""
        return retail_session_maker()

    def master_session_factory(self) -> Any:
        """Get master database session maker (legacy)."""
        return master_session_maker()

    def facts_session_factory(self) -> Any:
        """Get facts database session maker (legacy)."""
        return facts_session_maker()

    async def check_retail_health(self) -> bool:
        """
        Check retail database health (preferred).

        Returns:
            True if retail database is healthy, False otherwise
        """
        try:
            engine = self.retail_engine
            async with engine.begin() as conn:
                result = await conn.execute(text("SELECT 1"))
                result.scalar()
            logger.debug("Retail database health check: OK")
            return True
        except Exception as e:
            logger.error(f"Retail database health check failed: {e}")
            return False

    async def check_master_health(self) -> bool:
        """
        Check master database health (legacy).

        Returns:
            True if master database is healthy, False otherwise
        """
        try:
            engine = self.master_engine
            async with engine.begin() as conn:
                result = await conn.execute(text("SELECT 1"))
                result.scalar()
            logger.debug("Master database health check: OK")
            return True
        except Exception as e:
            logger.error(f"Master database health check failed: {e}")
            return False

    async def check_facts_health(self) -> bool:
        """
        Check facts database health (legacy).

        Returns:
            True if facts database is healthy, False otherwise
        """
        try:
            engine = self.facts_engine
            async with engine.begin() as conn:
                result = await conn.execute(text("SELECT 1"))
                result.scalar()
            logger.debug("Facts database health check: OK")
            return True
        except Exception as e:
            logger.error(f"Facts database health check failed: {e}")
            return False

    async def check_health(self) -> dict[str, bool]:
        """
        Check health of all databases.

        Returns:
            Dictionary with health status for each database:
            {
                "retail": True/False,
                "master": True/False (legacy),
                "facts": True/False (legacy),
                "overall": True/False
            }
        """
        retail_healthy = await self.check_retail_health()
        master_healthy = await self.check_master_health()
        facts_healthy = await self.check_facts_health()

        return {
            "retail": retail_healthy,
            "master": master_healthy,
            "facts": facts_healthy,
            "overall": retail_healthy and master_healthy and facts_healthy,
        }

    async def get_status(self) -> dict[str, Any]:
        """
        Get comprehensive status information.

        Returns:
            Dictionary containing:
            - is_running: Whether manager is running
            - health: Health check results
            - retail_info: Retail database information (primary)
            - master_info: Master database information (legacy)
            - facts_info: Facts database information (legacy)
            - config: Configuration settings
        """
        status = {
            "is_running": self._is_running,
            "health": await self.check_health() if self._is_running else {},
            "config": {
                "retail_db_path": DatabaseConfig.RETAIL_DB_PATH,
                "master_db_path": DatabaseConfig.MASTER_DB_PATH,
                "facts_db_path": DatabaseConfig.FACTS_DB_PATH,
                "pool_size": DatabaseConfig.POOL_SIZE,
                "pragmas": DatabaseConfig.SQLITE_PRAGMAS,
            },
        }

        # Add database info if running
        if self._is_running:
            try:
                status["retail_info"] = await get_database_info(self.retail_engine)
                status["master_info"] = await get_database_info(self.master_engine)
                status["facts_info"] = await get_database_info(self.facts_engine)
            except Exception as e:
                logger.error(f"Failed to get database info: {e}")
                status["retail_info"] = {"error": str(e)}
                status["master_info"] = {"error": str(e)}
                status["facts_info"] = {"error": str(e)}

        return status

    async def verify_integrity(self) -> dict[str, bool]:
        """
        Verify integrity of all databases.

        Returns:
            Dictionary with integrity check results:
            {
                "retail": True/False,
                "master": True/False (legacy),
                "facts": True/False (legacy),
                "overall": True/False
            }
        """
        retail_ok = await check_database_integrity(self.retail_engine)
        master_ok = await check_database_integrity(self.master_engine)
        facts_ok = await check_database_integrity(self.facts_engine)

        return {
            "retail": retail_ok,
            "master": master_ok,
            "facts": facts_ok,
            "overall": retail_ok and master_ok and facts_ok,
        }

    async def __aenter__(self) -> "DatabaseManager":
        """Support async context manager protocol."""
        await self.startup()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Support async context manager protocol."""
        await self.shutdown()


# Module-level singleton accessor
_db_manager: DatabaseManager | None = None


def get_db_manager() -> DatabaseManager:
    """
    Get the singleton DatabaseManager instance.

    Returns:
        DatabaseManager instance

    Example:
        >>> manager = get_db_manager()
        >>> await manager.startup()
    """
    global _db_manager

    if _db_manager is None:
        _db_manager = DatabaseManager()

    return _db_manager


async def startup_databases() -> None:
    """
    Convenience function to start the database manager.

    Example:
        >>> await startup_databases()
    """
    manager = get_db_manager()
    await manager.startup()


async def shutdown_databases() -> None:
    """
    Convenience function to shut down the database manager.

    Example:
        >>> await shutdown_databases()
    """
    manager = get_db_manager()
    await manager.shutdown()


async def get_database_status() -> dict[str, Any]:
    """
    Convenience function to get database status.

    Returns:
        Status dictionary from DatabaseManager

    Example:
        >>> status = await get_database_status()
        >>> print(f"Overall health: {status['health']['overall']}")
    """
    manager = get_db_manager()
    return await manager.get_status()
