"""
Database connection lifecycle manager.

Provides singleton manager for coordinating database engines, sessions,
and lifecycle events (startup/shutdown).
"""

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from retail_datagen.db.config import DatabaseConfig
from retail_datagen.db.engine import (
    dispose_engines,
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
    facts_session_maker,
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

    Example:
        >>> manager = DatabaseManager()
        >>> await manager.startup()
        >>> # Use databases...
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

            # Initialize databases (creates files, applies pragmas)
            await init_databases()

            # Cache engine references
            self._master_engine = get_master_engine()
            self._facts_engine = get_facts_engine()

            # Verify health
            master_healthy = await self.check_master_health()
            facts_healthy = await self.check_facts_health()

            if not (master_healthy and facts_healthy):
                raise RuntimeError("Database health checks failed during startup")

            self._is_running = True
            logger.info("DatabaseManager startup complete")

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
    def master_engine(self) -> AsyncEngine:
        """Get master database engine."""
        if self._master_engine is None:
            self._master_engine = get_master_engine()
        return self._master_engine

    @property
    def facts_engine(self) -> AsyncEngine:
        """Get facts database engine."""
        if self._facts_engine is None:
            self._facts_engine = get_facts_engine()
        return self._facts_engine

    async def get_master_session(self):
        """Get master database session context manager."""
        return get_master_session()

    async def get_facts_session(self):
        """Get facts database session context manager."""
        return get_facts_session()

    def master_session_factory(self) -> Any:
        """Get master database session maker."""
        return master_session_maker()

    def facts_session_factory(self) -> Any:
        """Get facts database session maker."""
        return facts_session_maker()

    async def check_master_health(self) -> bool:
        """
        Check master database health.

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
        Check facts database health.

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
                "master": True/False,
                "facts": True/False,
                "overall": True/False
            }
        """
        master_healthy = await self.check_master_health()
        facts_healthy = await self.check_facts_health()

        return {
            "master": master_healthy,
            "facts": facts_healthy,
            "overall": master_healthy and facts_healthy,
        }

    async def get_status(self) -> dict[str, Any]:
        """
        Get comprehensive status information.

        Returns:
            Dictionary containing:
            - is_running: Whether manager is running
            - health: Health check results
            - master_info: Master database information
            - facts_info: Facts database information
            - config: Configuration settings
        """
        status = {
            "is_running": self._is_running,
            "health": await self.check_health() if self._is_running else {},
            "config": {
                "master_db_path": DatabaseConfig.MASTER_DB_PATH,
                "facts_db_path": DatabaseConfig.FACTS_DB_PATH,
                "pool_size": DatabaseConfig.POOL_SIZE,
                "pragmas": DatabaseConfig.SQLITE_PRAGMAS,
            },
        }

        # Add database info if running
        if self._is_running:
            try:
                status["master_info"] = await get_database_info(self.master_engine)
                status["facts_info"] = await get_database_info(self.facts_engine)
            except Exception as e:
                logger.error(f"Failed to get database info: {e}")
                status["master_info"] = {"error": str(e)}
                status["facts_info"] = {"error": str(e)}

        return status

    async def verify_integrity(self) -> dict[str, bool]:
        """
        Verify integrity of both databases.

        Returns:
            Dictionary with integrity check results:
            {
                "master": True/False,
                "facts": True/False,
                "overall": True/False
            }
        """
        master_ok = await check_database_integrity(self.master_engine)
        facts_ok = await check_database_integrity(self.facts_engine)

        return {
            "master": master_ok,
            "facts": facts_ok,
            "overall": master_ok and facts_ok,
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
