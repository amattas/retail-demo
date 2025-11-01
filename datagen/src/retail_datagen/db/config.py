"""
Database configuration constants and settings.

Defines paths, SQLite pragmas, and connection pool settings for both
master (dimensions) and facts databases.

Supports two modes:
- Split mode: Separate master.db and facts.db files (legacy)
- Unified mode: Single retail.db file (preferred)
"""

import os


class DatabaseConfig:
    """Configuration for SQLite databases and connection pooling."""

    # Database file paths
    # Unified mode (preferred): Single database with all tables
    RETAIL_DB_PATH: str = "data/retail.db"

    # Split mode (legacy): Separate databases for dimensions and facts
    MASTER_DB_PATH: str = "data/master.db"
    FACTS_DB_PATH: str = "data/facts.db"

    # SQLite pragmas for optimal performance and safety
    # Applied on each connection via event listeners
    SQLITE_PRAGMAS: dict[str, str | int] = {
        # Write-Ahead Logging mode for better concurrency
        # Allows readers and writers to operate simultaneously
        "journal_mode": "WAL",
        # Balance between safety and performance
        # NORMAL is sufficient for most use cases with WAL
        "synchronous": "NORMAL",
        # Enable foreign key constraints (disabled by default in SQLite)
        "foreign_keys": 1,
        # Store temporary tables in memory for better performance
        "temp_store": "MEMORY",
        # Memory-mapped I/O for large databases (30GB)
        # Improves read performance significantly
        "mmap_size": 30000000000,
        # Page size in bytes (4KB is optimal for most systems)
        "page_size": 4096,
        # Negative value = size in KB (64MB cache)
        # Positive value = number of pages
        "cache_size": -64000,
        # Busy timeout in milliseconds
        # Wait up to 5 seconds when database is locked
        "busy_timeout": 5000,
    }

    # Connection pool settings
    # Note: SQLite uses StaticPool due to single-file nature
    # These are primarily for documentation and future-proofing
    POOL_SIZE: int = 20
    MAX_OVERFLOW: int = 10
    POOL_TIMEOUT: int = 30  # seconds
    POOL_RECYCLE: int = 3600  # seconds (1 hour)

    # Query execution settings
    STATEMENT_TIMEOUT: int = 300  # seconds (5 minutes)

    # Logging
    ECHO_SQL: bool = False  # Set to True for SQL query logging

    @classmethod
    def is_unified_mode(cls) -> bool:
        """
        Check if the system should use unified retail.db mode.

        The system uses unified mode when retail.db exists.
        If neither retail.db nor split databases exist, defaults to unified mode.

        Returns:
            True if unified retail.db should be used, False for split databases

        Logic:
            - If retail.db exists: Use unified mode
            - If split databases exist (master.db or facts.db): Use split mode
            - If no databases exist: Default to unified mode (preferred)
        """
        retail_exists = os.path.exists(cls.RETAIL_DB_PATH)
        master_exists = os.path.exists(cls.MASTER_DB_PATH)
        facts_exists = os.path.exists(cls.FACTS_DB_PATH)

        # If retail.db exists, always use unified mode
        if retail_exists:
            return True

        # If either split database exists, use split mode
        if master_exists or facts_exists:
            return False

        # No databases exist - default to unified mode (preferred)
        return True

    @classmethod
    def get_retail_db_url(cls) -> str:
        """
        Get SQLAlchemy database URL for unified retail database.

        Returns:
            Database URL string for retail database
        """
        return f"sqlite+aiosqlite:///{cls.RETAIL_DB_PATH}"

    @classmethod
    def get_master_db_url(cls) -> str:
        """
        Get SQLAlchemy database URL for master database.

        Returns:
            Database URL string for master database
        """
        return f"sqlite+aiosqlite:///{cls.MASTER_DB_PATH}"

    @classmethod
    def get_facts_db_url(cls) -> str:
        """
        Get SQLAlchemy database URL for facts database.

        Returns:
            Database URL string for facts database
        """
        return f"sqlite+aiosqlite:///{cls.FACTS_DB_PATH}"

    @classmethod
    def get_pragma_commands(cls) -> list[str]:
        """
        Get list of PRAGMA commands to execute on each connection.

        Returns:
            List of SQL PRAGMA statements
        """
        commands = []
        for pragma, value in cls.SQLITE_PRAGMAS.items():
            if isinstance(value, str):
                commands.append(f"PRAGMA {pragma}={value}")
            else:
                commands.append(f"PRAGMA {pragma}={value}")
        return commands
