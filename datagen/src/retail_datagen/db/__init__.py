"""
Database connection and session management for retail data generator.

This module provides connection pooling, transaction support, and SQLite
configuration for the unified retail database (preferred) and legacy split
databases (master.db + facts.db for backward compatibility).

Preferred Usage:
    from retail_datagen.db import get_retail_session, get_retail_engine

    async with get_retail_session() as session:
        # Work with unified retail database
        ...

Legacy Usage (backward compatibility):
    from retail_datagen.db import get_master_session, get_facts_session

    # For migration from split databases only
    ...
"""

from retail_datagen.db.config import DatabaseConfig
from retail_datagen.db.engine import (
    check_engine_health,
    create_engine,
    dispose_engines,
    get_facts_engine,
    get_master_engine,
    get_retail_engine,
)
from retail_datagen.db.init import (
    analyze_database,
    check_database_integrity,
    create_all_tables,
    drop_all_tables,
    ensure_database_directories,
    get_database_info,
    init_databases,
    reset_database,
    vacuum_database,
)
from retail_datagen.db.manager import (
    DatabaseManager,
    get_database_status,
    get_db_manager,
    shutdown_databases,
    startup_databases,
)
from retail_datagen.db.migration import (
    migrate_to_unified_db,
    needs_migration,
    verify_migration,
)
from retail_datagen.db.purge import (
    get_purge_candidates,
    get_unpublished_data_range,
    get_watermark_status,
    mark_data_unpublished,
    purge_all_fact_tables,
    purge_published_data,
    update_publication_watermark,
)
from retail_datagen.db.session import (
    SessionContext,
    facts_session_maker,
    get_facts_session,
    get_master_session,
    get_retail_session,
    get_session,
    master_session_maker,
    retail_session_maker,
)

__all__ = [
    # Configuration
    "DatabaseConfig",
    # Engine creation and management
    "create_engine",
    "get_master_engine",
    "get_facts_engine",
    "get_retail_engine",
    "dispose_engines",
    "check_engine_health",
    # Session management
    "get_master_session",
    "get_facts_session",
    "get_retail_session",
    "master_session_maker",
    "facts_session_maker",
    "retail_session_maker",
    "get_session",
    "SessionContext",
    # Initialization and utilities
    "init_databases",
    "create_all_tables",
    "drop_all_tables",
    "ensure_database_directories",
    "reset_database",
    "vacuum_database",
    "analyze_database",
    "get_database_info",
    "check_database_integrity",
    # Manager
    "DatabaseManager",
    "get_db_manager",
    "startup_databases",
    "shutdown_databases",
    "get_database_status",
    # Purge utilities
    "update_publication_watermark",
    "mark_data_unpublished",
    "get_unpublished_data_range",
    "purge_published_data",
    "purge_all_fact_tables",
    "get_purge_candidates",
    "get_watermark_status",
    # Migration utilities
    "needs_migration",
    "migrate_to_unified_db",
    "verify_migration",
]
