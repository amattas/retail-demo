"""
Database connection and session management for retail data generator.

This module provides connection pooling, transaction support, and SQLite
configuration for both master (dimensions) and facts databases.
"""

from retail_datagen.db.config import DatabaseConfig
from retail_datagen.db.engine import (
    create_engine,
    get_master_engine,
    get_facts_engine,
    dispose_engines,
    check_engine_health,
)
from retail_datagen.db.session import (
    get_master_session,
    get_facts_session,
    master_session_maker,
    facts_session_maker,
    get_session,
    SessionContext,
)
from retail_datagen.db.init import (
    init_databases,
    create_all_tables,
    drop_all_tables,
    ensure_database_directories,
    reset_database,
    vacuum_database,
    analyze_database,
    get_database_info,
    check_database_integrity,
)
from retail_datagen.db.manager import (
    DatabaseManager,
    get_db_manager,
    startup_databases,
    shutdown_databases,
    get_database_status,
)
from retail_datagen.db.purge import (
    update_publication_watermark,
    mark_data_unpublished,
    get_unpublished_data_range,
    purge_published_data,
    purge_all_fact_tables,
    get_purge_candidates,
    get_watermark_status,
)
from retail_datagen.db.migration import (
    migrate_master_data_from_csv,
    migrate_fact_data_from_csv,
    migrate_table_from_csv,
    validate_foreign_keys,
)

__all__ = [
    # Configuration
    "DatabaseConfig",
    # Engine creation and management
    "create_engine",
    "get_master_engine",
    "get_facts_engine",
    "dispose_engines",
    "check_engine_health",
    # Session management
    "get_master_session",
    "get_facts_session",
    "master_session_maker",
    "facts_session_maker",
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
    "migrate_master_data_from_csv",
    "migrate_fact_data_from_csv",
    "migrate_table_from_csv",
    "validate_foreign_keys",
]
