"""
Base generator infrastructure for master data generation.

Provides core functionality: database operations, progress tracking, validation.
"""

import logging
from typing import Any, Callable

from retail_datagen.config.models import RetailConfig
from retail_datagen.shared.cache import CacheManager
from retail_datagen.shared.validators import (
    ForeignKeyValidator,
    PricingCalculator,
    SyntheticDataValidator,
)

from ..progress_tracker import TableProgressTracker

logger = logging.getLogger(__name__)


class _DuckModel:
    def __init__(self, name: str):
        self.__tablename__ = name


# Logical-to-physical mappings for DuckDB tables
GeographyModel = _DuckModel("dim_geographies")
StoreModel = _DuckModel("dim_stores")
DistributionCenterModel = _DuckModel("dim_distribution_centers")
TruckModel = _DuckModel("dim_trucks")
CustomerModel = _DuckModel("dim_customers")
ProductModel = _DuckModel("dim_products")


class BaseGenerator:
    """
    Base class providing shared infrastructure for master data generation.
    
    Handles:
    - Database connection and bulk inserts
    - Progress tracking and callbacks
    - Foreign key validation
    - Cache management
    """

    def __init__(self, config: RetailConfig):
        """
        Initialize base generator infrastructure.

        Args:
            config: Retail configuration containing generation parameters
        """
        self.config = config

        # Initialize validators and utilities
        self.pricing_calculator = PricingCalculator(config.seed)
        self.synthetic_validator = SyntheticDataValidator()
        self.fk_validator = ForeignKeyValidator()

        # Progress callback for UI updates
        self._progress_callback: (
            Callable[
                [
                    str,
                    float,
                    str | None,
                    dict[str, int] | None,
                    list[str] | None,
                    list[str] | None,
                    list[str] | None,
                ],
                None,
            ]
            | None
        ) = None

        # Table progress tracker for state management
        self._progress_tracker: TableProgressTracker | None = None

        # Enable DuckDB fast path for master writes
        self._use_duckdb = True
        self._duckdb_conn = None
        try:
            from retail_datagen.db.duckdb_engine import get_duckdb_conn

            self._duckdb_conn = get_duckdb_conn()
        except Exception as e:
            logger.warning(
                f"Failed to initialize DuckDB connection, falling back to in-memory mode: {e}"
            )
            self._use_duckdb = False

    def set_progress_callback(
        self,
        callback: (
            Callable[
                [
                    str,
                    float,
                    str | None,
                    dict[str, int] | None,
                    list[str] | None,
                    list[str] | None,
                    list[str] | None,
                ],
                None,
            ]
            | None
        ),
    ) -> None:
        """Register or clear a callback for incremental progress updates."""
        self._progress_callback = callback

    def _get_ui_table_name(self, model_class: Any) -> str | None:
        """Map ORM model to UI table key used by progress tiles."""
        tbl = getattr(model_class, "__tablename__", None)
        if not tbl:
            return None

        mapping = {
            "dim_geographies": "geographies_master",
            "dim_stores": "stores",
            "dim_distribution_centers": "distribution_centers",
            "dim_trucks": "trucks",
            "dim_customers": "customers",
            "dim_products": "products_master",
        }
        return mapping.get(tbl)

    async def _insert_to_db(
        self,
        session: Any | None,
        model_class: Any,
        pydantic_models: list,
        batch_size: int = 10000,
        commit_every_batches: int = 5,
    ) -> None:
        """
        Insert Pydantic models into database table using bulk insertion.

        Args:
            session: Database session for insertion
            model_class: SQLAlchemy model class (e.g., GeographyModel)
            pydantic_models: List of Pydantic model instances to insert
            batch_size: Number of rows per batch insert (default: 10,000)
            commit_every_batches: Commit frequency for large tables
        """
        if not pydantic_models:
            logger.warning(f"No data to insert for {model_class.__tablename__}")
            return

        table_name = model_class.__tablename__
        ui_table_name = self._get_ui_table_name(model_class)
        total_records = len(pydantic_models)
        logger.info(f"Inserting {total_records:,} records into {table_name}")

        try:
            # Convert Pydantic models to dictionaries
            records = []
            for model in pydantic_models:
                record = model.model_dump()
                mapped_record = self._map_pydantic_to_db_columns(record, model_class)
                records.append(mapped_record)

            # DuckDB fast path: write entire table via Arrow/Parquet buffers
            if (
                getattr(self, "_use_duckdb", False)
                and getattr(self, "_duckdb_conn", None) is not None
            ):
                import pandas as pd

                from retail_datagen.db.duckdb_engine import insert_dataframe

                df = pd.DataFrame.from_records(records)
                inserted = insert_dataframe(self._duckdb_conn, table_name, df)

                # Emit final progress as completed
                if ui_table_name:
                    self._emit_progress(
                        ui_table_name,
                        1.0,
                        f"Writing {ui_table_name.replace('_', ' ')} ({inserted:,}/{total_records:,})",
                        {ui_table_name: int(inserted)},
                    )
                logger.info(
                    f"Inserted {inserted:,} / {total_records:,} rows into {table_name} (DuckDB)"
                )
                return

            # Insert in batches for performance and memory efficiency
            batch_index = 0
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]

                # Use bulk_insert_mappings for performance
                await session.execute(model_class.__table__.insert(), batch)
                await session.flush()  # Flush to database but don't commit yet

                records_inserted = min(i + batch_size, total_records)
                logger.info(
                    f"Inserted {records_inserted:,} / {total_records:,} rows into {table_name}"
                )

                # Emit incremental progress every batch
                if ui_table_name:
                    fraction = records_inserted / total_records if total_records else 1.0
                    self._emit_progress(
                        ui_table_name,
                        fraction,
                        f"Writing {ui_table_name.replace('_', ' ')} ({records_inserted:,}/{total_records:,})",
                        {ui_table_name: records_inserted},
                    )

                batch_index += 1
                if commit_every_batches > 0 and (batch_index % commit_every_batches == 0):
                    try:
                        await session.commit()
                        logger.info(f"Committed after {batch_index} batches for {table_name}")
                    except Exception as e:
                        logger.warning(
                            f"Commit failed after batch {batch_index} for {table_name}: {e}"
                        )

            logger.info(f"Successfully inserted all {total_records:,} records into {table_name}")

        except Exception as e:
            logger.error(f"Failed to insert data into {table_name}: {e}", exc_info=True)
            raise

    def _map_pydantic_to_db_columns(
        self,
        pydantic_dict: dict[str, Any],
        model_class: Any,
    ) -> dict[str, Any]:
        """Identity mapping for DuckDB inserts via pandas DataFrame."""
        from datetime import datetime
        from decimal import Decimal

        mapped = dict(pydantic_dict)
        # Normalize Decimal to float for DataFrame serialization
        for k, v in list(mapped.items()):
            if isinstance(v, Decimal):
                mapped[k] = float(v)
            elif isinstance(v, datetime):
                # Keep as datetime; DuckDB will coerce appropriately
                mapped[k] = v
        return mapped

    def _emit_progress(
        self,
        table_name: str,
        progress: float,
        message: str | None = None,
        table_counts: dict[str, int] | None = None,
    ) -> None:
        """Send progress to the registered callback (if any)."""
        if not self._progress_callback:
            return

        try:
            clamped = max(0.0, min(1.0, progress))

            # Update progress tracker if available
            if (
                self._progress_tracker
                and table_name in self._progress_tracker.get_all_states()
            ):
                self._progress_tracker.update_progress(table_name, clamped)

            # Derive table state lists from the tracker
            tables_completed: list[str] | None = None
            tables_in_progress: list[str] | None = None
            tables_remaining: list[str] | None = None

            if self._progress_tracker:
                tables_completed = self._progress_tracker.get_tables_by_state(
                    TableProgressTracker.STATE_COMPLETED
                )
                tables_in_progress = self._progress_tracker.get_tables_by_state(
                    TableProgressTracker.STATE_IN_PROGRESS
                )
                tables_remaining = self._progress_tracker.get_tables_by_state(
                    TableProgressTracker.STATE_NOT_STARTED
                )

            # Call progress callback with extended positional arguments
            self._progress_callback(
                table_name,
                clamped,
                message,
                table_counts,
                tables_completed,
                tables_in_progress,
                tables_remaining,
            )
        except Exception as exc:
            logger.debug(
                "Master progress callback failed for %s: %s",
                table_name,
                exc,
                exc_info=True,
            )

    def _cache_master_counts(
        self,
        geography_count: int,
        store_count: int,
        dc_count: int,
        truck_count: int,
        customer_count: int,
        product_count: int,
    ) -> None:
        """Cache master table counts for dashboard performance."""
        try:
            cache_manager = CacheManager()

            cache_manager.update_master_table("geographies_master", geography_count, "Master Data")
            cache_manager.update_master_table("stores", store_count, "Master Data")
            cache_manager.update_master_table("distribution_centers", dc_count, "Master Data")
            cache_manager.update_master_table("trucks", truck_count, "Master Data")
            cache_manager.update_master_table("customers", customer_count, "Master Data")
            cache_manager.update_master_table("products_master", product_count, "Master Data")

            print("Master data counts cached successfully")
        except Exception as e:
            print(f"Warning: Failed to cache counts: {e}")
