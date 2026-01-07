"""
Core FactDataGenerator class.

This module contains the main FactDataGenerator class which coordinates
historical fact data generation using specialized mixins for different
functional areas.
"""
from __future__ import annotations

import logging
import random
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from threading import Lock
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

# SessionMaker import for SQLite fallback path (deprecated, DuckDB-only runtime)
try:
    from retail_datagen.db.session import retail_session_maker
    SessionMaker = retail_session_maker()
except ImportError:
    SessionMaker = None  # type: ignore[assignment, misc]

import numpy as np

from retail_datagen.config.models import RetailConfig
from retail_datagen.generators.progress_tracker import TableProgressTracker
from retail_datagen.generators.seasonal_patterns import CompositeTemporalPatterns
from retail_datagen.generators.utils import ProgressReporter
from retail_datagen.shared.cache import CacheManager
from retail_datagen.shared.customer_geography import StoreSelector
from retail_datagen.shared.models import (
    Customer,
    DistributionCenter,
    GeographyMaster,
    ProductMaster,
    Store,
)

# Import business logic simulators
from ..retail_patterns import (
    BusinessRulesEngine,
    CustomerJourneySimulator,
    InventoryFlowSimulator,
    MarketingCampaignSimulator,
)
from .data_loading_mixin import DataLoadingMixin
from .inventory_mixin import InventoryMixin
from .logistics_mixin import LogisticsMixin
from .marketing_mixin import MarketingMixin
from .models import FactGenerationSummary
from .online_orders_mixin import OnlineOrdersMixin
from .persistence_mixin import PersistenceMixin
from .progress import HourlyProgressTracker
from .progress_reporting_mixin import ProgressReportingMixin
from .receipts_mixin import ReceiptsMixin
from .seasonal_mixin import SeasonalMixin
from .sensors_mixin import SensorsMixin
from .utils_mixin import UtilsMixin

logger = logging.getLogger(__name__)


class FactDataGenerator(
    DataLoadingMixin,
    InventoryMixin,
    LogisticsMixin,
    MarketingMixin,
    OnlineOrdersMixin,
    PersistenceMixin,
    ProgressReportingMixin,
    ReceiptsMixin,
    SeasonalMixin,
    SensorsMixin,
    UtilsMixin,
):
    """
    Main historical fact data generation engine.

    Generates all 9 fact tables with realistic retail behaviors, temporal patterns,
    and cross-fact coordination while maintaining business rule compliance.
        Writes data directly to DuckDB.
    """

    # Define the core fact tables that are always generated
    FACT_TABLES = [
        "dc_inventory_txn",
        "truck_moves",
        "truck_inventory",
        "store_inventory_txn",
        "receipts",
        "receipt_lines",
        "foot_traffic",
        "ble_pings",
        "marketing",
        # Omnichannel extension integrated into core facts
        "online_orders",
        "online_order_lines",
    ]

    # Truck unload duration constants (in minutes)
    MIN_UNLOAD_DURATION_MINUTES = 30  # Minimum realistic unload time
    DEFAULT_UNLOAD_DURATION_MINUTES = 60  # Default when ETA not available


    def __init__(
        self,
        config: RetailConfig,
        session: Any | None = None,
    ):
        """
        Initialize fact data generator.

        Args:
            config: Retail configuration containing generation parameters
            session: Deprecated; unused (DuckDB-only)
        """
        self.config = config
        self._rng = random.Random(config.seed)
        self._np_rng = np.random.default_rng(config.seed + 4242)
        # Keep attribute for compatibility; DuckDB path ignores it
        self._session = session
        # SQLite session deprecated; DuckDB used directly

        # Use DuckDB for fast fact writes by default
        self._use_duckdb = True
        self._duckdb_conn = None
        try:
            from retail_datagen.db.duckdb_engine import get_duckdb_conn

            self._duckdb_conn = get_duckdb_conn()
        except Exception as e:
            logger.warning(f"Failed to initialize DuckDB connection, falling back to in-memory mode: {e}")
            self._use_duckdb = False

        # Buffer for database writes
        # Structure: {table_name: [list of records]}
        self._db_buffer: dict[str, list[dict]] = {}

        # Precomputed sampling lists for hot-path customer selection
        # Maps store_id -> (customers_list, weights_list)
        self._store_customer_sampling: dict[int, tuple[list[Customer], list[float]]] = {}

        # Initialize patterns and simulators
        self.temporal_patterns = CompositeTemporalPatterns(config.seed)
        self.business_rules = BusinessRulesEngine()

        # Master data will be loaded from existing files
        self.geographies: list[GeographyMaster] = []
        self.stores: list[Store] = []
        self.distribution_centers: list[DistributionCenter] = []
        self.customers: list[Customer] = []
        self.products: list[ProductMaster] = []

        # Simulators will be initialized after loading master data
        self.customer_journey_sim: CustomerJourneySimulator | None = None
        self.inventory_flow_sim: InventoryFlowSimulator | None = None
        self.marketing_campaign_sim: MarketingCampaignSimulator | None = None

        # Customer geography and store selector (initialized after loading master data)
        self.store_selector: StoreSelector | None = None

        # Customer pools per store (for efficient selection during transaction generation)
        # Maps store_id -> list of (customer, weight) tuples
        self._store_customer_pools: dict[int, list[tuple[Customer, float]]] = {}

        # Track active campaigns and shipments
        self._active_campaigns: dict[str, Any] = {}
        self._active_shipments: dict[str, Any] = {}

        # Generate unique trace IDs
        self._trace_counter = 1

        # Progress callback for API integration (day-based throttled status)
        self._progress_callback = None
        # Per-table (master-style) progress callback
        self._table_progress_callback: (
            Callable[[str, float, str | None, dict | None], None] | None
        ) = None

        # Optional inclusion filter for which fact tables to generate
        self._included_tables: set[str] | None = None

        # Progress throttling for API updates (prevent flooding)
        self._last_progress_update_time = 0.0
        self._progress_lock = Lock()
        self._progress_history: list[tuple[float, float]] = []

        # Table state tracking for enhanced progress reporting
        self._progress_tracker: TableProgressTracker | None = None

        # Initialize hourly progress tracker for granular progress reporting
        self.hourly_tracker = HourlyProgressTracker(self.FACT_TABLES)
        logger.info("Initialized HourlyProgressTracker for progress reporting")

        # Fast CRM join map: AdId -> CustomerID (populated after master load)
        self._adid_to_customer_id: dict[str, int] = {}

        # Generation end date for filtering future-dated shipments (set during generate_historical_data)
        self._generation_end_date: datetime | None = None

        print(f"FactDataGenerator initialized with seed {config.seed}")


    def set_included_tables(self, tables: list[str] | None) -> None:
        """Restrict generation to a subset of FACT_TABLES, or clear filter if None."""
        if tables:
            allow = set(self.FACT_TABLES)
            self._included_tables = {t for t in tables if t in allow}
        else:
            self._included_tables = None
        self._reset_table_states()


    def _active_fact_tables(self) -> list[str]:
        return [
            t
            for t in self.FACT_TABLES
            if (self._included_tables is None or t in self._included_tables)
        ]


    async def generate_historical_data(
        self, start_date: datetime, end_date: datetime, *, publish_to_outbox: bool = False
    ) -> FactGenerationSummary:
        """
        Generate historical fact data for the specified date range.

        Writes data directly to DuckDB.

        Args:
            start_date: Start of historical data generation
            end_date: End of historical data generation

        Returns:
            Summary of generation results

        Note:
            This method is async to support database operations. Call with:
            ```python
            summary = await generator.generate_historical_data(start, end)
            ```
        """
        generation_start_time = datetime.now(UTC)
        # Remember outbox preference for this run so helpers
        # (e.g., _insert_hourly_to_db) can mirror to streaming_outbox
        self._publish_to_outbox = bool(publish_to_outbox)
        # Track generation end date for filtering future-dated shipments
        # Shipments scheduled beyond this date go to staging table
        self._generation_end_date = end_date
        print(
            f"Starting historical fact data generation from {start_date} to {end_date}"
        )

        # Reset table states for new generation run
        self._reset_table_states()

        # Reset hourly progress tracker for new generation run
        self.hourly_tracker.reset()

        # Ensure master data is loaded (DuckDB only)
        if not self.stores:
            self.load_master_data_from_duckdb()

        # Pre-check master readiness
        errors: list[str] = []
        if not self.stores:
            errors.append("No stores found in master database")
        if not self.customers:
            errors.append("No customers found in master database")
        if not self.products:
            errors.append("No products found in master database")
        if errors:
            raise ValueError("; ".join(errors))

        # Determine active tables
        active_tables = self._active_fact_tables()

        # Mark all tables as started
        for table in active_tables:
            self._progress_tracker.mark_table_started(table)

        # Initialize tracking for active tables only
        facts_generated = {t: 0 for t in active_tables}
        # Track records actually written to DB for live tile counts
        self._table_insert_counts: dict[str, int] = {t: 0 for t in active_tables}
        # Track DB totals to verify deltas (DuckDB path computes from summaries)
        self._fact_db_counts: dict[str, int] = {}

        # NEW: Add table progress tracking
        table_progress = {table: 0.0 for table in active_tables}

        total_days = (end_date - start_date).days + 1

        # Emit an early progress heartbeat so UIs show activity immediately
        try:
            self._send_throttled_progress_update(
                day_counter=0,
                message="Preparing historical data generation",
                total_days=total_days,
                table_progress=table_progress,
                tables_completed=[],
                tables_in_progress=active_tables,
                tables_remaining=[],
            )
        except Exception as e:
            logger.warning(f"Failed to send initial progress update: {e}")

        # Calculate expected records per table for accurate progress tracking
        # NOTE: customers_per_day is configured PER STORE, not total
        # Total daily customers = customers_per_day * number of stores
        customers_per_store_per_day = self.config.volume.customers_per_day
        total_customers_per_day = customers_per_store_per_day * len(self.stores)
        expected_records_all = {
            "receipts": total_days * total_customers_per_day,
            "receipt_lines": total_days * total_customers_per_day * 3,
            "foot_traffic": total_days * len(self.stores) * 100,
            "ble_pings": total_days * len(self.stores) * 500,
            "dc_inventory_txn": total_days * len(self.distribution_centers) * 50,
            "truck_moves": total_days * 10,
            "truck_inventory": total_days * 20,
            "store_inventory_txn": total_days * len(self.stores) * 20,
            "marketing": total_days * 10,
            "supply_chain_disruption": total_days * 2,
            "online_orders": total_days
            * max(0, int(self.config.volume.online_orders_per_day)),
        }
        expected_records = {
            k: v for k, v in expected_records_all.items() if k in active_tables
        }

        progress_reporter = ProgressReporter(total_days, "Generating historical data")

        # Generate data day by day
        current_date = start_date
        day_counter = 0

        # DuckDB-only runtime; no SQLAlchemy session

        async def _ensure_required_schema(session: AsyncSession) -> None:
            try:
                from sqlalchemy import text

                # Check if column exists
                res = await session.execute(text("PRAGMA table_info('fact_receipts')"))
                cols = [row[1] for row in res.fetchall()]
                # receipt_id_ext
                if "receipt_id_ext" not in cols:
                    await session.execute(text("ALTER TABLE fact_receipts ADD COLUMN receipt_id_ext TEXT"))
                    await session.execute(text("CREATE INDEX IF NOT EXISTS ix_fact_receipts_ext ON fact_receipts (receipt_id_ext)"))
                    logger.info("Migrated fact_receipts: added receipt_id_ext column and index")

                # receipt_type
                if "receipt_type" not in cols:
                    await session.execute(text("ALTER TABLE fact_receipts ADD COLUMN receipt_type TEXT NOT NULL DEFAULT 'SALE'"))
                    await session.execute(text("CREATE INDEX IF NOT EXISTS ix_fact_receipts_type ON fact_receipts (receipt_type)"))
                    logger.info("Migrated fact_receipts: added receipt_type column and index")

                # return_for_receipt_id
                if "return_for_receipt_id" not in cols:
                    await session.execute(text("ALTER TABLE fact_receipts ADD COLUMN return_for_receipt_id INTEGER"))
                    await session.execute(text("CREATE INDEX IF NOT EXISTS ix_fact_receipts_return_for ON fact_receipts (return_for_receipt_id)"))
                    logger.info("Migrated fact_receipts: added return_for_receipt_id column and index")

                # Ensure online order lines table exists
                await session.execute(text(
                    "CREATE TABLE IF NOT EXISTS fact_online_order_lines (\n"
                    " line_id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                    " order_id INTEGER NOT NULL,\n"
                    " product_id INTEGER NOT NULL,\n"
                    " line_num INTEGER NOT NULL,\n"
                    " quantity INTEGER NOT NULL,\n"
                    " unit_price FLOAT NOT NULL,\n"
                    " ext_price FLOAT NOT NULL,\n"
                    " promo_code VARCHAR(50) NULL\n"
                    ")"
                ))
                await session.execute(text("CREATE INDEX IF NOT EXISTS ix_online_order_lines_order ON fact_online_order_lines (order_id)"))
                await session.execute(text("CREATE INDEX IF NOT EXISTS ix_online_order_lines_order_product ON fact_online_order_lines (order_id, product_id)"))

                # Ensure dim_products has tags column
                res_prod = await session.execute(text("PRAGMA table_info('dim_products')"))
                prod_cols = [row[1] for row in res_prod.fetchall()]
                if "tags" not in prod_cols:
                    await session.execute(text("ALTER TABLE dim_products ADD COLUMN tags TEXT"))
                    logger.info("Migrated dim_products: added tags column")

                await session.commit()
            except Exception as e:
                logger.warning(f"Schema ensure failed (non-fatal): {e}")

        async def _run_with_session():
            nonlocal day_counter, current_date
            # Ensure schema is compatible (adds new columns/tables if missing)
            if not self._use_duckdb:
                await _ensure_required_schema(self._session)
        # Drop nonessential indexes for faster bulk loads (SQLite only; skipped in DuckDB)
            dropped_indexes: list[tuple[str, str]] = []
            try:
                if not self._use_duckdb:
                    # Notify UI about pre-load DB optimization
                    self._send_throttled_progress_update(
                        0,
                        "Optimizing database for bulk load (dropping indexes)",
                        total_days,
                        table_progress=table_progress,
                    )
                    dropped_indexes = await self._capture_and_drop_indexes(
                        self._session, active_tables
                    )
                    self._send_throttled_progress_update(
                        0,
                        "Bulk load optimizations applied",
                        total_days,
                        table_progress=table_progress,
                    )
            except Exception as e:
                logger.warning(f"Failed to drop indexes for bulk load optimization: {e}")
                dropped_indexes = []
            while current_date <= end_date:
                day_counter += 1

                # Generate daily facts (progress updates now happen during actual generation)
                daily_facts = await self._generate_daily_facts(
                    current_date, active_tables, day_counter, total_days
                )

                # Update counters
                for fact_type, records in daily_facts.items():
                    facts_generated[fact_type] += len(records)

                # Update per-table progress based on actual records generated
                for fact_type in facts_generated.keys():
                    current_count = facts_generated[fact_type]
                    expected = expected_records.get(fact_type, 1)
                    # Calculate actual progress (0.0 to 1.0), never exceed 1.0
                    table_progress[fact_type] = (
                        min(1.0, current_count / expected) if expected > 0 else 0.0
                    )

                # Emit per-table progress (master-style)
                for fact_type, prog in table_progress.items():
                    self._emit_table_progress(
                        fact_type,
                        prog,
                        f"Generating {fact_type.replace('_', ' ')}",
                        None,
                    )

                # Update progress tracker (progress only, not states)
                for table_name, progress in table_progress.items():
                    self._progress_tracker.update_progress(table_name, progress)

                # Get table lists from progress tracker
                tables_completed = self._progress_tracker.get_tables_by_state(
                    "completed"
                )
                tables_in_progress = self._progress_tracker.get_tables_by_state(
                    "in_progress"
                )
                tables_remaining = self._progress_tracker.get_tables_by_state(
                    "not_started"
                )

                # Calculate tables completed count
                tables_completed_count = len(tables_completed)

                # Enhanced message with table completion count
                enhanced_message = (
                    f"Generating data for {current_date.strftime('%Y-%m-%d')} "
                    f"(day {day_counter}/{total_days}) "
                    f"({tables_completed_count}/{len(active_tables)} tables complete)"
                )

                # Update API progress with throttling, include cumulative counts
                self._send_throttled_progress_update(
                    day_counter,
                    enhanced_message,
                    total_days,
                    table_progress=table_progress,
                    tables_completed=tables_completed,
                    tables_in_progress=tables_in_progress,
                    tables_remaining=tables_remaining,
                    # For UI tiles prefer DB-written counts if available, otherwise generation counts
                    table_counts=(
                        self._table_insert_counts.copy()
                        if getattr(self, "_table_insert_counts", None)
                        else facts_generated.copy()
                    ),
                )

                progress_reporter.update(1)
                current_date += timedelta(days=1)

            # Recreate any dropped indexes after generation completes for this run
            try:
                if (not self._use_duckdb) and dropped_indexes:
                    await self._recreate_indexes(self._session, dropped_indexes)
            except Exception as e:
                logger.warning(f"Failed to recreate indexes after generation: {e}")

        if self._use_duckdb:
            # No async DB session needed for DuckDB path
            await _run_with_session()
        elif self._session is None:
            async with SessionMaker() as session:
                self._session = session
                await _run_with_session()
                self._session = None
        else:
            await _run_with_session()

        progress_reporter.complete()

        # Mark generation complete (transitions all tables to 'completed')
        self._progress_tracker.mark_generation_complete()

        # Final validation
        validation_results = self.business_rules.get_validation_summary()

        generation_end_time = datetime.now(UTC)
        generation_time = (generation_end_time - generation_start_time).total_seconds()

        total_records = sum(facts_generated.values())

        summary = FactGenerationSummary(
            date_range=(start_date, end_date),
            facts_generated=facts_generated,
            total_records=total_records,
            validation_results=validation_results,
            generation_time_seconds=generation_time,
            partitions_created=0,  # No longer applicable in database-only mode
        )

        print(
            f"Historical data generation complete: {total_records} records "
            f"in {generation_time:.1f}s"
        )
        print(f"Generated {len(facts_generated)} fact tables")

        # Cache the counts for dashboard performance
        self._cache_fact_counts(facts_generated)

        # Update watermarks if database session provided (SQLite-only path)
        if self._session and not self._use_duckdb:
            await self._update_watermarks_after_generation(
                start_date, end_date, active_tables
            )
            logger.info("Updated watermarks for all generated fact tables")

        return summary


    def get_generation_summary(self, summary: FactGenerationSummary) -> dict[str, Any]:
        """Get detailed summary of fact generation."""
        return {
            "date_range": {
                "start": summary.date_range[0].isoformat(),
                "end": summary.date_range[1].isoformat(),
                "days": (summary.date_range[1] - summary.date_range[0]).days + 1,
            },
            "facts_generated": summary.facts_generated,
            "total_records": summary.total_records,
            "partitions_created": summary.partitions_created,
            "generation_time_seconds": summary.generation_time_seconds,
            "records_per_second": (
                summary.total_records / summary.generation_time_seconds
                if summary.generation_time_seconds > 0
                else 0
            ),
            "validation_results": summary.validation_results,
            "config": {
                "seed": self.config.seed,
                "stores": len(self.stores),
                "customers": len(self.customers),
                "products": len(self.products),
                "customers_per_day": self.config.volume.customers_per_day,
                "items_per_ticket_mean": self.config.volume.items_per_ticket_mean,
            },
        }


    def _cache_fact_counts(self, facts_generated: dict[str, int]) -> None:
        """
        Cache fact table counts for dashboard performance.

        Uses the counts from generation for accurate metrics.

        Args:
            facts_generated: Dictionary of table names and their record counts
        """
        try:
            cache_manager = CacheManager()

            # Cache the generation counts directly
            for table_name, count in facts_generated.items():
                cache_manager.update_fact_table(table_name, count, "Historical Data")
                logger.info(f"Cached {table_name}: {count} records")

            logger.info("Fact data counts cached successfully")
        except Exception as e:
            logger.error(f"Failed to cache counts: {e}")


# Convenience function for direct usage

