"""
Historical fact data generation engine for retail data generator.

This module implements the FactDataGenerator class that creates realistic
retail transaction data for all 9 fact tables with proper temporal patterns,
business logic coordination, and SQLite database storage.
"""

import inspect
import logging
import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from datetime import time as dt_time
from decimal import Decimal
from pathlib import Path
from threading import Lock
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

from retail_datagen.generators.progress_tracker import TableProgressTracker
from retail_datagen.generators.seasonal_patterns import CompositeTemporalPatterns
from retail_datagen.generators.utils import ProgressReporter
from retail_datagen.shared.cache import CacheManager
from retail_datagen.shared.models import (
    Customer,
    DistributionCenter,
    GeographyMaster,
    InventoryReason,
    ProductMaster,
    Store,
    TenderType,
)

from ..config.models import RetailConfig
from .retail_patterns import (
    BusinessRulesEngine,
    CustomerJourneySimulator,
    InventoryFlowSimulator,
    MarketingCampaignSimulator,
)

logger = logging.getLogger(__name__)


class HourlyProgressTracker:
    """
    Thread-safe tracker for hourly progress across fact tables.

    Tracks progress on a per-table, per-day, per-hour basis to enable fine-grained
    progress reporting during historical data generation.

    Thread-safety is achieved using a threading.Lock to protect all shared state.
    """

    def __init__(self, fact_tables: list[str]):
        """
        Initialize the hourly progress tracker.

        Args:
            fact_tables: List of fact table names to track
        """
        self._fact_tables = fact_tables
        self._lock = Lock()

        # Track completed hours per table: {table: {day: {hour: True}}}
        self._progress_data: dict[str, dict[int, dict[int, bool]]] = {}

        # Track current position for each table
        self._current_day: dict[str, int] = {}
        self._current_hour: dict[str, int] = {}

        # Total days for progress calculation
        self._total_days = 0

        # Initialize tracking structures
        for table in fact_tables:
            self._progress_data[table] = {}
            self._current_day[table] = 0
            self._current_hour[table] = 0

        logger.debug(f"HourlyProgressTracker initialized for {len(fact_tables)} tables")

    def update_hourly_progress(
        self, table: str, day: int, hour: int, total_days: int
    ) -> None:
        """
        Update progress for a specific table after completing an hour.

        This method is thread-safe.

        Args:
            table: Name of the fact table
            day: Day number (1-indexed)
            hour: Hour of day (0-23)
            total_days: Total number of days being generated
        """
        with self._lock:
            # Validate inputs
            if table not in self._fact_tables:
                logger.warning(
                    f"Attempted to update progress for unknown table: {table}"
                )
                return

            if not (0 <= hour <= 23):
                logger.warning(f"Invalid hour value: {hour} (must be 0-23)")
                return

            # Update total days if changed
            self._total_days = total_days

            # Initialize day structure if needed
            if day not in self._progress_data[table]:
                self._progress_data[table][day] = {}

            # Mark hour as completed
            self._progress_data[table][day][hour] = True

            # Update current position
            self._current_day[table] = day
            self._current_hour[table] = hour

            logger.debug(
                f"Progress updated: {table} day {day} hour {hour} "
                f"({self._count_completed_hours(table)}/({total_days}*24) hours)"
            )

    def get_current_progress(self) -> dict:
        """
        Get current progress state for all tables.

        Returns:
            Dictionary containing:
            - overall_progress: float (0.0 to 1.0) - aggregate progress across all tables
            - tables_in_progress: list[str] - tables currently being processed
            - current_day: int - most recent day being processed
            - current_hour: int - most recent hour being processed
            - per_table_progress: dict[str, float] - progress for each table (0.0 to 1.0)
            - completed_hours: dict[str, int] - number of completed hours per table
        """
        with self._lock:
            # Calculate per-table progress
            per_table_progress = {}
            completed_hours_map = {}
            tables_in_progress = []

            total_hours_expected = self._total_days * 24 if self._total_days > 0 else 1

            for table in self._fact_tables:
                completed_hours = self._count_completed_hours(table)
                completed_hours_map[table] = completed_hours

                # Calculate progress as fraction of total hours
                progress = (
                    completed_hours / total_hours_expected
                    if total_hours_expected > 0
                    else 0.0
                )
                per_table_progress[table] = min(1.0, progress)

                # Table is in progress if it has completed some hours but not all
                if 0 < progress < 1.0:
                    tables_in_progress.append(table)

            # Calculate overall progress based on hours completed (not per-table average)
            # Since all tables are generated hour-by-hour together, use max hours completed
            max_completed_hours = (
                max(completed_hours_map.values()) if completed_hours_map else 0
            )
            overall_progress = (
                max_completed_hours / total_hours_expected
                if total_hours_expected > 0
                else 0.0
            )

            # All tables are "in progress" until all hours are complete (since they move together)
            # Only show tables as in_progress if we've started and haven't finished
            if 0 < overall_progress < 1.0:
                tables_in_progress = sorted(self._fact_tables)
            else:
                tables_in_progress = []

            # Find the most advanced position across all tables
            # Return None instead of 0 to avoid validation issues (current_day must be >= 1)
            max_day = max(self._current_day.values()) if self._current_day else None
            max_hour = max(self._current_hour.values()) if self._current_hour else None

            return {
                "overall_progress": min(1.0, overall_progress),
                "tables_in_progress": tables_in_progress,
                "current_day": max_day,
                "current_hour": max_hour,
                "per_table_progress": per_table_progress,
                "completed_hours": completed_hours_map,
                "total_days": self._total_days,
            }

    def reset(self) -> None:
        """
        Reset all tracking state.

        Called when starting a new generation run.
        """
        with self._lock:
            self._progress_data = {}
            self._current_day = {}
            self._current_hour = {}
            self._total_days = 0

            # Reinitialize structures for each table
            for table in self._fact_tables:
                self._progress_data[table] = {}
                self._current_day[table] = 0
                self._current_hour[table] = 0

            logger.debug("HourlyProgressTracker reset")

    def _count_completed_hours(self, table: str) -> int:
        """
        Count total completed hours for a table.

        Must be called with lock held.

        Args:
            table: Table name

        Returns:
            Total number of completed hours
        """
        if table not in self._progress_data:
            return 0

        total = 0
        for day_hours in self._progress_data[table].values():
            total += len(day_hours)

        return total


@dataclass
class FactGenerationSummary:
    """Summary of fact data generation results."""

    date_range: tuple[datetime, datetime]
    facts_generated: dict[str, int]
    total_records: int
    validation_results: dict[str, Any]
    generation_time_seconds: float
    partitions_created: int


@dataclass(frozen=True)
class MasterTableSpec:
    """Configuration describing how to load a master table."""

    attr_name: str
    filename: str
    model_cls: type[Any]
    dtype: dict[str, Any] | None = None
    row_adapter: Callable[[dict[str, Any]], dict[str, Any]] | None = None


class FactDataGenerator:
    """
    Main historical fact data generation engine.

    Generates all 9 fact tables with realistic retail behaviors, temporal patterns,
    and cross-fact coordination while maintaining business rule compliance.
    Writes data directly to SQLite database.
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
    ]

    def __init__(
        self,
        config: RetailConfig,
        session: AsyncSession,
    ):
        """
        Initialize fact data generator.

        Args:
            config: Retail configuration containing generation parameters
            session: AsyncSession for facts.db (required)
        """
        self.config = config
        self._rng = random.Random(config.seed)
        self._session = session

        # Buffer for database writes
        # Structure: {table_name: [list of records]}
        self._db_buffer: dict[str, list[dict]] = {}

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

        print(f"FactDataGenerator initialized with seed {config.seed}")

    @staticmethod
    def _normalize_geography_row(row: dict[str, Any]) -> dict[str, Any]:
        """Ensure geography rows preserve ZipCode formatting."""

        zip_code = row.get("ZipCode", "")
        return {**row, "ZipCode": str(zip_code)}

    @staticmethod
    def _to_decimal(value: Any) -> Decimal:
        """Convert a value into a Decimal instance."""

        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    @staticmethod
    def _to_bool(value: Any) -> bool:
        """Convert truthy strings and numerics to bool safely."""

        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes", "y", "t"}
        return bool(value)

    @classmethod
    def _normalize_product_row(cls, row: dict[str, Any]) -> dict[str, Any]:
        """Perform type conversions for product master rows."""

        launch_date = row.get("LaunchDate")
        if isinstance(launch_date, datetime):
            parsed_launch_date = launch_date
        else:
            parsed_launch_date = datetime.fromisoformat(str(launch_date))

        return {
            **row,
            "Cost": cls._to_decimal(row.get("Cost")),
            "MSRP": cls._to_decimal(row.get("MSRP")),
            "SalePrice": cls._to_decimal(row.get("SalePrice")),
            "RequiresRefrigeration": cls._to_bool(
                row.get("RequiresRefrigeration", False)
            ),
            "LaunchDate": parsed_launch_date,
        }

    def _master_table_specs(self) -> list[MasterTableSpec]:
        """Return specifications for all master tables that need loading."""

        return [
            MasterTableSpec(
                attr_name="geographies",
                filename="geographies_master.csv",
                model_cls=GeographyMaster,
                dtype={"ZipCode": str},
                row_adapter=self._normalize_geography_row,
            ),
            MasterTableSpec(
                attr_name="stores",
                filename="stores.csv",
                model_cls=Store,
            ),
            MasterTableSpec(
                attr_name="distribution_centers",
                filename="distribution_centers.csv",
                model_cls=DistributionCenter,
            ),
            MasterTableSpec(
                attr_name="customers",
                filename="customers.csv",
                model_cls=Customer,
            ),
            MasterTableSpec(
                attr_name="products",
                filename="products_master.csv",
                model_cls=ProductMaster,
                row_adapter=self._normalize_product_row,
            ),
        ]

    def _load_master_table(self, master_path: Path, spec: MasterTableSpec) -> list[Any]:
        """Load a master table based on the supplied specification."""

        dataframe = pd.read_csv(master_path / spec.filename, dtype=spec.dtype)
        records: list[Any] = []
        for _, row in dataframe.iterrows():
            row_dict = row.to_dict()
            if spec.row_adapter:
                row_dict = spec.row_adapter(row_dict)
            records.append(spec.model_cls(**row_dict))
        return records

    def _reset_table_states(self) -> None:
        """Reset table states using progress tracker."""
        active_tables = self._active_fact_tables()
        self._progress_tracker = TableProgressTracker(active_tables)
        self._progress_history = []

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

    def _calculate_eta(self, current_progress: float) -> float | None:
        """
        Calculate estimated seconds remaining based on progress rate.

        Args:
            current_progress: Current progress as a fraction (0.0 to 1.0)

        Returns:
            Estimated seconds remaining, or None if not enough data
        """
        if len(self._progress_history) < 2:
            return None

        # Calculate progress rate from history
        oldest = self._progress_history[0]
        newest = self._progress_history[-1]
        time_elapsed = newest[0] - oldest[0]
        progress_made = newest[1] - oldest[1]

        if progress_made <= 0 or time_elapsed <= 0:
            return None

        progress_rate = progress_made / time_elapsed  # progress per second
        remaining_progress = 1.0 - current_progress

        if progress_rate > 0:
            return remaining_progress / progress_rate

        return None

    # Per-table progress (master-style), similar to MasterDataGenerator
    def set_table_progress_callback(
        self,
        callback: Callable[[str, float, str | None, dict | None], None] | None,
    ) -> None:
        self._table_progress_callback = callback

    def set_progress_callback(
        self,
        callback: Callable[[int, str, dict], None] | None,
    ) -> None:
        """
        Set the day-based progress callback for historical generation.

        The callback will be invoked with progress updates during generation.
        Matches the pattern used by MasterDataGenerator for consistency.

        Args:
            callback: Progress callback function(day_num, message, **kwargs), or None to clear
        """
        self._progress_callback = callback

    def _emit_table_progress(
        self,
        table_name: str,
        progress: float,
        message: str | None = None,
        table_counts: dict | None = None,
    ) -> None:
        if not self._table_progress_callback:
            return
        try:
            clamped = max(0.0, min(1.0, progress))
            self._table_progress_callback(table_name, clamped, message, table_counts)
        except Exception:
            pass

    def load_master_data(self) -> None:
        """Legacy CSV loader (retained for backward compatibility)."""
        print("Loading master data for fact generation (CSV legacy path)...")

        master_path = Path(self.config.paths.master)

        loaded_counts: dict[str, int] = {}
        for spec in self._master_table_specs():
            records = self._load_master_table(master_path, spec)
            setattr(self, spec.attr_name, records)
            loaded_counts[spec.attr_name] = len(records)

        # Initialize simulators with loaded data
        self.customer_journey_sim = CustomerJourneySimulator(
            self.customers, self.products, self.stores, self.config.seed + 1000
        )

        self.inventory_flow_sim = InventoryFlowSimulator(
            self.distribution_centers,
            self.stores,
            self.products,
            self.config.seed + 2000,
        )

        self.marketing_campaign_sim = MarketingCampaignSimulator(
            self.customers, self.config.seed + 3000
        )

        print(
            "Loaded master data (CSV): "
            f"{loaded_counts.get('geographies', 0)} geographies, "
            f"{loaded_counts.get('stores', 0)} stores, "
            f"{loaded_counts.get('distribution_centers', 0)} DCs, "
            f"{loaded_counts.get('customers', 0)} customers, "
            f"{loaded_counts.get('products', 0)} products"
        )

    async def load_master_data_from_db(self) -> None:
        """Load master data directly from SQLite master database."""
        print("Loading master data for fact generation (SQLite)...")

        # Import ORM models lazily to avoid circulars
        from retail_datagen.db.models.master import (
            Customer as CustomerModel,
        )
        from retail_datagen.db.models.master import (
            DistributionCenter as DistributionCenterModel,
        )
        from retail_datagen.db.models.master import (
            Geography as GeographyModel,
        )
        from retail_datagen.db.models.master import (
            Product as ProductModel,
        )
        from retail_datagen.db.models.master import (
            Store as StoreModel,
        )
        from retail_datagen.db.session import retail_session_maker

        SessionMaker = retail_session_maker()
        async with SessionMaker() as session:
            # Geographies
            geos = (await session.execute(select(GeographyModel))).scalars().all()
            self.geographies = [
                GeographyMaster(
                    ID=g.geography_id,
                    City=g.city,
                    State=g.state,
                    ZipCode=str(g.postal_code),
                    District=g.district,
                    Region=g.region,
                )
                for g in geos
            ]

            # Stores
            stores = (await session.execute(select(StoreModel))).scalars().all()
            self.stores = [
                Store(
                    ID=s.store_id,
                    StoreNumber=s.store_number,
                    Address=s.address,
                    GeographyID=s.geography_id,
                )
                for s in stores
            ]

            # Distribution Centers
            dcs = (
                (await session.execute(select(DistributionCenterModel))).scalars().all()
            )
            self.distribution_centers = [
                DistributionCenter(
                    ID=d.dc_id,
                    DCNumber=d.dc_number,
                    Address=d.address,
                    GeographyID=d.geography_id,
                )
                for d in dcs
            ]

            # Customers
            customers = (await session.execute(select(CustomerModel))).scalars().all()
            self.customers = [
                Customer(
                    ID=c.customer_id,
                    FirstName=c.first_name,
                    LastName=c.last_name,
                    Address=c.address,
                    GeographyID=c.geography_id,
                    LoyaltyCard=c.loyalty_card,
                    Phone=c.phone,
                    BLEId=c.ble_id,
                    AdId=c.ad_id,
                )
                for c in customers
            ]

            # Products
            products = (await session.execute(select(ProductModel))).scalars().all()
            self.products = []
            for p in products:
                # Convert pricing floats to Decimal and date to datetime
                launch_dt = (
                    datetime.combine(p.launch_date, dt_time(0, 0))
                    if hasattr(p, "launch_date") and p.launch_date
                    else datetime.now()
                )
                self.products.append(
                    ProductMaster(
                        ID=p.product_id,
                        ProductName=p.product_name,
                        Brand=p.brand,
                        Company=p.company,
                        Department=p.department,
                        Category=p.category,
                        Subcategory=p.subcategory,
                        Cost=self._to_decimal(p.cost),
                        MSRP=self._to_decimal(p.msrp),
                        SalePrice=self._to_decimal(p.sale_price),
                        RequiresRefrigeration=bool(p.requires_refrigeration),
                        LaunchDate=launch_dt,
                    )
                )

        # Initialize simulators with loaded data
        self.customer_journey_sim = CustomerJourneySimulator(
            self.customers, self.products, self.stores, self.config.seed + 1000
        )

        self.inventory_flow_sim = InventoryFlowSimulator(
            self.distribution_centers,
            self.stores,
            self.products,
            self.config.seed + 2000,
        )

        self.marketing_campaign_sim = MarketingCampaignSimulator(
            self.customers, self.config.seed + 3000
        )

        print(
            "Loaded master data (SQLite): "
            f"{len(self.geographies)} geographies, "
            f"{len(self.stores)} stores, "
            f"{len(self.distribution_centers)} DCs, "
            f"{len(self.customers)} customers, "
            f"{len(self.products)} products"
        )

    async def generate_historical_data(
        self, start_date: datetime, end_date: datetime
    ) -> FactGenerationSummary:
        """
        Generate historical fact data for the specified date range.

        Writes data directly to SQLite database.

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
        generation_start_time = datetime.now()
        print(
            f"Starting historical fact data generation from {start_date} to {end_date}"
        )

        # Reset table states for new generation run
        self._reset_table_states()

        # Reset hourly progress tracker for new generation run
        self.hourly_tracker.reset()

        # Ensure master data is loaded (from SQLite)
        if not self.stores:
            await self.load_master_data_from_db()

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
        # Track DB totals to verify deltas
        self._fact_db_counts: dict[str, int] = {}
        try:
            from sqlalchemy import func, select

            # Read initial DB totals once for all active tables
            for t in active_tables:
                model = self._get_model_for_table(t)
                total = (
                    await self._session.execute(select(func.count()).select_from(model))
                ).scalar() or 0
                self._fact_db_counts[t] = int(total)
        except Exception as e:
            logger.debug(f"Initial DB count read failed (will verify per-hour): {e}")

        # NEW: Add table progress tracking
        table_progress = {table: 0.0 for table in active_tables}

        total_days = (end_date - start_date).days + 1

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

        # Generate data sequentially with managed retail session if not provided
        from retail_datagen.db.session import retail_session_maker

        created_session = False
        if self._session is None:
            SessionMaker = retail_session_maker()

        async def _ensure_receipt_ext_column(session: AsyncSession) -> None:
            try:
                from sqlalchemy import text

                # Check if column exists
                res = await session.execute(text("PRAGMA table_info('fact_receipts')"))
                cols = [row[1] for row in res.fetchall()]
                if "receipt_id_ext" not in cols:
                    await session.execute(
                        text("ALTER TABLE fact_receipts ADD COLUMN receipt_id_ext TEXT")
                    )
                    await session.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS ix_fact_receipts_ext ON fact_receipts (receipt_id_ext)"
                        )
                    )
                    await session.commit()
                    logger.info(
                        "Migrated fact_receipts: added receipt_id_ext column and index"
                    )
            except Exception as e:
                logger.warning(f"Could not ensure receipt_id_ext column exists: {e}")

        async def _run_with_session():
            nonlocal day_counter, current_date
            # Ensure schema is compatible (adds receipt_id_ext if missing)
            await _ensure_receipt_ext_column(self._session)
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

        if self._session is None:
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

        generation_end_time = datetime.now()
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

        # Update watermarks if database session provided
        if self._session:
            await self._update_watermarks_after_generation(
                start_date, end_date, active_tables
            )
            logger.info("Updated watermarks for all generated fact tables")

        return summary

    async def _generate_daily_facts(
        self, date: datetime, active_tables: list[str], day_index: int, total_days: int
    ) -> dict[str, list[dict]]:
        """
        Generate all fact data for a single day.

        Hourly progress updates are sent after each hour's data is exported, with
        thread-safe locking to prevent race conditions.

        Args:
            date: Date to generate facts for
            active_tables: List of fact tables to generate
            day_index: Current day number (1-indexed)
            total_days: Total number of days being generated

        Returns:
            Dictionary of fact tables with their records

        Note:
            Now async to support database operations during hourly exports.
        """
        daily_facts = {t: [] for t in active_tables}

        # Update available products for this date
        available_products = self._get_available_products_for_date(date)
        if self.customer_journey_sim:
            self.customer_journey_sim.update_available_products(available_products)

        # Generate base activity level for the day
        base_multiplier = self.temporal_patterns.get_overall_multiplier(date)

        # 1. Generate DC inventory transactions (supplier deliveries)
        if "dc_inventory_txn" in active_tables:
            dc_transactions = (
                self._generate_dc_inventory_txn(date, base_multiplier)
                if hasattr(self, "_generate_dc_inventory_txn")
                else self._generate_dc_inventory_transactions(date, base_multiplier)
            )
            daily_facts["dc_inventory_txn"].extend(dc_transactions)
            # Insert daily DC transactions immediately (not hourly)
            if dc_transactions:
                try:
                    import pandas as pd

                    df_dc = pd.DataFrame(dc_transactions)
                    await self._insert_hourly_to_db(
                        self._session,
                        "dc_inventory_txn",
                        df_dc,
                        hour=0,
                        commit_every_batches=1,
                    )
                    # Update progress for this daily-generated table (track all 24 hours as complete)
                    for hour in range(24):
                        self.hourly_tracker.update_hourly_progress(
                            "dc_inventory_txn", day_index, hour, total_days
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to insert dc_inventory_txn for {date.strftime('%Y-%m-%d')}: {e}"
                    )

        # 2. Generate marketing campaigns and impressions
        # Digital marketing runs 24/7 independently of store traffic/hours
        # Use constant multiplier of 1.0 for consistent digital ad delivery
        if "marketing" in active_tables:
            marketing_records = self._generate_marketing_activity(date, 1.0)
            if marketing_records:
                logger.debug(
                    f"Generated {len(marketing_records)} marketing records for {date.strftime('%Y-%m-%d')}"
                )
            daily_facts["marketing"].extend(marketing_records)

            # NEW: Update marketing progress (treated as completing all 24 hours at once)
            for hour in range(24):
                self.hourly_tracker.update_hourly_progress(
                    table="marketing", day=day_index, hour=hour, total_days=total_days
                )

            # NEW: Insert marketing records for the day directly (not hourly)
            if marketing_records:
                try:
                    import pandas as pd

                    df = pd.DataFrame(marketing_records)
                    await self._insert_hourly_to_db(
                        self._session, "marketing", df, hour=0, commit_every_batches=1
                    )
                    # Update progress for this daily-generated table (track all 24 hours as complete)
                    for hour in range(24):
                        self.hourly_tracker.update_hourly_progress(
                            "marketing", day_index, hour, total_days
                        )
                    # Verify daily insert size once for marketing
                    try:
                        from sqlalchemy import func, select

                        model = self._get_model_for_table("marketing")
                        total_db = (
                            await self._session.execute(
                                select(func.count()).select_from(model)
                            )
                        ).scalar() or 0
                        logger.info(
                            f"marketing verification (daily): inserted={len(marketing_records)}, db_total={int(total_db)}"
                        )
                    except Exception as ve:
                        logger.debug(f"Marketing verification skipped: {ve}")
                except Exception as e:
                    logger.error(
                        f"Failed to insert marketing for {date.strftime('%Y-%m-%d')}: {e}"
                    )

        # 3. Generate and write store operations hour-by-hour to minimize memory usage
        # Define which tables are generated hourly (others are generated daily)
        hourly_generated_tables = [
            "receipts",
            "receipt_lines",
            "store_inventory_txn",
            "foot_traffic",
            "ble_pings",
        ]

        # Log hourly data processing for debugging
        logger.debug(
            f"Day {day_index}/{total_days} ({date.strftime('%Y-%m-%d')}): Processing 24 hours"
        )

        # Generate and export each hour immediately to avoid accumulating all 24 hours in memory
        for hour_idx in range(24):
            # Generate data for this specific hour only
            hour_datetime = date.replace(
                hour=hour_idx, minute=0, second=0, microsecond=0
            )
            hour_multiplier = self.temporal_patterns.get_overall_multiplier(
                hour_datetime
            )

            if hour_multiplier == 0:  # Store closed
                hour_data = {
                    "receipts": [],
                    "receipt_lines": [],
                    "store_inventory_txn": [],
                    "foot_traffic": [],
                    "ble_pings": [],
                }
            else:
                hour_data = {
                    "receipts": [],
                    "receipt_lines": [],
                    "store_inventory_txn": [],
                    "foot_traffic": [],
                    "ble_pings": [],
                }

                # Generate customer transactions for each store for this hour
                for store in self.stores:
                    store_hour_data = self._generate_store_hour_activity(
                        store, hour_datetime, hour_multiplier
                    )
                    for fact_type, records in store_hour_data.items():
                        hour_data[fact_type].extend(records)
            hourly_subset = {
                t: (hour_data.get(t, []) if t in active_tables else [])
                for t in active_tables
            }
            try:
                await self._export_hourly_facts(date, hour_idx, hourly_subset)

                # NEW: Update hourly progress tracker after successful export
                # Only update progress for tables that are actually generated hourly
                for table in hourly_generated_tables:
                    if table in active_tables:
                        self.hourly_tracker.update_hourly_progress(
                            table=table,
                            day=day_index,
                            hour=hour_idx,
                            total_days=total_days,
                        )
                        # Log receipts progress at debug level
                        if table == "receipts":
                            logger.debug(
                                f"Receipts progress updated: day={day_index}, hour={hour_idx}, total_days={total_days}"
                            )

                # NEW: Send progress update after hourly exports complete (throttled)
                if self._progress_callback:
                    progress_data = self.hourly_tracker.get_current_progress()
                    # Convert to table progress dict format expected by throttled update
                    table_progress = progress_data.get("per_table_progress", {})

                    # Log thread info for debugging
                    thread_name = threading.current_thread().name
                    logger.debug(
                        f"[{thread_name}] Sending hourly progress: day {day_index}/{total_days}, "
                        f"hour {hour_idx + 1}/24"
                    )

                    # Send throttled progress update with hourly detail
                    self._send_throttled_progress_update(
                        day_counter=day_index,
                        message=f"Generating {date.strftime('%Y-%m-%d')} (day {day_index}/{total_days}, hour {hour_idx + 1}/24)",
                        total_days=total_days,
                        table_progress=table_progress,
                        tables_in_progress=progress_data.get("tables_in_progress", []),
                    )
            except Exception as e:
                logger.error(f"Hourly export failed for {date} hour {hour_idx}: {e}")
            for fact_type, records in hour_data.items():
                if fact_type in active_tables:
                    daily_facts[fact_type].extend(records)

        # 4. Generate truck movements (based on inventory needs)
        if "truck_moves" in active_tables:
            base_store_txn = daily_facts.get("store_inventory_txn", [])
            truck_movements = self._generate_truck_movements(date, base_store_txn)
            daily_facts["truck_moves"].extend(truck_movements)
            if truck_movements:
                try:
                    import pandas as pd

                    df_tm = pd.DataFrame(truck_movements)
                    await self._insert_hourly_to_db(
                        self._session,
                        "truck_moves",
                        df_tm,
                        hour=0,
                        commit_every_batches=1,
                    )
                    # Update progress for this daily-generated table (track all 24 hours as complete)
                    for hour in range(24):
                        self.hourly_tracker.update_hourly_progress(
                            "truck_moves", day_index, hour, total_days
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to insert truck_moves for {date.strftime('%Y-%m-%d')}: {e}"
                    )

        # 4a. Generate truck inventory tracking events
        if "truck_inventory" in active_tables:
            truck_inventory_events = (
                self.inventory_flow_sim.track_truck_inventory_status(date)
            )
            for event in truck_inventory_events:
                daily_facts["truck_inventory"].append(
                    {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": self._randomize_time_within_day(event["EventTS"]),
                        "TruckId": event["TruckId"],
                        "ShipmentId": event["ShipmentId"],
                        "ProductID": event["ProductID"],
                        "Quantity": event["Quantity"],
                        "Action": event["Action"],
                        "LocationID": event["LocationID"],
                        "LocationType": event["LocationType"],
                    }
                )

        # 5. Update inventory based on truck deliveries
        if "store_inventory_txn" in active_tables:
            base_truck_moves = daily_facts.get("truck_moves", [])
            delivery_transactions = self._process_truck_deliveries(
                date, base_truck_moves
            )
            daily_facts["store_inventory_txn"].extend(delivery_transactions)

        # 6. Generate online orders and integrate inventory effects
        if "online_orders" in active_tables:
            online_orders, online_store_txn, online_dc_txn = (
                self._generate_online_orders(date)
            )
            daily_facts["online_orders"].extend(online_orders)
            # Write online orders immediately (daily batch)
            if online_orders:
                try:
                    import pandas as pd

                    df_oo = pd.DataFrame(online_orders)
                    await self._insert_hourly_to_db(
                        self._session,
                        "online_orders",
                        df_oo,
                        hour=0,
                        commit_every_batches=1,
                    )
                    # Update progress for this daily-generated table (track all 24 hours as complete)
                    for hour in range(24):
                        self.hourly_tracker.update_hourly_progress(
                            "online_orders", day_index, hour, total_days
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to insert online_orders for {date.strftime('%Y-%m-%d')}: {e}"
                    )
            # Cascade inventory effects
            if "store_inventory_txn" in active_tables and online_store_txn:
                daily_facts["store_inventory_txn"].extend(online_store_txn)
            if "dc_inventory_txn" in active_tables and online_dc_txn:
                daily_facts["dc_inventory_txn"].extend(online_dc_txn)

        # 7. Generate supply chain disruptions
        if "supply_chain_disruption" in active_tables:
            disruption_events = (
                self.inventory_flow_sim.simulate_supply_chain_disruptions(date)
            )
            for disruption in disruption_events:
                daily_facts["supply_chain_disruption"].append(
                    {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": self._randomize_time_within_day(
                            disruption["EventTS"]
                        ),
                        "DCID": disruption["DCID"],
                        "Type": disruption["DisruptionType"].value,
                        "Severity": disruption["Severity"].value,
                        "Description": disruption["Description"],
                        "StartTime": disruption["StartTime"],
                        "EndTime": disruption["EndTime"],
                        "ImpactPercentage": disruption["ImpactPercentage"],
                        "AffectedProducts": disruption["AffectedProducts"],
                    }
                )

        return daily_facts

    def _generate_online_orders(
        self, date: datetime
    ) -> tuple[list[dict], list[dict], list[dict]]:
        """Generate online orders for the given date and corresponding inventory effects.

        Returns:
            (orders, store_inventory_txn, dc_inventory_txn)
        """
        orders: list[dict] = []
        store_txn: list[dict] = []
        dc_txn: list[dict] = []

        base_per_day = max(0, int(self.config.volume.online_orders_per_day))
        if base_per_day == 0 or not self.customers:
            return orders, store_txn, dc_txn

        # Seasonality/holiday multiplier, not bounded by store hours
        seasonal_mult = self.temporal_patterns.seasonal.get_seasonal_multiplier(date)
        # Smooth out extremes
        seasonal_mult = max(0.5, min(seasonal_mult, 2.5))
        total_orders = max(0, int(base_per_day * seasonal_mult))

        # Convenience
        rng = self._rng
        # InventoryReason and TenderType already imported from shared.models at module level

        for i in range(total_orders):
            # Random event time during the day
            hour = rng.randint(0, 23)
            minute = rng.randint(0, 59)
            second = rng.randint(0, 59)
            event_ts = datetime(date.year, date.month, date.day, hour, minute, second)

            customer = rng.choice(self.customers)

            # Generate a small basket using the same simulator
            basket = self.customer_journey_sim.generate_shopping_basket(customer.ID)

            # Choose fulfillment mode and node
            mode = rng.choices(
                ["SHIP_FROM_STORE", "SHIP_FROM_DC", "BOPIS"],
                weights=[0.55, 0.35, 0.10],
            )[0]

            if mode in ("SHIP_FROM_STORE", "BOPIS") and self.stores:
                node_type = "STORE"
                store = rng.choice(self.stores)
                node_id = store.ID
            else:
                node_type = "DC"
                dc = (
                    rng.choice(self.distribution_centers)
                    if self.distribution_centers
                    else None
                )
                if not dc:
                    # Fallback to store if no DCs
                    node_type = "STORE"
                    store = rng.choice(self.stores)
                    node_id = store.ID
                else:
                    node_id = dc.ID

            # Tally totals similar to receipts
            subtotal = basket.estimated_total
            tax_rate = Decimal("0.08")
            tax = subtotal * tax_rate
            total = subtotal + tax

            order_id = f"ONL{date.strftime('%Y%m%d')}{i:05d}{rng.randint(100, 999)}"
            trace_id = self._generate_trace_id()

            # Create one order record per product (like receipt lines)
            for product, qty in basket.items:
                # Calculate line total for this product
                line_subtotal = product.SalePrice * qty
                line_tax = line_subtotal * tax_rate
                line_total = line_subtotal + line_tax

                orders.append(
                    {
                        "TraceId": trace_id,
                        "EventTS": event_ts,
                        "OrderId": order_id,
                        "CustomerID": customer.ID,
                        "ProductID": product.ID,
                        "Quantity": qty,
                        "Total": str(line_total),
                        "FulfillmentMode": mode,
                        "FulfillmentNodeType": node_type,
                        "FulfillmentNodeID": node_id,
                        "Subtotal": str(line_subtotal),
                        "Tax": str(line_tax),
                        "TenderType": TenderType.CREDIT_CARD.value,
                    }
                )

            # Create inventory effects: decrement stock at node
            for product, qty in basket.items:
                if node_type == "STORE":
                    # Update store inventory and get balance
                    key = (node_id, product.ID)
                    current_balance = self.inventory_flow_sim._store_inventory.get(
                        key, 0
                    )
                    new_balance = max(0, current_balance - qty)
                    self.inventory_flow_sim._store_inventory[key] = new_balance

                    store_txn.append(
                        {
                            "TraceId": trace_id,
                            "EventTS": event_ts,
                            "StoreID": node_id,
                            "ProductID": product.ID,
                            "QtyDelta": -qty,
                            "Reason": InventoryReason.SALE.value,
                            "Source": "ONLINE",
                            "Balance": new_balance,
                        }
                    )
                else:  # DC
                    # Update DC inventory and get balance
                    key = (node_id, product.ID)
                    current_balance = self.inventory_flow_sim._dc_inventory.get(key, 0)
                    new_balance = max(0, current_balance - qty)
                    self.inventory_flow_sim._dc_inventory[key] = new_balance

                    dc_txn.append(
                        {
                            "TraceId": trace_id,
                            "EventTS": event_ts,
                            "DCID": node_id,
                            "ProductID": product.ID,
                            "QtyDelta": -qty,
                            "Reason": InventoryReason.SALE.value,
                            "Source": "ONLINE",
                            "Balance": new_balance,
                        }
                    )

        return orders, store_txn, dc_txn

    def _generate_dc_inventory_transactions(
        self, date: datetime, multiplier: float
    ) -> list[dict]:
        """Generate DC inventory transactions for a day."""
        transactions = []

        # Each DC receives shipments
        for dc in self.distribution_centers:
            dc_transactions = self.inventory_flow_sim.simulate_dc_receiving(dc.ID, date)

            for transaction in dc_transactions:
                # Get current balance from inventory simulator
                key = (transaction["DCID"], transaction["ProductID"])
                balance = self.inventory_flow_sim._dc_inventory.get(key, 0)

                transactions.append(
                    {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": self._randomize_time_within_day(date),
                        "DCID": transaction["DCID"],
                        "ProductID": transaction["ProductID"],
                        "QtyDelta": transaction["QtyDelta"],
                        "Reason": transaction["Reason"].value,
                        "Balance": balance,
                    }
                )

        return transactions

    def _generate_marketing_activity(
        self, date: datetime, multiplier: float
    ) -> list[dict]:
        """Generate marketing impressions and campaign activity."""

        # Defensive check: Verify simulator exists
        if self.marketing_campaign_sim is None:
            logger.error(
                "Marketing simulator not initialized - skipping marketing generation"
            )
            return []

        # Lightweight trace (DEBUG to avoid perf impact)
        logger.debug(
            f"_generate_marketing_activity: date={date}, mult={multiplier}, active={len(self._active_campaigns)}"
        )

        marketing_records = []

        # Check if new campaigns should start
        new_campaign_type = self.marketing_campaign_sim.should_start_campaign(
            date, multiplier
        )
        logger.debug(f"should_start_campaign returned: {new_campaign_type}")
        if new_campaign_type:
            campaign_id = self.marketing_campaign_sim.start_campaign(
                new_campaign_type, date
            )
            logger.debug(f"start_campaign returned: {campaign_id}")

            # Validation: Ensure campaign was actually created in simulator
            if campaign_id in self.marketing_campaign_sim._active_campaigns:
                # Store reference to campaign info, not just boolean
                campaign_info = self.marketing_campaign_sim._active_campaigns[
                    campaign_id
                ]
                self._active_campaigns[campaign_id] = campaign_info
                logger.debug(
                    f"Started new {new_campaign_type} campaign: {campaign_id} "
                    f"(end_date: {campaign_info['end_date']})"
                )
                logger.debug(
                    f"Campaign {campaign_id} added (total: {len(self._active_campaigns)})"
                )
            else:
                logger.error(f"Campaign {campaign_id} failed to create in simulator")
                logger.debug(
                    f"Simulator active campaigns: {list(self.marketing_campaign_sim._active_campaigns.keys())}"
                )
                # Critical failure - don't continue processing this day

        # Debug: Log state before sync
        logger.debug(
            f"Campaigns: fact_gen={len(self._active_campaigns)} sim={len(self.marketing_campaign_sim._active_campaigns)}"
        )

        # Sync: Remove orphaned campaigns that exist in tracking but not in simulator
        orphaned = set(self._active_campaigns.keys()) - set(
            self.marketing_campaign_sim._active_campaigns.keys()
        )
        if orphaned:
            logger.warning(f"Found {len(orphaned)} orphaned campaigns: {orphaned}")

        for campaign_id in orphaned:
            logger.debug(f"Removing orphaned campaign {campaign_id}")
            del self._active_campaigns[campaign_id]

        logger.debug(f"After sync: fact_gen campaigns={len(self._active_campaigns)}")

        # Performance guard: cap total impressions/day (scaled by multiplier)
        base_cap = (
            getattr(self.config.volume, "marketing_impressions_per_day", 10000) or 10000
        )
        daily_cap = max(1000, int(base_cap * max(0.5, min(multiplier, 2.0))))
        emitted = 0

        # Generate impressions for active campaigns
        for campaign_id in list(self._active_campaigns.keys()):
            logger.debug(f"Processing campaign {campaign_id}")

            # Check if campaign has reached its end date
            campaign = self.marketing_campaign_sim._active_campaigns.get(campaign_id)

            # CRITICAL: Detect state corruption
            if campaign is None:
                logger.error(
                    f"STATE CORRUPTION: Campaign {campaign_id} tracked in fact_gen "
                    f"but missing from simulator. Removing from fact_gen."
                )
                del self._active_campaigns[campaign_id]
                continue  # Skip this campaign entirely

            logger.debug(
                f"Campaign: {campaign.get('type', 'unknown')} end={campaign.get('end_date', 'unknown')}"
            )

            if date > campaign["end_date"]:
                # Campaign has completed its scheduled run
                del self._active_campaigns[campaign_id]
                logger.debug(f"Campaign {campaign_id} completed on {date}")
                logger.info(f"    Campaign {campaign_id} DELETED (expired)")
                continue

            logger.debug(f"Campaign {campaign_id} active, generating impressions...")

            try:
                impressions = self.marketing_campaign_sim.generate_campaign_impressions(
                    campaign_id, date, multiplier
                )
            except Exception as e:
                logger.error(
                    f"generate_campaign_impressions failed for {campaign_id}: {e}"
                )
                impressions = []

            logger.debug(f"impressions returned={len(impressions)}")

            if not impressions:
                logger.warning(
                    f"    No impressions generated for campaign {campaign_id}"
                )

            # Note: Zero impressions are acceptable - campaign continues if not expired
            for impression in impressions:
                logger.debug(
                    f"      Creating marketing record: {impression.get('channel', 'unknown')}"
                )
                marketing_records.append(
                    {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": self._randomize_time_within_day(date),
                        "Channel": impression["Channel"].value,
                        "CampaignId": impression["CampaignId"],
                        "CreativeId": impression["CreativeId"],
                        "CustomerAdId": impression["CustomerAdId"],
                        "ImpressionId": impression["ImpressionId"],
                        "Cost": str(impression["Cost"]),
                        "Device": impression["Device"].value,
                    }
                )
                emitted += 1
                if emitted >= daily_cap:
                    logger.warning(
                        f"Marketing daily cap reached ({daily_cap}) on {date}. Truncating impressions."
                    )
                    break

            if emitted >= daily_cap:
                break

        logger.debug(
            f"_generate_marketing_activity complete: {len(marketing_records)} total"
        )
        return marketing_records

    # NOTE: _generate_hourly_store_activity was removed in favor of inline hour-by-hour
    # generation to reduce memory usage. The logic is now inlined in _generate_daily_facts
    # starting at line ~1015 to write each hour to the database immediately instead of
    # accumulating all 24 hours in memory first.

    def _generate_store_hour_activity(
        self, store: Store, hour_datetime: datetime, multiplier: float
    ) -> dict[str, list[dict]]:
        """Generate activity for a single store during one hour."""
        hour_data = {
            "receipts": [],
            "receipt_lines": [],
            "store_inventory_txn": [],
            "foot_traffic": [],
            "ble_pings": [],
        }

        # Calculate expected customers for this hour
        # NOTE: customers_per_day is configured PER STORE, not total across all stores
        base_customers_per_hour = self.config.volume.customers_per_day / 24
        expected_customers = int(base_customers_per_hour * multiplier)

        if expected_customers == 0:
            return hour_data

        # Generate foot traffic (slightly more than actual customers)
        foot_traffic_count = max(1, int(expected_customers * 1.2))
        foot_traffic_records = self._generate_foot_traffic(
            store, hour_datetime, foot_traffic_count
        )
        hour_data["foot_traffic"].extend(foot_traffic_records)

        # Generate customer transactions
        for _ in range(expected_customers):
            customer = self._rng.choice(self.customers)

            # Generate shopping basket
            basket = self.customer_journey_sim.generate_shopping_basket(customer.ID)

            # Create receipt
            receipt_data = self._create_receipt(store, customer, basket, hour_datetime)
            hour_data["receipts"].append(receipt_data["receipt"])
            hour_data["receipt_lines"].extend(receipt_data["lines"])
            hour_data["store_inventory_txn"].extend(
                receipt_data["inventory_transactions"]
            )

            # Generate BLE pings for this customer
            ble_records = self._generate_ble_pings(store, customer, hour_datetime)
            hour_data["ble_pings"].extend(ble_records)

        return hour_data

    def _create_receipt(
        self, store: Store, customer: Customer, basket: Any, transaction_time: datetime
    ) -> dict[str, list[dict]]:
        """Create receipt, receipt lines, and inventory transactions."""
        receipt_id = (
            f"RCP{transaction_time.strftime('%Y%m%d%H%M')}"
            f"{store.ID:03d}{self._rng.randint(1000, 9999)}"
        )
        trace_id = self._generate_trace_id()

        # Calculate receipt totals
        subtotal = basket.estimated_total
        tax_rate = Decimal("0.08")  # 8% tax rate
        tax = subtotal * tax_rate
        total = subtotal + tax

        # Select tender type based on customer segment
        tender_weights = {
            TenderType.CREDIT_CARD: 0.4,
            TenderType.DEBIT_CARD: 0.3,
            TenderType.CASH: 0.2,
            TenderType.MOBILE_PAY: 0.1,
        }

        tender_options = list(tender_weights.keys())
        weights = list(tender_weights.values())
        tender_type = self._rng.choices(tender_options, weights=weights)[0]

        # Create receipt header
        receipt = {
            "TraceId": trace_id,
            "EventTS": transaction_time,
            "StoreID": store.ID,
            "CustomerID": customer.ID,
            "ReceiptId": receipt_id,
            "Subtotal": str(subtotal),
            "Tax": str(tax),
            "Total": str(total),
            "TenderType": tender_type.value,
        }

        # Create receipt lines and inventory transactions
        lines = []
        inventory_transactions = []

        for line_num, (product, qty) in enumerate(basket.items, 1):
            # Apply any promotional pricing
            unit_price = product.SalePrice
            promo_code = None

            # Random promotional discounts (10% chance)
            if self._rng.random() < 0.1:
                discount = self._rng.uniform(0.05, 0.25)
                unit_price = unit_price * (1 - Decimal(str(discount)))
                promo_code = f"PROMO{self._rng.randint(100, 999)}"

            ext_price = unit_price * qty

            line = {
                "TraceId": trace_id,
                "EventTS": transaction_time,
                "ReceiptId": receipt_id,
                "Line": line_num,
                "ProductID": product.ID,
                "Qty": qty,
                "UnitPrice": str(unit_price),
                "ExtPrice": str(ext_price),
                "PromoCode": promo_code,
            }
            lines.append(line)

            # Create inventory transaction (sale)
            # Update store inventory and get balance
            key = (store.ID, product.ID)
            current_balance = self.inventory_flow_sim._store_inventory.get(key, 0)
            new_balance = max(0, current_balance - qty)
            self.inventory_flow_sim._store_inventory[key] = new_balance

            inventory_transaction = {
                "TraceId": trace_id,
                "EventTS": transaction_time,
                "StoreID": store.ID,
                "ProductID": product.ID,
                "QtyDelta": -qty,  # Negative for sale
                "Reason": InventoryReason.SALE.value,
                "Source": "CUSTOMER_PURCHASE",
                "Balance": new_balance,
            }
            inventory_transactions.append(inventory_transaction)

        return {
            "receipt": receipt,
            "lines": lines,
            "inventory_transactions": inventory_transactions,
        }

    def _generate_foot_traffic(
        self, store: Store, hour_datetime: datetime, traffic_count: int
    ) -> list[dict]:
        """Generate foot traffic sensor records."""
        traffic_records = []

        # Store zones where sensors are placed
        zones = ["ENTRANCE", "AISLES_A", "AISLES_B", "CHECKOUT", "EXIT"]

        for _ in range(traffic_count):
            # Simulate customer path through store
            zone = self._rng.choice(zones)
            sensor_id = f"SENSOR_{store.ID:03d}_{zone}"

            # Dwell time based on zone
            dwell_times = {
                "ENTRANCE": (30, 120),  # 30 seconds to 2 minutes
                "AISLES_A": (120, 600),  # 2 to 10 minutes
                "AISLES_B": (120, 600),  # 2 to 10 minutes
                "CHECKOUT": (60, 300),  # 1 to 5 minutes
                "EXIT": (15, 60),  # 15 seconds to 1 minute
            }

            min_dwell, max_dwell = dwell_times[zone]
            dwell_time = self._rng.randint(min_dwell, max_dwell)

            traffic_record = {
                "TraceId": self._generate_trace_id(),
                "EventTS": self._randomize_time_within_hour(hour_datetime),
                "StoreID": store.ID,
                "SensorId": sensor_id,
                "Zone": zone,
                "Dwell": dwell_time,
                "Count": 1,  # Single person detection
            }
            traffic_records.append(traffic_record)

        return traffic_records

    def _generate_ble_pings(
        self, store: Store, customer: Customer, transaction_time: datetime
    ) -> list[dict]:
        """Generate BLE beacon pings for a customer visit."""
        ble_records = []

        # Simulate customer journey through store with BLE pings
        zones = ["ENTRANCE", "ELECTRONICS", "GROCERY", "CLOTHING", "CHECKOUT"]
        beacons = [f"BEACON_{store.ID:03d}_{zone}" for zone in zones]

        # Customer visits 2-4 zones during their journey
        visited_zones = self._rng.sample(
            list(zip(zones, beacons)), self._rng.randint(2, 4)
        )

        for zone, beacon_id in visited_zones:
            # Multiple pings per zone (2-5 pings)
            ping_count = self._rng.randint(2, 5)

            for _ in range(ping_count):
                # RSSI varies by distance/proximity
                rssi = self._rng.randint(-80, -30)  # dBm

                ping_time = transaction_time + timedelta(
                    minutes=self._rng.randint(
                        -15, 15
                    )  # Within 15 minutes of transaction
                )

                ble_record = {
                    "TraceId": self._generate_trace_id(),
                    "EventTS": ping_time,
                    "StoreID": store.ID,
                    "BeaconId": beacon_id,
                    "CustomerBLEId": customer.BLEId,
                    "RSSI": rssi,
                    "Zone": zone,
                }
                ble_records.append(ble_record)

        return ble_records

    def _generate_truck_movements(
        self, date: datetime, store_transactions: list[dict]
    ) -> list[dict]:
        """Generate truck movements based on store inventory needs."""
        truck_movements = []

        # Analyze store inventory needs
        store_demands = {}
        for transaction in store_transactions:
            if transaction["QtyDelta"] < 0:  # Sales
                store_id = transaction["StoreID"]
                if store_id not in store_demands:
                    store_demands[store_id] = 0
                store_demands[store_id] += abs(transaction["QtyDelta"])

        # Generate truck shipments for stores with high demand
        for store_id, demand in store_demands.items():
            if demand > 100:  # Threshold for triggering shipment
                next(s for s in self.stores if s.ID == store_id)

                # Find nearest DC (simplified - use first DC)
                dc = self.distribution_centers[0]

                # Check reorder needs
                reorder_list = self.inventory_flow_sim.check_reorder_needs(store_id)

                if reorder_list:
                    # Generate truck shipment
                    departure_time = date.replace(hour=6, minute=0)  # 6 AM departure
                    shipment_info = self.inventory_flow_sim.generate_truck_shipment(
                        dc.ID, store_id, reorder_list, departure_time
                    )

                    truck_record = {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": departure_time,
                        "TruckId": shipment_info["truck_id"],
                        "DCID": shipment_info["dc_id"],
                        "StoreID": shipment_info["store_id"],
                        "ShipmentId": shipment_info["shipment_id"],
                        "Status": shipment_info["status"].value,
                        "ETA": shipment_info["eta"],
                        "ETD": shipment_info["etd"],
                    }
                    truck_movements.append(truck_record)

                    # Track shipment for future delivery processing
                    self._active_shipments[shipment_info["shipment_id"]] = shipment_info

        return truck_movements

    def _process_truck_deliveries(
        self, date: datetime, truck_moves: list[dict]
    ) -> list[dict]:
        """Process truck deliveries and generate inventory transactions."""
        delivery_transactions = []

        # Check for shipments completing delivery
        completed_shipments = []
        for shipment_id, shipment_info in self._active_shipments.items():
            if shipment_info["etd"].date() <= date.date():
                # Complete delivery
                transactions = self.inventory_flow_sim.complete_delivery(shipment_id)

                for transaction in transactions:
                    # Get current balance from inventory simulator
                    key = (transaction["StoreID"], transaction["ProductID"])
                    balance = self.inventory_flow_sim._store_inventory.get(key, 0)

                    delivery_transactions.append(
                        {
                            "TraceId": self._generate_trace_id(),
                            "EventTS": transaction["EventTS"],
                            "StoreID": transaction["StoreID"],
                            "ProductID": transaction["ProductID"],
                            "QtyDelta": transaction["QtyDelta"],
                            "Reason": transaction["Reason"].value,
                            "Source": transaction["Source"],
                            "Balance": balance,
                        }
                    )

                completed_shipments.append(shipment_id)

        # Remove completed shipments
        for shipment_id in completed_shipments:
            del self._active_shipments[shipment_id]

        return delivery_transactions

    def _get_available_products_for_date(self, date: datetime) -> list[ProductMaster]:
        """Get products that have been launched by the given date."""
        return [
            product
            for product in self.products
            if product.LaunchDate.date() <= date.date()
        ]

    async def _export_hourly_facts(
        self, date: datetime, hour: int, hourly_facts: dict[str, list[dict]]
    ) -> None:
        """
        Export hourly facts to database.

        Args:
            date: Date being generated
            hour: Hour of day (0-23)
            hourly_facts: Dictionary mapping table names to record lists
        """
        # Deterministic insertion order to respect FKs
        preferred_order = [
            "receipts",
            "receipt_lines",
            "store_inventory_txn",
            "dc_inventory_txn",
            "truck_moves",
            "foot_traffic",
            "ble_pings",
            "online_orders",
        ]
        for fact_table in preferred_order:
            records = hourly_facts.get(fact_table) or []
            if not records:
                continue

            try:
                # Convert records to DataFrame for database insertion
                df = pd.DataFrame(records)
                await self._insert_hourly_to_db(self._session, fact_table, df, hour)
            except Exception as e:
                logger.error(
                    f"Failed to insert {fact_table} hour {hour} to database: {e}"
                )
                raise

    def _send_throttled_progress_update(
        self,
        day_counter: int,
        message: str,
        total_days: int,
        table_progress: dict[str, float] | None = None,
        tables_completed: list[str] | None = None,
        tables_in_progress: list[str] | None = None,
        tables_remaining: list[str] | None = None,
        tables_failed: list[str] | None = None,
        table_counts: dict[str, int] | None = None,
    ) -> None:
        """
        Send progress update to callback with throttling and ETA calculation.

        Updates are throttled to minimum 100ms intervals to ensure they're
        visible to users and don't overwhelm the API.

        Args:
            day_counter: Current day number
            message: Progress message
            total_days: Total number of days
            table_progress: Per-table progress percentages
            tables_completed: List of completed tables
            tables_in_progress: List of in-progress tables
            tables_remaining: List of not-started tables
            tables_failed: List of failed tables
        """
        if not self._progress_callback:
            logger.warning(f"Progress callback is None! Cannot send update: {message}")
            return

        thread_name = threading.current_thread().name
        logger.info(
            f"[PROGRESS][{thread_name}] Callback exists, sending update: {message[:50]}"
        )
        with self._progress_lock:
            current_time = time.time()
            progress = day_counter / total_days if total_days > 0 else 0.0

            # Throttle: Skip update if too soon (less than 50ms since last update)
            time_since_last = current_time - self._last_progress_update_time
            if time_since_last < 0.05:
                logger.debug(
                    f"[{thread_name}] Throttling progress update (too soon: {time_since_last * 1000:.1f}ms < 50ms)"
                )
                return

            # Update progress history for ETA calculation
            self._progress_history.append((current_time, progress))
            if len(self._progress_history) > 10:
                self._progress_history.pop(0)

            # Calculate ETA
            eta = self._calculate_eta(progress)

            # Calculate progress rate (for informational purposes)
            progress_rate = None
            if eta is not None and (1.0 - progress) > 0:
                progress_rate = (1.0 - progress) / eta

            # Determine current table (first in_progress table, if any)
            current_table = None
            if tables_in_progress and len(tables_in_progress) > 0:
                current_table = tables_in_progress[0]

            # Get hourly progress data from tracker
            hourly_progress_data = self.hourly_tracker.get_current_progress()

            callback_kwargs = {
                "table_progress": table_progress.copy() if table_progress else None,
                "current_table": current_table,
                "tables_completed": (tables_completed or []).copy(),
                "tables_failed": (tables_failed or []).copy(),
                "tables_in_progress": (tables_in_progress or []).copy()
                if tables_in_progress is not None
                else [],
                "tables_remaining": (tables_remaining or []).copy()
                if tables_remaining is not None
                else [],
                "estimated_seconds_remaining": eta,
                "progress_rate": progress_rate,
                "table_counts": table_counts,
                # NEW: Add hourly progress fields (note: current_day is passed as first positional arg, don't duplicate)
                "current_hour": hourly_progress_data.get("current_hour"),
                "hourly_progress": hourly_progress_data.get("per_table_progress"),
                "total_hours_completed": sum(
                    hourly_progress_data.get("completed_hours", {}).values()
                ),
            }

            filtered_kwargs = self._filter_progress_kwargs(callback_kwargs)

            # Send the progress update
            try:
                self._progress_callback(day_counter, message, **filtered_kwargs)
                logger.debug(
                    f"Progress update sent: {progress:.2%} (day {day_counter}/{total_days}) "
                    f"ETA: {eta:.0f}s, tables_in_progress: {tables_in_progress}"
                    if eta
                    else f"at {current_time:.3f}"
                )
            except TypeError:
                # Fallback for old callbacks that only accept 2 parameters
                try:
                    self._progress_callback(day_counter, message)
                    logger.debug(
                        f"Progress update sent (legacy): {progress:.2%} at {current_time:.3f}"
                    )
                except TypeError:
                    logger.debug(
                        "Legacy progress callback invocation failed; "
                        "suppressing TypeError to preserve generation flow"
                    )

            # Update last update timestamp
            self._last_progress_update_time = current_time

    def _filter_progress_kwargs(
        self, candidate_kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        """Return only the keyword arguments supported by the progress callback."""
        callback = self._progress_callback
        if not callback:
            return {}

        # Drop fields that have no value so legacy callbacks don't see noisy kwargs
        cleaned_kwargs = {
            key: value for key, value in candidate_kwargs.items() if value is not None
        }
        if not cleaned_kwargs:
            return {}

        try:
            signature = inspect.signature(callback)
        except (TypeError, ValueError):
            # If the signature can't be inspected, assume callback can handle everything we pass now
            return cleaned_kwargs

        if any(
            param.kind == inspect.Parameter.VAR_KEYWORD
            for param in signature.parameters.values()
        ):
            return cleaned_kwargs

        accepted_names: set[str] = set()
        for name, param in signature.parameters.items():
            if param.kind in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ):
                accepted_names.add(name)

        # Remove the first positional parameters since we pass them positionally (day, message)
        positional_count = 0
        for name, param in signature.parameters.items():
            if param.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                if positional_count < 2:
                    accepted_names.discard(name)
                    positional_count += 1
                continue
            break

        if not accepted_names:
            return {}

        return {
            key: value for key, value in cleaned_kwargs.items() if key in accepted_names
        }

    def _get_model_for_table(self, table_name: str) -> type[DeclarativeBase]:
        """
        Map fact table name to SQLAlchemy model.

        Args:
            table_name: Name of fact table (e.g., "receipts", "dc_inventory_txn")

        Returns:
            SQLAlchemy model class for the table

        Raises:
            ValueError: If table name is unknown
        """
        from retail_datagen.db.models.facts import (
            BLEPing,
            DCInventoryTransaction,
            FootTraffic,
            MarketingImpression,
            OnlineOrder,
            Receipt,
            ReceiptLine,
            StoreInventoryTransaction,
            TruckMove,
        )

        mapping = {
            "dc_inventory_txn": DCInventoryTransaction,
            "truck_moves": TruckMove,
            "store_inventory_txn": StoreInventoryTransaction,
            "receipts": Receipt,
            "receipt_lines": ReceiptLine,
            "foot_traffic": FootTraffic,
            "ble_pings": BLEPing,
            "marketing": MarketingImpression,
            "online_orders": OnlineOrder,
        }

        if table_name not in mapping:
            raise ValueError(f"Unknown table: {table_name}")

        return mapping[table_name]

    def _map_field_names_for_db(self, table_name: str, record: dict) -> dict:
        """
        Map generator field names (PascalCase) to database field names (snake_case).

        Args:
            table_name: Name of fact table
            record: Record dict with generator field names

        Returns:
            New dict with database-compatible field names

        Notes:
            - TraceId field is skipped (not in DB models)
            - balance field (DC/Store inventory) not in generator output - will be NULL
            - Some generator fields don't map to DB (e.g., ReceiptId string, OrderId string)
            - Receipts table: discount_amount field not in generator - will default to 0.0
        """
        # Define field mappings for each table
        # Generator fields -> Database fields
        common_mappings = {
            # Skip TraceId - not in DB models
            "EventTS": "event_ts",
        }

        table_specific_mappings = {
            "receipts": {
                **common_mappings,
                "StoreID": "store_id",
                "CustomerID": "customer_id",
                # Store external receipt id for linkage with receipt_lines
                "ReceiptId": "receipt_id_ext",
                "Tax": "tax_amount",
                "Total": "total_amount",
                "TenderType": "payment_method",
                # Note: discount_amount field in DB will default to 0.0 (not in generator)
            },
            "receipt_lines": {
                **common_mappings,
                # ReceiptId will be resolved to numeric FK by lookup before insert
                "ProductID": "product_id",
                "Qty": "quantity",
                "UnitPrice": "unit_price",
                "ExtPrice": "line_total",
                # Note: Line and PromoCode fields in generator don't exist in DB model
            },
            "dc_inventory_txn": {
                **common_mappings,
                "DCID": "dc_id",
                "ProductID": "product_id",
                "QtyDelta": "quantity",
                "Reason": "txn_type",
                "Balance": "balance",
            },
            "truck_moves": {
                **common_mappings,
                "TruckId": "truck_id",
                "DCID": "dc_id",
                "StoreID": "store_id",
                "ProductID": "product_id",
                "Status": "status",
                "ShipmentId": "shipment_id",
                "ETA": "eta",
                "ETD": "etd",
            },
            "store_inventory_txn": {
                **common_mappings,
                "StoreID": "store_id",
                "ProductID": "product_id",
                "QtyDelta": "quantity",
                "Reason": "txn_type",
                "Balance": "balance",
                # Note: Source field in generator doesn't exist in DB model (will be ignored)
            },
            "foot_traffic": {
                **common_mappings,
                "StoreID": "store_id",
                "SensorId": "sensor_id",
                "Zone": "zone",
                "Dwell": "dwell_seconds",
                "Count": "count",
            },
            "ble_pings": {
                **common_mappings,
                "StoreID": "store_id",
                "CustomerBLEId": "customer_ble_id",
                "BeaconId": "beacon_id",
                "RSSI": "rssi",
                "Zone": "zone",
                # Note: customer_id field in DB is nullable (requires lookup from BLE ID)
            },
            "marketing": {
                **common_mappings,
                "CampaignId": "campaign_id",
                "CreativeId": "creative_id",
                "ImpressionId": "impression_id_ext",
                "CustomerAdId": "customer_ad_id",
                "Channel": "channel",
                "Device": "device",
                "Cost": "cost",
                # Note: customer_id field in DB is nullable (requires lookup from ad ID)
            },
            "online_orders": {
                **common_mappings,
                "CustomerID": "customer_id",
                "ProductID": "product_id",
                "Quantity": "quantity",
                "Total": "total_amount",
                "FulfillmentMode": "fulfillment_status",
                # Note: OrderId, FulfillmentNodeType, FulfillmentNodeID, Subtotal, Tax, TenderType
                # fields from generator don't exist in DB model (will be ignored)
            },
        }

        mapping = table_specific_mappings.get(table_name, common_mappings)
        mapped_record = {}

        for gen_field, value in record.items():
            # Skip TraceId field entirely (not in any DB model)
            if gen_field == "TraceId":
                continue

            db_field = mapping.get(gen_field, gen_field.lower())
            mapped_record[db_field] = value

        return mapped_record

    async def _insert_hourly_to_db(
        self,
        session: AsyncSession,
        table_name: str,
        df: pd.DataFrame,
        hour: int,
        batch_size: int = 10000,
        commit_every_batches: int = 1,
    ) -> None:
        """
        Insert hourly data batch into SQLite.

        Args:
            session: Database session for facts.db
            table_name: Name of fact table (e.g., "receipts")
            df: DataFrame with hourly data
            hour: Hour index (0-23)
            batch_size: Rows per batch insert (default: 10000)

        Note:
            Commits once after all batches are inserted for performance.
            Individual batches are not committed to minimize I/O overhead.
            Field names are automatically mapped from PascalCase to snake_case.
        """
        if df.empty:
            logger.debug(f"No data to insert for {table_name} hour {hour}")
            return

        # Map table name to model
        try:
            model_class = self._get_model_for_table(table_name)
        except ValueError as e:
            logger.error(f"Cannot insert data: {e}")
            return

        # Convert DataFrame to records
        records = df.to_dict("records")

        # Special handling: link receipt_lines to receipts by external id
        if table_name == "receipt_lines":
            try:
                # Collect unique external ids
                ext_ids = list(
                    {r.get("ReceiptId") for r in records if r.get("ReceiptId")}
                )
                # Build map from external id -> numeric PK
                receipts_model = self._get_model_for_table("receipts")
                from sqlalchemy import select

                rows = (
                    await session.execute(
                        select(
                            receipts_model.receipt_id, receipts_model.receipt_id_ext
                        ).where(receipts_model.receipt_id_ext.in_(ext_ids))
                    )
                ).all()
                id_map = {ext: pk for (pk, ext) in rows}

                mapped_records = []
                for record in records:
                    mapped = self._map_field_names_for_db(table_name, record)
                    ext = record.get("ReceiptId")
                    pk = id_map.get(ext)
                    if not pk:
                        # No matching receipt yet; skip this line to preserve FK integrity
                        logger.debug(
                            f"Skipping receipt_line with unknown ReceiptId={ext}"
                        )
                        continue
                    mapped["receipt_id"] = int(pk)
                    mapped_records.append(mapped)
            except Exception as e:
                logger.error(f"Failed to resolve receipt_ids for receipt_lines: {e}")
                return
        else:
            # Default mapping path
            mapped_records = [
                self._map_field_names_for_db(table_name, record) for record in records
            ]

        # Batch insert using bulk operations
        try:
            total_hour_rows = len(mapped_records)
            batch_index = 0
            for i in range(0, total_hour_rows, batch_size):
                batch = mapped_records[i : i + batch_size]

                # Use bulk insert for performance
                # Note: This doesn't populate auto-increment IDs back to Python objects
                await session.execute(model_class.__table__.insert(), batch)
                # Flush to DB
                await session.flush()

                logger.debug(
                    f"Inserted batch {i // batch_size + 1} for {table_name} hour {hour}: "
                    f"{len(batch)} rows"
                )

                # Live per-table progress and counts (master-style tiles)
                try:
                    # Update cumulative DB-written counts for this table
                    if not hasattr(self, "_table_insert_counts"):
                        self._table_insert_counts = {}
                    self._table_insert_counts[table_name] = (
                        self._table_insert_counts.get(table_name, 0) + len(batch)
                    )

                    # Compute fractional progress across the whole range using hourly tracker state
                    tracker_state = self.hourly_tracker.get_current_progress()
                    completed_hours = tracker_state.get("completed_hours", {}).get(
                        table_name, 0
                    )
                    total_days = tracker_state.get("total_days") or 1
                    total_hours_expected = max(1, total_days * 24)
                    # Partial hour progress within this hour based on batch position
                    partial_hour = (
                        (i + len(batch)) / total_hour_rows
                        if total_hour_rows > 0
                        else 1.0
                    )
                    per_table_fraction = min(
                        1.0, (completed_hours + partial_hour) / total_hours_expected
                    )

                    # Emit per-table progress callback (router merges counts and recomputes overall)
                    self._emit_table_progress(
                        table_name,
                        per_table_fraction,
                        f"Writing {table_name.replace('_', ' ')} ({self._table_insert_counts[table_name]:,})",
                        {table_name: self._table_insert_counts[table_name]},
                    )
                except Exception as _:
                    # Non-fatal: progress updates should not break inserts
                    pass

                # Periodic commit for durability
                batch_index += 1
                if commit_every_batches > 0 and (
                    batch_index % commit_every_batches == 0
                ):
                    try:
                        await session.commit()
                        logger.debug(
                            f"Committed {len(batch)} rows for {table_name} hour {hour}, batch {batch_index}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Interim commit failed for {table_name} hour {hour}: {e}"
                        )

            # Commit once after all batches (not per batch for performance)
            await session.commit()
            logger.info(
                f"Inserted {len(mapped_records)} rows for {table_name} hour {hour}"
            )

            # Optional verification: compare DB count delta (throttled by hour)
            try:
                if hour % 6 == 0:  # verify every 6 hours to reduce overhead
                    from sqlalchemy import func, select

                    total_db = (
                        await session.execute(
                            select(func.count()).select_from(model_class)
                        )
                    ).scalar() or 0
                    prev = 0
                    if not hasattr(self, "_fact_db_counts"):
                        self._fact_db_counts = {}
                    prev = int(self._fact_db_counts.get(table_name, 0))
                    added = int(total_db) - prev
                    self._fact_db_counts[table_name] = int(total_db)
                    logger.info(
                        f"{table_name} verification (hour {hour}): added={added}, expected={len(mapped_records)}, db_total={total_db}"
                    )
            except Exception as e:
                logger.debug(f"Verification skipped for {table_name} hour {hour}: {e}")

        except Exception as e:
            logger.error(
                f"Failed to insert hourly data for {table_name} hour {hour}: {e}"
            )
            await session.rollback()
            raise

    async def _update_watermarks_after_generation(
        self,
        start_date: datetime,
        end_date: datetime,
        active_tables: list[str],
    ) -> None:
        """
        Update watermarks for all generated fact tables.

        Marks the generated date range as unpublished so streaming knows
        where to start publishing from.

        Args:
            start_date: Start of generated data range
            end_date: End of generated data range
            active_tables: List of table names that were generated

        Note:
            Called after generation completes successfully.
        """
        if not self._session:
            return

        from retail_datagen.db.purge import mark_data_unpublished

        # Map generator table names to database table names
        table_name_mapping = {
            "dc_inventory_txn": "fact_dc_inventory_txn",
            "truck_moves": "fact_truck_moves",
            "store_inventory_txn": "fact_store_inventory_txn",
            "receipts": "fact_receipts",
            "receipt_lines": "fact_receipt_lines",
            "foot_traffic": "fact_foot_traffic",
            "ble_pings": "fact_ble_pings",
            "marketing": "fact_marketing",
            "online_orders": "fact_online_orders",
        }

        for table_name in active_tables:
            db_table_name = table_name_mapping.get(table_name)
            if db_table_name:
                try:
                    await mark_data_unpublished(
                        self._session, db_table_name, start_date, end_date
                    )
                    logger.debug(
                        f"Marked {db_table_name} as unpublished: "
                        f"{start_date} to {end_date}"
                    )
                except Exception as e:
                    logger.error(f"Failed to update watermark for {db_table_name}: {e}")
                    # Don't fail generation if watermark update fails
                    continue

    def _generate_trace_id(self) -> str:
        """Generate unique trace ID."""
        trace_id = f"TRC{self._trace_counter:010d}"
        self._trace_counter += 1
        return trace_id

    def _randomize_time_within_day(self, date: datetime) -> datetime:
        """Generate random time within the given day."""
        hour = self._rng.randint(0, 23)
        minute = self._rng.randint(0, 59)
        second = self._rng.randint(0, 59)
        return date.replace(hour=hour, minute=minute, second=second)

    def _randomize_time_within_hour(self, hour_datetime: datetime) -> datetime:
        """Generate random time within the given hour."""
        minute = self._rng.randint(0, 59)
        second = self._rng.randint(0, 59)
        return hour_datetime.replace(minute=minute, second=second)

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
def generate_historical_facts(
    config_path: str, start_date: datetime, end_date: datetime
) -> FactDataGenerator:
    """
    Convenience function to generate historical fact data from config file.

    Args:
        config_path: Path to configuration JSON file
        start_date: Start date for historical data
        end_date: End date for historical data

    Returns:
        FactDataGenerator instance with generated data
    """
    from retail_datagen.config.models import RetailConfig

    config = RetailConfig.from_file(config_path)
    # NOTE: This convenience function is legacy; in API flow we construct with an AsyncSession.
    # Here we construct a temporary generator without DB session which is not supported in DB mode.
    # Users should use the FastAPI endpoints instead.
    raise RuntimeError("Use API endpoints for historical generation in SQLite mode")

    # Unreachable in DB mode
