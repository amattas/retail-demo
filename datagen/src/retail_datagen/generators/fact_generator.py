"""
Historical fact data generation engine for retail data generator.

This module implements the FactDataGenerator class that creates realistic
retail transaction data for all fact tables with proper temporal patterns,
business logic coordination, and DuckDB-backed storage.
"""
from __future__ import annotations

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

import numpy as np
import pandas as pd

from retail_datagen.generators.online_order_generator import (
    generate_online_orders_with_lifecycle,
)
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
    Truck,
)

from ..config.models import RetailConfig
from ..shared.customer_geography import GeographyAssigner, StoreSelector
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
    """Deprecated: CSV-based master specs removed in DuckDB-only mode."""

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
        except Exception:
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

    def load_master_data_from_duckdb(self) -> None:
        """Load master data from DuckDB and initialize simulators."""
        from retail_datagen.db.duck_master_reader import read_all_masters

        print("Loading master data from DuckDB for fact generation...")
        (
            self.geographies,
            self.stores,
            self.distribution_centers,
            self.customers,
            self.products,
            self.trucks,
        ) = read_all_masters()

        # Initialize simulators with loaded data
        self.customer_journey_sim = CustomerJourneySimulator(
            self.customers, self.products, self.stores, self.config.seed + 1000
        )

        self.inventory_flow_sim = InventoryFlowSimulator(
            self.distribution_centers,
            self.stores,
            self.products,
            self.config.seed + 2000,
            trucks=getattr(self, "trucks", None),
        )

        self.marketing_campaign_sim = MarketingCampaignSimulator(
            self.customers, self.config.seed + 3000, self.config.marketing_cost
        )

        # Initialize customer geography and store selector
        print("Assigning customer geographies and store affinities...")
        geography_assigner = GeographyAssigner(
            self.customers, self.stores, self.geographies, self.config.seed + 4000
        )
        customer_geographies = geography_assigner.assign_geographies()
        self.store_selector = StoreSelector(
            customer_geographies, self.stores, self.config.seed + 5000
        )

        # Build customer pools per store for efficient selection
        print("Building customer pools for each store...")
        self._build_store_customer_pools(customer_geographies)

        print(
            "Loaded master data (DuckDB): "
            f"{len(self.geographies)} geographies, "
            f"{len(self.stores)} stores, "
            f"{len(self.distribution_centers)} DCs, "
            f"{len(self.customers)} customers, "
            f"{len(self.products)} products, "
            f"{len(getattr(self, 'trucks', []))} trucks"
        )

    def _master_table_specs(self) -> list[MasterTableSpec]:
        """Deprecated: No longer used (DuckDB-only)."""
        return []

    def _load_master_table(self, master_path: Path, spec: MasterTableSpec) -> list[Any]:
        """Deprecated: CSV master load removed."""
        return []

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

    # Removed legacy CSV loader: DuckDB-only path is used for master data

    async def load_master_data_from_db(self) -> None:
        """Deprecated SQLite path removed; DuckDB-only runtime."""
        raise RuntimeError("SQLite master load is not supported. Use DuckDB loader.")

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
        from retail_datagen.db.models.master import (
            Truck as TruckModel,
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
                    tax_rate=Decimal(str(s.tax_rate)) if getattr(s, "tax_rate", None) is not None else None,
                    volume_class=s.volume_class,
                    store_format=s.store_format,
                    operating_hours=s.operating_hours,
                    daily_traffic_multiplier=Decimal(str(s.daily_traffic_multiplier)) if s.daily_traffic_multiplier is not None else None,
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
                        taxability=getattr(p, "taxability", None) or None,
                        Tags=getattr(p, "tags", None),
                    )
                )

            # Trucks
            trucks = (await session.execute(select(TruckModel))).scalars().all()
            self.trucks = [
                Truck(
                    ID=t.truck_id,
                    LicensePlate=t.license_plate,
                    Refrigeration=bool(t.refrigeration),
                    DCID=t.dc_id,
                )
                for t in trucks
            ]

        # Initialize simulators with loaded data
        self.customer_journey_sim = CustomerJourneySimulator(
            self.customers, self.products, self.stores, self.config.seed + 1000
        )

        self.inventory_flow_sim = InventoryFlowSimulator(
            self.distribution_centers,
            self.stores,
            self.products,
            self.config.seed + 2000,
            trucks=getattr(self, "trucks", None),
        )

        self.marketing_campaign_sim = MarketingCampaignSimulator(
            self.customers, self.config.seed + 3000, self.config.marketing_cost
        )

        # Initialize customer geography and store selector
        print("Assigning customer geographies and store affinities...")
        geography_assigner = GeographyAssigner(
            self.customers, self.stores, self.geographies, self.config.seed + 4000
        )
        customer_geographies = geography_assigner.assign_geographies()
        self.store_selector = StoreSelector(
            customer_geographies, self.stores, self.config.seed + 5000
        )

        # Build customer pools per store for efficient selection
        print("Building customer pools for each store...")
        self._build_store_customer_pools(customer_geographies)

        print(
            "Loaded master data (legacy SQLite): "
            f"{len(self.geographies)} geographies, "
            f"{len(self.stores)} stores, "
            f"{len(self.distribution_centers)} DCs, "
            f"{len(self.customers)} customers, "
            f"{len(self.products)} products"
        )

    def _build_store_customer_pools(self, customer_geographies: dict) -> None:
        """
        Build customer pools for each store for efficient weighted selection.

        For each store, create a list of (customer, weight) tuples where weight
        represents the probability of that customer shopping at that store.

        Args:
            customer_geographies: Dictionary mapping customer_id to CustomerGeography
        """
        # Initialize pools for all stores
        for store in self.stores:
            self._store_customer_pools[store.ID] = []

        # Build pools by calculating weights for each customer
        store_ids = [store.ID for store in self.stores]

        for customer in self.customers:
            customer_geo = customer_geographies.get(customer.ID)
            if not customer_geo:
                # If no geography, give equal weight to all stores
                equal_weight = 1.0 / len(self.stores)
                for store in self.stores:
                    self._store_customer_pools[store.ID].append((customer, equal_weight))
                continue

            # Get store selection weights for this customer
            store_weights = customer_geo.get_store_selection_weights(store_ids)

            # Add customer to each store's pool with appropriate weight
            for store_id, weight in store_weights.items():
                if weight > 0:  # Only add if there's a non-zero probability
                    self._store_customer_pools[store_id].append((customer, weight))

        # Build precomputed sampling arrays (customers list + weights) per store
        for store_id, pool in self._store_customer_pools.items():
            if not pool:
                # Fallback to global customers uniformly if pool is empty
                self._store_customer_sampling[store_id] = (
                    self.customers[:],
                    [1.0] * len(self.customers),
                )
                continue

            customers_list = [c for c, _ in pool]
            weights_list = [w for _, w in pool]
            total_w = sum(weights_list)
            if total_w <= 0:
                # Equal weights if all zero
                weights_list = [1.0] * len(customers_list)
            else:
                # Normalize once to avoid hot-loop normalization
                weights_list = [w / total_w for w in weights_list]

            self._store_customer_sampling[store_id] = (customers_list, weights_list)
        # Also cache NumPy-ready arrays for fast vector sampling
        self._store_customer_sampling_np: dict[int, tuple[np.ndarray, np.ndarray]] = {}
        for sid, (clist, wlist) in self._store_customer_sampling.items():
            try:
                idx = np.arange(len(clist), dtype=np.int32)
                p = np.asarray(wlist, dtype=np.float64)
                # Ensure probabilities sum to 1
                s = p.sum()
                if s > 0:
                    p = p / s
                else:
                    p.fill(1.0 / len(p))
                self._store_customer_sampling_np[sid] = (idx, p)
            except Exception:
                # Fallback will use Python choices
                pass

        # Log summary statistics
        pool_sizes = [len(pool) for pool in self._store_customer_pools.values()]
        avg_pool_size = sum(pool_sizes) / len(pool_sizes) if pool_sizes else 0
        min_pool_size = min(pool_sizes) if pool_sizes else 0
        max_pool_size = max(pool_sizes) if pool_sizes else 0

        logger.info(f"Built customer pools for {len(self.stores)} stores")
        logger.info(f"  Average pool size: {avg_pool_size:.0f} customers per store")
        logger.info(f"  Min pool size: {min_pool_size}")
        logger.info(f"  Max pool size: {max_pool_size}")

    async def generate_historical_data(
        self, start_date: datetime, end_date: datetime
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
        generation_start_time = datetime.now()
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
        except Exception:
            pass

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
            except Exception:
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
            except Exception:
                pass

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

        # Update watermarks if database session provided (SQLite-only path)
        if self._session and not self._use_duckdb:
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
                    await self._insert_hourly_to_db(
                        self._session,
                        "dc_inventory_txn",
                        dc_transactions,
                        hour=0,
                        commit_every_batches=0,
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
            marketing_boost = 1.0
            try:
                marketing_boost = self._compute_marketing_multiplier(date)
            except Exception:
                pass
            marketing_records = self._generate_marketing_activity(date, marketing_boost)
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
                    await self._insert_hourly_to_db(
                        self._session, "marketing", marketing_records, hour=0, commit_every_batches=0
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

            # Early heartbeat for this hour before heavy generation
            try:
                if self._progress_callback:
                    progress_state = self.hourly_tracker.get_current_progress()
                    self._send_throttled_progress_update(
                        day_counter=day_index,
                        message=(
                            f"Preparing hour {hour_idx + 1}/24 for {date.strftime('%Y-%m-%d')}"
                        ),
                        total_days=total_days,
                        table_progress=progress_state.get("per_table_progress", {}),
                        tables_in_progress=progress_state.get("tables_in_progress", []),
                    )
            except Exception:
                pass

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
        # This creates initial shipments in SCHEDULED status
        if "truck_moves" in active_tables:
            base_store_txn = daily_facts.get("store_inventory_txn", [])
            truck_movements = self._generate_truck_movements(date, base_store_txn)
            daily_facts["truck_moves"].extend(truck_movements)

            # Process all active shipments and generate status progression throughout the day
            truck_lifecycle_records, dc_outbound_txn, store_inbound_txn = (
                self._process_truck_lifecycle(date)
            )

            # Add truck status progression records
            daily_facts["truck_moves"].extend(truck_lifecycle_records)

            # Add DC outbound transactions (when trucks are loaded)
            if "dc_inventory_txn" in active_tables and dc_outbound_txn:
                daily_facts["dc_inventory_txn"].extend(dc_outbound_txn)
                # Insert these lifecycle DC transactions immediately (daily batch)
                try:
                    await self._insert_hourly_to_db(
                        self._session,
                        "dc_inventory_txn",
                        dc_outbound_txn,
                        hour=0,
                        commit_every_batches=0,
                    )
                    # Mark hours complete for this table (lifecycle-generated)
                    for hour in range(24):
                        self.hourly_tracker.update_hourly_progress(
                            "dc_inventory_txn", day_index, hour, total_days
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to insert lifecycle dc_inventory_txn for {date.strftime('%Y-%m-%d')}: {e}"
                    )

            # Add store inbound transactions (when trucks are unloaded)
            if "store_inventory_txn" in active_tables and store_inbound_txn:
                daily_facts["store_inventory_txn"].extend(store_inbound_txn)
                # Insert these lifecycle store transactions immediately (daily batch)
                try:
                    await self._insert_hourly_to_db(
                        self._session,
                        "store_inventory_txn",
                        store_inbound_txn,
                        hour=0,
                        commit_every_batches=0,
                    )
                    # Mark hours complete for this table (lifecycle-generated)
                    for hour in range(24):
                        self.hourly_tracker.update_hourly_progress(
                            "store_inventory_txn", day_index, hour, total_days
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to insert lifecycle store_inventory_txn for {date.strftime('%Y-%m-%d')}: {e}"
                    )

            # Write all truck_moves records (including lifecycle progression)
            all_truck_moves = daily_facts.get("truck_moves", [])
            if all_truck_moves:
                try:
                    await self._insert_hourly_to_db(
                        self._session,
                        "truck_moves",
                        all_truck_moves,
                        hour=0,
                        commit_every_batches=0,
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
            # Update progress for this daily-generated table (treated as complete across hours)
            for hour in range(24):
                self.hourly_tracker.update_hourly_progress(
                    "truck_inventory", day_index, hour, total_days
                )

        # 5. Legacy delivery processing - now handled by _process_truck_lifecycle
        # Kept for backward compatibility but will be empty since lifecycle handles it
        if "store_inventory_txn" in active_tables:
            base_truck_moves = daily_facts.get("truck_moves", [])
            delivery_transactions = self._process_truck_deliveries(
                date, base_truck_moves
            )
            if delivery_transactions:
                # Only add if not already added by lifecycle processing
                # This prevents double-counting
                logger.debug(f"Legacy delivery processing added {len(delivery_transactions)} transactions")
                # Skip adding these since _process_truck_lifecycle handles it
                pass

        # 6. Generate online orders and integrate inventory effects
        if "online_orders" in active_tables:
            online_orders, online_store_txn, online_dc_txn, online_order_lines = (
                self._generate_online_orders(date)
            )
            daily_facts["online_orders"].extend(online_orders)
            # First write online order headers (so lines can resolve order_id)
            if online_orders:
                try:
                    await self._insert_hourly_to_db(
                        self._session,
                        "online_orders",
                        online_orders,
                        hour=0,
                        commit_every_batches=0,
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
            # Then write online order lines (daily batch)
            if online_order_lines:
                try:
                    await self._insert_hourly_to_db(
                        self._session,
                        "online_order_lines",
                        online_order_lines,
                        hour=0,
                        commit_every_batches=0,
                    )
                    # Update progress for line items (treated as complete across hours)
                    for hour in range(24):
                        self.hourly_tracker.update_hourly_progress(
                            "online_order_lines", day_index, hour, total_days
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to insert online_order_lines for {date.strftime('%Y-%m-%d')}: {e}"
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

        # 8. Generate return receipts and inventory effects (baseline + holiday spikes)
        try:
            await self._generate_and_insert_returns(date, active_tables)
        except Exception as e:
            logger.warning(f"Return generation failed for {date.strftime('%Y-%m-%d')}: {e}")

        return daily_facts

    def _generate_online_orders(
        self, date: datetime
    ) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
        """Generate online orders for the given date with complete lifecycle and corresponding inventory effects.

        Delegates to generate_online_orders_with_lifecycle for full implementation including:
        - Multi-line orders (1-5 items per order via basket generation)
        - Status progression (created -> picked -> shipped -> delivered)
        - Proper tax calculation based on fulfillment location
        - Realistic tender type distribution

        Returns:
            (orders, store_inventory_txn, dc_inventory_txn, order_lines)
        """
        # Basket adjuster applies the same holiday overlay used for POS
        def _adjuster(ts: datetime, basket):
            self._apply_holiday_overlay_to_basket(ts, basket)

        return generate_online_orders_with_lifecycle(
            date=date,
            config=self.config,
            customers=self.customers,
            geographies=self.geographies,
            stores=self.stores,
            distribution_centers=self.distribution_centers,
            customer_journey_sim=self.customer_journey_sim,
            inventory_flow_sim=self.inventory_flow_sim,
            temporal_patterns=self.temporal_patterns,
            rng=self._rng,
            generate_trace_id_func=self._generate_trace_id,
            basket_adjuster=_adjuster,
        )

    def _generate_dc_inventory_transactions(
        self, date: datetime, multiplier: float
    ) -> list[dict]:
        """Generate DC inventory transactions for a day."""
        transactions = []

        # Each DC receives shipments
        for dc in self.distribution_centers:
            dc_transactions = self.inventory_flow_sim.simulate_dc_receiving(dc.ID, date)

            for transaction in dc_transactions:
                # Get current balance after this transaction
                balance = self.inventory_flow_sim.get_dc_balance(
                    transaction["DCID"], transaction["ProductID"]
                )

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

                # Resolve customer_id from AdId (5% of the time - CRM join / authenticated users)
                customer_id = None
                if self._rng.random() < 0.05:
                    # Find customer with this AdId
                    customer_ad_id = impression["CustomerAdId"]
                    for customer in self.customers:
                        if customer.AdId == customer_ad_id:
                            customer_id = customer.ID
                            break

                marketing_records.append(
                    {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": self._randomize_time_within_day(date),
                        "Channel": impression["Channel"].value,
                        "CampaignId": impression["CampaignId"],
                        "CreativeId": impression["CreativeId"],
                        "CustomerAdId": impression["CustomerAdId"],
                        "CustomerId": customer_id,  # 5% resolution
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

        # Holiday closure: Christmas Day closed (no activity)
        if hour_datetime.month == 12 and hour_datetime.day == 25:
            return hour_data

        # Calculate expected customers for this hour
        # NOTE: customers_per_day is configured PER STORE, not total across all stores
        base_customers_per_hour = self.config.volume.customers_per_day / 24

        # Apply store profile multiplier for realistic variability
        store_multiplier = float(getattr(store, 'daily_traffic_multiplier', Decimal("1.0")))
        expected_customers = int(base_customers_per_hour * multiplier * store_multiplier)

        # Generate foot traffic (will be calibrated to receipts with conversion rates)
        # Pass expected_customers (receipt count) to calculate realistic foot traffic
        foot_traffic_records = self._generate_foot_traffic(
            store, hour_datetime, expected_customers
        )
        hour_data["foot_traffic"].extend(foot_traffic_records)

        if expected_customers == 0:
            return hour_data

        # Generate customer transactions
        # Use precomputed per-store sampling to select customers for this hour in bulk
        if expected_customers > 0:
            if store.ID in self._store_customer_sampling_np:
                # Vectorized sampling via NumPy over index array, then map back to customers
                idx_arr, p = self._store_customer_sampling_np[store.ID]
                if len(idx_arr) > 0:
                    chosen_idx = self._np_rng.choice(
                        idx_arr, size=expected_customers, replace=True, p=p
                    )
                    clist = self._store_customer_sampling[store.ID][0]
                    selected_customers = [clist[i] for i in chosen_idx]
                else:
                    selected_customers = [self._rng.choice(self.customers) for _ in range(expected_customers)]
            elif store.ID in self._store_customer_sampling:
                customers_list, weights_list = self._store_customer_sampling[store.ID]
                selected_customers = self._rng.choices(
                    customers_list, weights=weights_list, k=expected_customers
                )
            else:
                selected_customers = [self._rng.choice(self.customers) for _ in range(expected_customers)]

            for customer in selected_customers:
                # Generate shopping basket (pass store for format-based adjustments)
                basket = self.customer_journey_sim.generate_shopping_basket(customer.ID, store=store)
                # Apply holiday overlay to adjust basket composition/quantities
                try:
                    self._apply_holiday_overlay_to_basket(hour_datetime, basket)
                except Exception:
                    pass

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

    # ---------------- Holiday Overlay Helpers -----------------
    def _thanksgiving_date(self, year: int) -> datetime:
        # 4th Thursday in November
        from datetime import timedelta
        d = datetime(year, 11, 1)
        # weekday(): Mon=0..Sun=6; Thursday=3
        first_thu = d + timedelta(days=(3 - d.weekday() + 7) % 7)
        return first_thu + timedelta(weeks=3)

    def _memorial_day(self, year: int) -> datetime:
        # Last Monday of May
        from datetime import timedelta
        d = datetime(year, 5, 31)
        return d - timedelta(days=(d.weekday() - 0) % 7)

    def _labor_day(self, year: int) -> datetime:
        # First Monday of September
        from datetime import timedelta
        d = datetime(year, 9, 1)
        return d + timedelta(days=(0 - d.weekday()) % 7)

    def _in_window(self, date: datetime, center: datetime, lead_days: int, lag_days: int) -> bool:
        from datetime import timedelta
        start = center - timedelta(days=lead_days)
        end = center + timedelta(days=lag_days)
        return start.date() <= date.date() <= end.date()

    def _product_has_keywords(self, product: ProductMaster, keywords: list[str]) -> bool:
        t = (getattr(product, 'Tags', None) or getattr(product, 'tags', None) or '')
        hay = ' '.join([
            str(product.ProductName),
            str(product.Department),
            str(product.Category),
            str(product.Subcategory),
            str(t or ''),
        ]).lower()
        return any(k in hay for k in keywords)

    def _get_product_multiplier(self, date: datetime, product: ProductMaster) -> float:
        year = date.year
        tg = self._thanksgiving_date(year)
        bf = tg.replace(day=tg.day) + timedelta(days=1)
        xmas = datetime(year, 12, 25)
        # Thanksgiving lead core foods
        if self._in_window(date, tg, 10, 1):
            core = [
                'thanksgiving','turkey','stuffing','cranberry','cranberries','pie','pumpkin','rolls',
                'casserole','green bean','cream of mushroom','fried onion','gravy','yams','sweet potato','baking'
            ]
            baking = ['baking','flour','sugar','spice','cinnamon','nutmeg','clove']
            if self._product_has_keywords(product, core):
                return 3.5
            if self._product_has_keywords(product, baking):
                return 1.8
            # general grocery light bump
            if self._product_has_keywords(product, ['grocery','produce','meat','beverage','snack']):
                return 1.3
        # Black Friday (non-food)
        if date.date() == bf.date():
            if self._product_has_keywords(product, ['electronics','tv','laptop','headphone','gaming','appliance']):
                return 5.0
            if self._product_has_keywords(product, ['toy','lego','action figure','doll']):
                return 3.0
            if self._product_has_keywords(product, ['home','home goods','cookware','small appliance']):
                return 2.2
            if self._product_has_keywords(product, ['apparel','clothing','shoe','footwear']):
                return 2.3
        # Christmas ramp
        if self._in_window(date, xmas, 14, 0):
            if self._product_has_keywords(product, ['ham','roast','cookie','baking','candy','cider','eggnog','hot chocolate','hot beverage']):
                return 1.8
            if self._product_has_keywords(product, ['electronics','toy','apparel','home']):
                return 1.6
        # Grill-out windows
        mem = self._memorial_day(year)
        lab = self._labor_day(year)
        jul4 = datetime(year, 7, 4)
        grill_tags = ['grill','hot dog','hotdog','sausage','burger','ground beef','steak','chicken breast','bun','buns','ketchup','mustard','relish','bbq sauce','charcoal','chips','soda','ice']
        if self._in_window(date, mem, 2, 2) or self._in_window(date, lab, 2, 2) or self._in_window(date, jul4, 2, 2):
            if self._product_has_keywords(product, grill_tags):
                return 2.5
        return 1.0

    def _apply_holiday_overlay_to_basket(self, date: datetime, basket) -> None:
        """Adjust basket in-place based on holiday overlay (qty boosts + occasional extra lines)."""
        from decimal import Decimal
        if not getattr(basket, 'items', None):
            return
        # Increase quantity for existing targeted items
        new_items = []
        targeted_candidates = []
        for product, qty in basket.items:
            m = self._get_product_multiplier(date, product)
            if m > 1.0:
                # Rough qty bump: +1 for ~each full +0.8 in multiplier
                bump = 0
                if m >= 3.0:
                    bump = self._rng.choice([1, 2])
                elif m >= 1.5:
                    bump = self._rng.choice([0, 1])
                qty = max(1, qty + bump)
                targeted_candidates.append(product)
            new_items.append((product, qty))

        # Basket size bump for some holidays
        basket_mult = 1.0
        year = date.year
        tg = self._thanksgiving_date(year)
        xmas = datetime(year, 12, 25)
        if self._in_window(date, tg, 10, 1):
            basket_mult = 1.2
        elif self._in_window(date, xmas, 2, 0):
            basket_mult = 1.2

        extra = 0
        if basket_mult > 1.0:
            base_count = sum(q for _, q in new_items)
            extra = max(0, int(base_count * (basket_mult - 1.0) * 0.5))

        # Add a few extra targeted items if needed
        if extra > 0:
            # Choose from strong targets first; otherwise random
            pool = targeted_candidates or [p for p, _ in new_items]
            for _ in range(extra):
                product = self._rng.choice(pool)
                new_items.append((product, 1))

        basket.items = new_items

    def _compute_marketing_multiplier(self, date: datetime) -> float:
        # Conservative boosts; configurable later
        year = date.year
        tg = self._thanksgiving_date(year)
        bf = tg + timedelta(days=1)
        xmas = datetime(year, 12, 25)
        if self._in_window(date, tg, 10, 1):
            return 1.7
        if date.date() == bf.date():
            return 2.5
        if self._in_window(date, xmas, 14, 0):
            return 1.5
        # Grill weekends
        if self._in_window(date, self._memorial_day(year), 2, 2) or self._in_window(date, datetime(year,7,4), 2, 2) or self._in_window(date, self._labor_day(year), 2, 2):
            return 1.5
        return 1.0

    def _create_receipt(
        self, store: Store, customer: Customer, basket: Any, transaction_time: datetime
    ) -> dict[str, list[dict]]:
        """Create receipt, receipt lines, and inventory transactions.

        Args:
            store: Store where transaction occurred
            customer: Customer making purchase
            basket: ShoppingBasket with items to purchase
            transaction_time: Timestamp of transaction

        Returns:
            Dictionary with receipt, lines, and inventory_transactions

        Raises:
            ValueError: If basket has no items (business rule violation)
        """
        # CRITICAL: Validate basket has at least 1 item
        # Empty receipts violate business rules and should never be generated
        if not basket.items or len(basket.items) == 0:
            raise ValueError(
                f"Cannot create receipt with empty basket for customer {customer.ID} "
                f"at store {store.ID}. All receipts must have at least 1 line."
            )

        receipt_id = (
            f"RCP{transaction_time.strftime('%Y%m%d%H%M')}"
            f"{store.ID:03d}{self._rng.randint(1000, 9999)}"
        )
        trace_id = self._generate_trace_id()

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

        # Apply promotions to basket using CustomerJourneySimulator
        discount_amount, basket_items_with_promos = (
            self.customer_journey_sim.apply_promotions_to_basket(
                basket=basket,
                customer_id=customer.ID,
                transaction_date=transaction_time,
            )
        )
        # Helpers for integer-cents math
        from retail_datagen.shared.models import ProductTaxability
        def _to_cents(d: Decimal) -> int:
            return int((d * 100).quantize(Decimal("1")))

        def _fmt_cents(c: int) -> str:
            sign = '-' if c < 0 else ''
            c = abs(c)
            return f"{sign}{c // 100}.{c % 100:02d}"

        def _tax_cents(amount_cents: int, rate: Decimal, taxability: ProductTaxability) -> int:
            # rate to basis points (1/100 of a percent), multiplier as integer percentage
            rate_bps = int((rate * 10000).quantize(Decimal("1")))
            mult_pct = 100 if taxability == ProductTaxability.TAXABLE else 50 if taxability == ProductTaxability.REDUCED_RATE else 0
            # Compute rounded cents: (amount_cents * rate_bps * mult_pct) / 1_000_000
            num = amount_cents * rate_bps * mult_pct
            return (num + 500_000) // 1_000_000

        # Create receipt lines and inventory transactions using integer cents
        lines: list[dict] = []
        inventory_transactions: list[dict] = []
        subtotal_cents = 0
        total_tax_cents = 0

        # Get store tax rate (with backward compatibility default)
        store_tax_rate = (
            store.tax_rate if store.tax_rate is not None else Decimal("0.07407")
        )

        for line_num, item_data in enumerate(basket_items_with_promos, 1):
            product = item_data["product"]
            qty = int(item_data["qty"])  # ensure int
            promo_code = item_data.get("promo_code")
            line_discount_cents = _to_cents(item_data.get("discount", Decimal("0.00")))

            # Calculate unit price and ext price in cents
            unit_price_cents = _to_cents(product.SalePrice)
            ext_before_cents = unit_price_cents * qty
            ext_after_cents = max(0, ext_before_cents - line_discount_cents)

            # Calculate tax for this line item based on POST-DISCOUNT price
            taxability = getattr(product, "taxability", ProductTaxability.TAXABLE)
            line_tax_cents = _tax_cents(ext_after_cents, store_tax_rate, taxability)

            # Accumulate totals
            subtotal_cents += ext_after_cents
            total_tax_cents += line_tax_cents

            line = {
                "TraceId": trace_id,
                "EventTS": transaction_time,
                "ReceiptId": receipt_id,
                "Line": line_num,
                "ProductID": product.ID,
                "Qty": qty,
                "UnitPrice": _fmt_cents(unit_price_cents),
                "ExtPrice": _fmt_cents(ext_after_cents),
                "PromoCode": promo_code,
            }
            lines.append(line)

            # Create inventory transaction (sale)
            key = (store.ID, product.ID)
            current_balance = self.inventory_flow_sim._store_inventory.get(key, 0)
            new_balance = max(0, current_balance - qty)
            self.inventory_flow_sim._store_inventory[key] = new_balance

            # Get current balance after this transaction
            balance = self.inventory_flow_sim.get_store_balance(store.ID, product.ID)

            inventory_transaction = {
                "TraceId": trace_id,
                "EventTS": transaction_time,
                "StoreID": store.ID,
                "ProductID": product.ID,
                "QtyDelta": -qty,  # Negative for sale
                "Reason": InventoryReason.SALE.value,
                "Source": receipt_id,
                "Balance": balance,
            }
            inventory_transactions.append(inventory_transaction)

        # Header-level totals (preserve existing formula: Subtotal - Discount + Tax)
        discount_amount_cents = _to_cents(discount_amount)
        total_cents = subtotal_cents - discount_amount_cents + total_tax_cents

        # Validate subtotal (sanity check)
        try:
            calculated_subtotal_cents = sum(_to_cents(Decimal(line["ExtPrice"])) for line in lines)
            if abs(calculated_subtotal_cents - subtotal_cents) > 1:
                logger.error(
                    f"Receipt {receipt_id}: Subtotal mismatch! "
                    f"Calculated={calculated_subtotal_cents}, Recorded={subtotal_cents}"
                )
        except Exception:
            pass

        # Create receipt header
        receipt = {
            "TraceId": trace_id,
            "EventTS": transaction_time,
            "StoreID": store.ID,
            "CustomerID": customer.ID,
            "ReceiptId": receipt_id,
            "ReceiptType": "SALE",
            "Subtotal": _fmt_cents(subtotal_cents),
            "DiscountAmount": _fmt_cents(discount_amount_cents),  # Phase 2.2: Promotional discounts
            "Tax": _fmt_cents(total_tax_cents),
            "Total": _fmt_cents(total_cents),
            "TenderType": tender_type.value,
        }

        return {
            "receipt": receipt,
            "lines": lines,
            "inventory_transactions": inventory_transactions,
        }

    def _generate_foot_traffic(
        self, store: Store, hour_datetime: datetime, receipt_count: int
    ) -> list[dict]:
        """Vectorized foot traffic generator using NumPy for per-sensor aggregates.

        Builds a compact DataFrame and converts to records to minimize
        Python per-row overhead.
        """
        # If no receipts, still may have some foot traffic (browsers)
        if receipt_count == 0:
            if self._rng.random() > 0.7:
                return []
            receipt_count = 1

        hour = hour_datetime.hour
        is_weekend = hour_datetime.weekday() >= 5

        base_conversion = 0.20
        if hour in [12, 13, 17, 18, 19]:
            conv_adj = 1.3
        elif hour in [10, 11, 14, 15, 16]:
            conv_adj = 1.0
        elif hour in [8, 9, 20, 21]:
            conv_adj = 0.7
        else:
            conv_adj = 0.5
        if is_weekend:
            conv_adj *= 0.9
        conversion_rate = base_conversion * conv_adj

        total_foot_traffic = max(
            receipt_count + 1, int(receipt_count / max(conversion_rate, 1e-6))
        )

        zones = np.array([
            "ENTRANCE_MAIN",
            "ENTRANCE_SIDE",
            "AISLES_A",
            "AISLES_B",
            "CHECKOUT",
        ])
        proportions = np.array([0.35, 0.15, 0.20, 0.15, 0.15], dtype=np.float64)
        base_counts = np.floor(total_foot_traffic * proportions).astype(np.int32)
        # Add ±10% variance per zone
        variance = np.floor(base_counts * 0.1).astype(np.int32)
        # Draw symmetric noise per zone
        noise = np.array([
            0 if v <= 0 else self._np_rng.integers(-v, v + 1)
            for v in variance
        ], dtype=np.int32)
        counts = np.maximum(0, base_counts + noise)

        # Dwell ranges per zone
        dwell_min = np.array([45, 30, 180, 120, 90], dtype=np.int32)
        dwell_max = np.array([90, 75, 420, 300, 240], dtype=np.int32)
        dwell = self._np_rng.integers(dwell_min, dwell_max + 1)

        try:
            import pandas as _pd

            df = _pd.DataFrame(
                {
                    "TraceId": [self._generate_trace_id()] * len(zones),
                    "EventTS": [hour_datetime] * len(zones),
                    "StoreID": [store.ID] * len(zones),
                    "SensorId": [f"SENSOR_{store.ID:03d}_{z}" for z in zones],
                    "Zone": zones.astype(str),
                    "Dwell": dwell.astype(int),
                    "Count": counts.astype(int),
                }
            )
            return df.to_dict("records")
        except Exception:
            # Fallback to simple list if pandas unavailable
            return [
                {
                    "TraceId": self._generate_trace_id(),
                    "EventTS": hour_datetime,
                    "StoreID": store.ID,
                    "SensorId": f"SENSOR_{store.ID:03d}_{zones[i]}",
                    "Zone": str(zones[i]),
                    "Dwell": int(dwell[i]),
                    "Count": int(counts[i]),
                }
                for i in range(len(zones))
            ]

    def _generate_ble_pings(
        self, store: Store, customer: Customer, transaction_time: datetime
    ) -> list[dict]:
        """Generate BLE beacon pings for a customer visit (vectorized inner loop).

        30% of BLE pings will have customer_id resolved (customers with store app).
        70% remain anonymous (BLEId only).
        """
        zones_all = np.array(["ENTRANCE", "ELECTRONICS", "GROCERY", "CLOTHING", "CHECKOUT"], dtype=object)
        beacons_all = np.array([f"BEACON_{store.ID:03d}_{z}" for z in zones_all], dtype=object)
        # Choose 2-4 distinct zones
        k = int(self._np_rng.integers(2, 5))
        pick_idx = np.array(self._np_rng.choice(len(zones_all), size=k, replace=False))
        has_store_app = self._rng.random() < 0.30

        # Build per-zone ping counts and flatten
        per_zone_counts = self._np_rng.integers(2, 6, size=k)  # 2-5 pings per chosen zone
        total = int(per_zone_counts.sum())
        if total <= 0:
            return []
        # Repeat per-zone attributes to match ping counts
        zones_rep = np.repeat(zones_all[pick_idx], per_zone_counts)
        beacons_rep = np.repeat(beacons_all[pick_idx], per_zone_counts)
        rssi = self._np_rng.integers(-80, -29, size=total)
        offsets = self._np_rng.integers(-15, 16, size=total)

        try:
            import pandas as _pd

            df = _pd.DataFrame(
                {
                    "TraceId": [self._generate_trace_id()] * total,
                    "EventTS": [transaction_time] * total,
                    "StoreID": store.ID,
                    "BeaconId": beacons_rep,
                    "CustomerBLEId": customer.BLEId,
                    "CustomerId": customer.ID if has_store_app else None,
                    "RSSI": rssi.astype(int),
                    "Zone": zones_rep,
                }
            )
            # Apply offsets to timestamps efficiently
            df["EventTS"] = df["EventTS"] + _pd.to_timedelta(offsets, unit="m")
            return df.to_dict("records")
        except Exception:
            # Fallback to Python list
            out = []
            for i in range(total):
                out.append(
                    {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": transaction_time + timedelta(minutes=int(offsets[i])),
                        "StoreID": store.ID,
                        "BeaconId": str(beacons_rep[i]),
                        "CustomerBLEId": customer.BLEId,
                        "CustomerId": customer.ID if has_store_app else None,
                        "RSSI": int(rssi[i]),
                        "Zone": str(zones_rep[i]),
                    }
                )
            return out

    def _generate_truck_movements(
        self, date: datetime, store_transactions: list[dict]
    ) -> list[dict]:
        """Generate truck movements based on store inventory needs.

        This method creates initial shipments in SCHEDULED status.
        The _process_truck_lifecycle method will handle status progression.
        """
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
                    # Generate truck shipment (initial status: SCHEDULED)
                    departure_time = date.replace(hour=6, minute=0)  # 6 AM departure
                    shipment_info = self.inventory_flow_sim.generate_truck_shipment(
                        dc.ID, store_id, reorder_list, departure_time
                    )

                    # Create initial truck_move record in SCHEDULED status
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

                    # Track shipment for lifecycle processing
                    self._active_shipments[shipment_info["shipment_id"]] = shipment_info

        return truck_movements

    def _process_truck_lifecycle(
        self, date: datetime
    ) -> tuple[list[dict], list[dict], list[dict]]:
        """Process truck lifecycle for all active shipments on this date.

        Generates status progression records and inventory transactions:
        - SCHEDULED → LOADING: Generate DC OUTBOUND transactions
        - LOADING → IN_TRANSIT → ARRIVED
        - ARRIVED → UNLOADING: Generate Store INBOUND transactions
        - UNLOADING → COMPLETED

        Returns:
            Tuple of (truck_move_records, dc_outbound_txn, store_inbound_txn)
        """
        truck_lifecycle_records = []
        dc_outbound_txn = []
        store_inbound_txn = []

        # Process each active shipment to check for status changes on this date
        shipments_to_process = list(self._active_shipments.values())

        for shipment_info in shipments_to_process:
            shipment_id = shipment_info["shipment_id"]
            previous_status = shipment_info.get("status")

            # Update shipment status based on current date/time
            # We'll check at multiple times throughout the day to capture transitions
            for hour in range(24):
                check_time = date.replace(hour=hour, minute=0)
                updated_info = self.inventory_flow_sim.update_shipment_status(
                    shipment_id, check_time
                )

                if updated_info is None:
                    # Shipment was completed and removed from tracking
                    break

                current_status = updated_info["status"]

                # Generate records for status transitions
                if current_status != previous_status:
                    # Create truck_move record for this status change
                    truck_record = {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": check_time,
                        "TruckId": updated_info["truck_id"],
                        "DCID": updated_info["dc_id"],
                        "StoreID": updated_info["store_id"],
                        "ShipmentId": shipment_id,
                        "Status": current_status.value,
                        "ETA": updated_info["eta"],
                        "ETD": updated_info["etd"],
                    }
                    truck_lifecycle_records.append(truck_record)

                    # Generate inventory transactions at specific lifecycle stages
                    from retail_datagen.shared.models import TruckStatus

                    if current_status == TruckStatus.LOADING:
                        # Generate DC OUTBOUND transactions
                        dc_txn = self.inventory_flow_sim.generate_dc_outbound_transactions(
                            updated_info, check_time
                        )
                        for txn in dc_txn:
                            dc_outbound_txn.append({
                                "TraceId": self._generate_trace_id(),
                                "EventTS": txn["EventTS"],
                                "DCID": txn["DCID"],
                                "ProductID": txn["ProductID"],
                                "QtyDelta": txn["QtyDelta"],
                                "Reason": txn["Reason"].value,
                                "Source": txn["Source"],
                                # Ensure NOT NULL balance is populated for DC inventory
                                "Balance": txn.get("Balance", self.inventory_flow_sim.get_dc_balance(txn["DCID"], txn["ProductID"]))
                            })

                    elif current_status == TruckStatus.UNLOADING:
                        # Generate Store INBOUND transactions
                        store_txn = self.inventory_flow_sim.generate_store_inbound_transactions(
                            updated_info, check_time
                        )
                        for txn in store_txn:
                            store_inbound_txn.append({
                                "TraceId": self._generate_trace_id(),
                                "EventTS": txn["EventTS"],
                                "StoreID": txn["StoreID"],
                                "ProductID": txn["ProductID"],
                                "QtyDelta": txn["QtyDelta"],
                                "Reason": txn["Reason"].value,
                                "Source": txn["Source"],
                                "Balance": txn["Balance"],
                            })

                    previous_status = current_status

        return truck_lifecycle_records, dc_outbound_txn, store_inbound_txn

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
                    # Get current balance after this transaction
                    balance = self.inventory_flow_sim.get_store_balance(
                        transaction["StoreID"], transaction["ProductID"]
                    )

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
                # Insert records directly without DataFrame conversion
                await self._insert_hourly_to_db(self._session, fact_table, records, hour)
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
                # Use the most advanced table's completed hours; all tables move together
                "total_hours_completed": (
                    max(hourly_progress_data.get("completed_hours", {}).values())
                    if hourly_progress_data.get("completed_hours")
                    else 0
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
            OnlineOrderHeader,
            OnlineOrderLine,
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
            "online_orders": OnlineOrderHeader,
            "online_order_lines": OnlineOrderLine,
        }

        if table_name not in mapping:
            raise ValueError(f"Unknown table: {table_name}")

        return mapping[table_name]

    def _is_food_product(self, product: ProductMaster) -> bool:
        dept = (product.Department or "").lower()
        cat = (product.Category or "").lower()
        food_keywords = [
            "grocery",
            "food",
            "produce",
            "meat",
            "seafood",
            "dairy",
            "bakery",
            "beverage",
            "snack",
            "pantry",
            "frozen",
        ]
        return any(k in dept or k in cat for k in food_keywords)

    async def _generate_and_insert_returns(self, date: datetime, active_tables: list[str]) -> None:
        """Generate return receipts for this date (baseline + Dec 26 spike) and insert into DB.

        Strategy: sample a small subset of recent receipts, build negative receipts with
        corresponding inventory transactions and dispositions.
        """
        from sqlalchemy import text

        if "receipts" not in active_tables or "receipt_lines" not in active_tables:
            return

        # Determine spike factor for Dec 26 (day after Christmas)
        spike = 1.0
        if date.month == 12 and date.day == 26:
            spike = 6.0  # mid point of 5–10x

        # Baseline target returns per day as percentage of receipts (approx)
        # We'll cap to avoid heavy runtime on large datasets
        target_pct = 0.01 * spike  # 1% baseline, spiked on Dec 26

        # Fetch today's receipts (ids and store_id)
        rows = (
            await self._session.execute(
                text(
                    "SELECT receipt_id, store_id, event_ts FROM fact_receipts "
                    "WHERE date(event_ts)=:d AND (receipt_type IS NULL OR receipt_type='SALE')"
                ),
                {"d": date.strftime('%Y-%m-%d')},
            )
        ).fetchall()
        if not rows:
            return

        import random as _r
        max_returns = max(1, int(len(rows) * min(0.1, target_pct)))  # cap at 10% for safety
        sampled = _r.sample(rows, min(max_returns, len(rows)))

        # Build return receipts
        return_receipts = []
        return_lines = []
        return_store_txn = []

        store_rates = {s.ID: (s.tax_rate if s.tax_rate is not None else Decimal("0.07407")) for s in self.stores}
        products_by_id = {p.ID: p for p in self.products}

        for (orig_receipt_pk, store_id, event_ts) in sampled:
            # Fetch lines for original receipt
            line_rows = (
                await self._session.execute(
                    text(
                        "SELECT product_id, quantity, unit_price, ext_price, line_num FROM fact_receipt_lines WHERE receipt_id=:rid"
                    ),
                    {"rid": orig_receipt_pk},
                )
            ).fetchall()
            if not line_rows:
                continue

            # Build return header
            return_id_ext = f"RET{date.strftime('%Y%m%d')}{store_id:03d}{self._rng.randint(1000,9999)}"
            trace_id = self._generate_trace_id()
            store_tax_rate = store_rates.get(store_id, Decimal("0.07407"))
            subtotal = Decimal("0.00")
            total_tax = Decimal("0.00")

            # Lines
            for (product_id, qty, unit_price, ext_price, line_num) in line_rows:
                product = products_by_id.get(int(product_id))
                if not product:
                    continue
                # Negative quantities and ext price
                nqty = int(qty) * -1
                unit_price_dec = self._to_decimal(unit_price)
                next_price = (unit_price_dec * nqty).quantize(Decimal("0.01"))
                # ext_price could be used, but recompute to ensure consistency with negative qty
                neg_ext = (unit_price_dec * Decimal(nqty)).quantize(Decimal("0.01"))

                # Taxability
                from retail_datagen.shared.models import ProductTaxability

                taxability = getattr(product, "taxability", ProductTaxability.TAXABLE)
                if taxability == ProductTaxability.TAXABLE:
                    tax_mult = Decimal("1.0")
                elif taxability == ProductTaxability.REDUCED_RATE:
                    tax_mult = Decimal("0.5")
                else:
                    tax_mult = Decimal("0.0")

                line_tax = (neg_ext * store_tax_rate * tax_mult).quantize(Decimal("0.01"))

                subtotal += neg_ext
                total_tax += line_tax

                return_lines.append(
                    {
                        "TraceId": trace_id,
                        "EventTS": date.replace(hour=12, minute=0, second=0),
                        "ReceiptId": return_id_ext,
                        "Line": int(line_num),
                        "ProductID": int(product_id),
                        "Qty": nqty,
                        "UnitPrice": str(unit_price_dec.quantize(Decimal("0.01"))),
                        "ExtPrice": str(neg_ext),
                        "PromoCode": "RETURN",
                    }
                )

                # Inventory add for return
                key = (int(store_id), int(product_id))
                current_balance = self.inventory_flow_sim._store_inventory.get(key, 0)
                self.inventory_flow_sim._store_inventory[key] = current_balance + (-nqty)
                balance = self.inventory_flow_sim.get_store_balance(int(store_id), int(product_id))

                return_store_txn.append(
                    {
                        "TraceId": trace_id,
                        "EventTS": date.replace(hour=12, minute=30, second=0),
                        "StoreID": int(store_id),
                        "ProductID": int(product_id),
                        "QtyDelta": -nqty,  # positive added back
                        "Reason": InventoryReason.RETURN.value,
                        "Source": return_id_ext,
                        "Balance": balance,
                    }
                )

                # Disposition
                if self._is_food_product(product):
                    # Destroy all food returns
                    self.inventory_flow_sim._store_inventory[key] = max(0, self.inventory_flow_sim._store_inventory[key] - (-nqty))
                    balance2 = self.inventory_flow_sim.get_store_balance(int(store_id), int(product_id))
                    return_store_txn.append(
                        {
                            "TraceId": trace_id,
                            "EventTS": date.replace(hour=12, minute=45, second=0),
                            "StoreID": int(store_id),
                            "ProductID": int(product_id),
                            "QtyDelta": nqty,  # remove the quantity (negative of added)
                            "Reason": InventoryReason.DAMAGED.value,
                            "Source": "RETURN_DESTROY",
                            "Balance": balance2,
                        }
                    )
                else:
                    # Non-food: 40% restock, 20% open-box, 30% RTV, 10% destroy
                    r = self._rng.random()
                    if r < 0.40:
                        # Restock: nothing additional
                        pass
                    elif r < 0.60:
                        # Open box: keep in stock for future sale (no immediate txn)
                        pass
                    elif r < 0.90:
                        # Return to vendor: outbound shipment
                        self.inventory_flow_sim._store_inventory[key] = max(0, self.inventory_flow_sim._store_inventory[key] - (-nqty))
                        balance2 = self.inventory_flow_sim.get_store_balance(int(store_id), int(product_id))
                        return_store_txn.append(
                            {
                                "TraceId": trace_id,
                                "EventTS": date.replace(hour=13, minute=0, second=0),
                                "StoreID": int(store_id),
                                "ProductID": int(product_id),
                                "QtyDelta": nqty,
                                "Reason": InventoryReason.OUTBOUND_SHIPMENT.value,
                                "Source": "RTV",
                                "Balance": balance2,
                            }
                        )
                    else:
                        # Destroy/damaged
                        self.inventory_flow_sim._store_inventory[key] = max(0, self.inventory_flow_sim._store_inventory[key] - (-nqty))
                        balance2 = self.inventory_flow_sim.get_store_balance(int(store_id), int(product_id))
                        return_store_txn.append(
                            {
                                "TraceId": trace_id,
                                "EventTS": date.replace(hour=13, minute=15, second=0),
                                "StoreID": int(store_id),
                                "ProductID": int(product_id),
                                "QtyDelta": nqty,
                                "Reason": InventoryReason.DAMAGED.value,
                                "Source": "RETURN_DESTROY",
                                "Balance": balance2,
                            }
                        )

            total = (subtotal + total_tax).quantize(Decimal("0.01"))
            return_receipts.append(
                {
                    "TraceId": trace_id,
                    "EventTS": date.replace(hour=12, minute=0, second=0),
                    "StoreID": int(store_id),
                    "CustomerID": None,
                    "ReceiptId": return_id_ext,
                    "ReceiptType": "RETURN",
                    "ReturnForReceiptId": int(orig_receipt_pk),
                    "Subtotal": str(subtotal),
                    "DiscountAmount": str(Decimal("0.00")),
                    "Tax": str(total_tax),
                    "Total": str(total),
                    "TenderType": TenderType.CREDIT_CARD.value,
                }
            )

        # Insert returns in a single commit each (daily batch)
        if return_receipts:
            await self._insert_hourly_to_db(self._session, "receipts", return_receipts, hour=0, commit_every_batches=0)
        if return_lines:
            await self._insert_hourly_to_db(self._session, "receipt_lines", return_lines, hour=0, commit_every_batches=0)
        if return_store_txn and "store_inventory_txn" in active_tables:
            await self._insert_hourly_to_db(self._session, "store_inventory_txn", return_store_txn, hour=0, commit_every_batches=0)

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
                "ReceiptType": "receipt_type",
                "ReturnForReceiptId": "return_for_receipt_id",
                # Note: Subtotal field in generator is not stored (can be calculated)
                "DiscountAmount": "discount_amount",
                "Tax": "tax_amount",
                "Total": "total_amount",
                "TenderType": "payment_method",
            },
            "receipt_lines": {
                **common_mappings,
                # ReceiptId will be resolved to numeric FK by lookup before insert
                "ProductID": "product_id",
                "Line": "line_num",
                "Qty": "quantity",
                "UnitPrice": "unit_price",
                "ExtPrice": "ext_price",
                "PromoCode": "promo_code",
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
                "Source": "source",
                "Balance": "balance",
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
                # Header shape
                "CustomerID": "customer_id",
                "Subtotal": "subtotal_amount",
                "Tax": "tax_amount",
                "Total": "total_amount",
                "TenderType": "payment_method",
                "CompletedTS": "completed_ts",
                # Optional external order id
                "OrderId": "order_id_ext",
            },
            "online_order_lines": {
                **common_mappings,
                "OrderId": "order_id",
                "ProductID": "product_id",
                "Line": "line_num",
                "Qty": "quantity",
                "UnitPrice": "unit_price",
                "ExtPrice": "ext_price",
                "PromoCode": "promo_code",
                # Per-line lifecycle
                "PickedTS": "picked_ts",
                "ShippedTS": "shipped_ts",
                "DeliveredTS": "delivered_ts",
                "FulfillmentStatus": "fulfillment_status",
                "FulfillmentMode": "fulfillment_mode",
                "NodeType": "node_type",
                "NodeID": "node_id",
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

        # DuckDB fast-path: keep external linking keys on line tables to avoid FK lookups
        if getattr(self, "_use_duckdb", False):
            if table_name == "receipt_lines":
                ext = record.get("ReceiptId")
                if ext is not None and "receipt_id" not in mapped_record:
                    mapped_record["receipt_id_ext"] = ext
            elif table_name == "online_order_lines":
                ext = record.get("OrderId")
                if ext is not None and "order_id" not in mapped_record:
                    mapped_record["order_id_ext"] = ext

        return mapped_record

    async def _capture_and_drop_indexes(
        self, session: AsyncSession, generator_table_names: list[str]
    ) -> list[tuple[str, str]]:
        """
        Capture and drop nonessential indexes for specified tables to speed bulk inserts.

        Returns a list of (index_name, create_sql) to recreate later.
        Keeps indexes that are critical to linkage lookups (receipt_id_ext, order_id_ext).
        """
        from sqlalchemy import text

        captured: list[tuple[str, str]] = []
        try:
            for gen_name in generator_table_names:
                try:
                    model = self._get_model_for_table(gen_name)
                except Exception:
                    continue
                tbl = getattr(model, "__tablename__", None)
                if not tbl:
                    continue
                rows = (
                    await session.execute(
                        text(
                            "SELECT name, sql FROM sqlite_master "
                            "WHERE type='index' AND tbl_name=:tbl AND sql IS NOT NULL"
                        ),
                        {"tbl": tbl},
                    )
                ).all()

                for name, sql in rows:
                    if not name or not sql:
                        continue
                    lname = str(name).lower()
                    lsql = str(sql).lower()
                    # Keep ext-id linkage indexes to avoid slow lookups during generation
                    if (
                        ("receipt" in lname and "ext" in lname)
                        or ("order" in lname and "ext" in lname)
                        or ("receipt_id_ext" in lsql)
                        or ("order_id_ext" in lsql)
                    ):
                        continue
                    captured.append((name, sql))
                    try:
                        await session.execute(text(f'DROP INDEX IF EXISTS "{name}"'))
                    except Exception as e:
                        logger.debug(f"Failed to drop index {name} on {tbl}: {e}")
            await session.commit()
        except Exception as e:
            logger.debug(f"Index capture/drop skipped due to error: {e}")
        return captured

    async def _recreate_indexes(
        self, session: AsyncSession, index_defs: list[tuple[str, str]]
    ) -> None:
        """Recreate previously captured indexes after bulk inserts complete."""
        from sqlalchemy import text

        if not index_defs:
            return
        try:
            for name, sql in index_defs:
                try:
                    await session.execute(text(sql))
                except Exception as e:
                    logger.debug(f"Failed to recreate index {name}: {e}")
            await session.commit()
        except Exception as e:
            logger.debug(f"Index recreation encountered an error: {e}")

    async def _insert_hourly_to_db(
        self,
        session: AsyncSession,
        table_name: str,
        data: list[dict] | pd.DataFrame,
        hour: int,
        batch_size: int = 10000,
        commit_every_batches: int = 0,
    ) -> None:
        """
        Insert hourly data batch into the database (DuckDB fast-path).

        Args:
            session: Database session for facts.db
            table_name: Name of fact table (e.g., "receipts")
            data: List of dicts or DataFrame with hourly data
            hour: Hour index (0-23)
            batch_size: Rows per batch insert (default: 10000)

        Note:
            Commits once after all batches are inserted for performance.
            Individual batches are not committed to minimize I/O overhead.
            Field names are automatically mapped from PascalCase to snake_case.
        """
        # Normalize input into list of dict records
        records: list[dict]
        try:
            import pandas as _pd
            if isinstance(data, _pd.DataFrame):
                if data.empty:
                    logger.debug(f"No data to insert for {table_name} hour {hour}")
                    return
                records = data.to_dict("records")
            else:
                records = list(data or [])
        except Exception:
            # If pandas not available, assume list path
            records = list(data or [])  # type: ignore[arg-type]
        if not records:
            logger.debug(f"No data to insert for {table_name} hour {hour}")
            return

        # DuckDB fast path
        if getattr(self, "_use_duckdb", False) and getattr(self, "_duckdb_conn", None) is not None:
            try:
                # Map and normalize records for DuckDB
                mapped_records = [self._map_field_names_for_db(table_name, r) for r in records]

                # Normalize NaNs/NaT to None
                try:
                    import pandas as _pd

                    df = _pd.DataFrame(mapped_records)
                except Exception:
                    import pandas as _pd  # fallback

                    df = _pd.DataFrame.from_records(mapped_records)

                # Create/insert into DuckDB table
                from retail_datagen.db.duckdb_engine import insert_dataframe

                # Map generator table name to DuckDB table (match existing naming)
                duck_table = {
                    "dc_inventory_txn": "fact_dc_inventory_txn",
                    "truck_moves": "fact_truck_moves",
                    "store_inventory_txn": "fact_store_inventory_txn",
                    "receipts": "fact_receipts",
                    "receipt_lines": "fact_receipt_lines",
                    "foot_traffic": "fact_foot_traffic",
                    "ble_pings": "fact_ble_pings",
                    "marketing": "fact_marketing",
                    "online_orders": "fact_online_order_headers",
                    "online_order_lines": "fact_online_order_lines",
                }.get(table_name, table_name)

                inserted = insert_dataframe(self._duckdb_conn, duck_table, df)

                # Update per-table counts and emit progress
                try:
                    if not hasattr(self, "_table_insert_counts"):
                        self._table_insert_counts = {}
                    self._table_insert_counts[table_name] = (
                        self._table_insert_counts.get(table_name, 0) + int(inserted)
                    )

                    tracker_state = self.hourly_tracker.get_current_progress()
                    completed_hours = tracker_state.get("completed_hours", {}).get(
                        table_name, 0
                    )
                    total_days = tracker_state.get("total_days") or 1
                    total_hours_expected = max(1, total_days * 24)
                    # Treat full batch as completing the hour for this table
                    per_table_fraction = min(
                        1.0, (completed_hours + 1.0) / total_hours_expected
                    )
                    self._emit_table_progress(
                        table_name,
                        per_table_fraction,
                        f"Writing {table_name.replace('_', ' ')} ({self._table_insert_counts[table_name]:,})",
                        {table_name: self._table_insert_counts[table_name]},
                    )
                except Exception:
                    pass
            except Exception as e:
                logger.error(f"DuckDB insert failed for {table_name}: {e}")
            return

        # Map table name to model (SQLite path)
        try:
            model_class = self._get_model_for_table(table_name)
        except ValueError as e:
            logger.error(f"Cannot insert data: {e}")
            return

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
        elif table_name == "online_order_lines":
            try:
                # Collect unique external order ids from raw records
                ext_ids = list({r.get("OrderId") for r in records if r.get("OrderId")})
                if not ext_ids:
                    logger.debug("No online order external IDs to resolve for lines")
                    return
                # Map external order id -> header PK
                headers_model = self._get_model_for_table("online_orders")
                from sqlalchemy import select

                rows = (
                    await session.execute(
                        select(headers_model.order_id, headers_model.order_id_ext).where(
                            headers_model.order_id_ext.in_(ext_ids)
                        )
                    )
                ).all()
                id_map = {ext: pk for (pk, ext) in rows}

                mapped_records = []
                for record in records:
                    mapped = self._map_field_names_for_db(table_name, record)
                    ext = record.get("OrderId")
                    pk = id_map.get(ext)
                    if not pk:
                        logger.debug(
                            f"Skipping online_order_line with unknown OrderId={ext}"
                        )
                        continue
                    mapped["order_id"] = int(pk)
                    mapped_records.append(mapped)
            except Exception as e:
                logger.error(f"Failed to resolve order_ids for online_order_lines: {e}")
                return
        else:
            # Default mapping path
            mapped_records = [
                self._map_field_names_for_db(table_name, record) for record in records
            ]

        # Normalize pandas NaT/NaN values to None for DB serialization
        # Pandas can produce NaT (datetime) and NaN (float) which fail for
        # INTEGER/DATETIME columns during bulk insert. Convert any pd.isna(x)
        # values to plain None so NULLs are written instead.
        try:
            import pandas as _pd  # local import to avoid hard dependency elsewhere

            normalized: list[dict] = []
            for rec in mapped_records:
                clean: dict = {}
                for k, v in rec.items():
                    # Convert pandas/NumPy missing values to None
                    if _pd.isna(v):  # True for NaN/NaT
                        clean[k] = None
                    else:
                        clean[k] = v
                normalized.append(clean)
            mapped_records = normalized
        except Exception:
            # If pandas isn't available or any issue occurs, proceed without normalization
            pass

        # Filter out any keys that are not actual columns in the target table
        try:
            allowed_cols = {col.name for col in model_class.__table__.columns}
            filtered_records = []
            for rec in mapped_records:
                filtered = {k: v for k, v in rec.items() if k in allowed_cols}
                filtered_records.append(filtered)
            mapped_records = filtered_records
        except Exception:
            # Defensive: if column introspection fails, proceed without filtering
            pass

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

    RetailConfig.from_file(config_path)
    # NOTE: This convenience function is legacy; in API flow we construct with an AsyncSession.
    # Here we construct a temporary generator without DB session which is not supported in DB mode.
    # Users should use the FastAPI endpoints instead.
    raise RuntimeError("Use API endpoints for historical generation in SQLite mode")

    # Unreachable in DB mode
