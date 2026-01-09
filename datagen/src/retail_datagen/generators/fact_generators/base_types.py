"""
Type definitions for fact generator mixins.

This module provides a base class with type annotations for attributes
that are shared across fact generator mixins. Mixins should inherit from
this base class to get proper type checking support.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from datetime import datetime
from threading import Lock
from typing import TYPE_CHECKING, Any

import numpy as np

from retail_datagen.config.models import RetailConfig
from retail_datagen.generators.seasonal_patterns import CompositeTemporalPatterns
from retail_datagen.shared.customer_geography import StoreSelector
from retail_datagen.shared.models import (
    Customer,
    DistributionCenter,
    GeographyMaster,
    ProductMaster,
    Store,
    Truck,
)

from ..retail_patterns import (
    BusinessRulesEngine,
    CustomerJourneySimulator,
    InventoryFlowSimulator,
    MarketingCampaignSimulator,
)
from .progress import HourlyProgressTracker

if TYPE_CHECKING:
    import duckdb

    from retail_datagen.generators.progress_tracker import TableProgressTracker


class FactGeneratorBase:
    """
    Base class providing type annotations for fact generator mixins.

    This class enables mypy to understand the mixin pattern used by
    FactDataGenerator. All mixins inherit from this class to get type
    checking support for cross-mixin calls.

    MAINTENANCE CONTRACT:
    ---------------------
    1. Attributes here must match those in core.py FactDataGenerator.__init__
    2. Method signatures must match actual implementations in the mixins
    3. When adding a new attribute to FactDataGenerator, add it here too
    4. When adding a cross-mixin method, add a stub here
    5. The `...` body in method stubs is intentional - implementations in mixins

    Type checking flow:
    - Mixins inherit from FactGeneratorBase (for type hints)
    - FactDataGenerator inherits from all mixins (for implementations)
    - mypy sees attributes/methods via FactGeneratorBase
    - Runtime uses actual implementations from mixins

    See also:
    - core.py: FactDataGenerator class with actual attribute initialization
    - *_mixin.py: Mixin classes with actual method implementations
    """

    # Configuration
    config: RetailConfig
    _rng: random.Random
    _np_rng: np.random.Generator

    # DuckDB connection
    _use_duckdb: bool
    _duckdb_conn: duckdb.DuckDBPyConnection | None

    # Database buffer for batched writes
    _db_buffer: dict[str, list[dict]]

    # Master data collections
    geographies: list[GeographyMaster]
    stores: list[Store]
    distribution_centers: list[DistributionCenter]
    customers: list[Customer]
    products: list[ProductMaster]
    trucks: list[Truck]

    # Business logic simulators
    temporal_patterns: CompositeTemporalPatterns
    business_rules: BusinessRulesEngine
    customer_journey_sim: CustomerJourneySimulator | None
    inventory_flow_sim: InventoryFlowSimulator | None
    marketing_campaign_sim: MarketingCampaignSimulator | None

    # Customer/store selection
    store_selector: StoreSelector | None
    _store_customer_pools: dict[int, list[tuple[Customer, float]]]
    _store_customer_sampling: dict[int, tuple[list[Customer], list[float]]]

    # Fast CRM lookup
    _adid_to_customer_id: dict[str, int]

    # Active campaigns and shipments
    _active_campaigns: dict[str, Any]
    _active_shipments: dict[str, Any]

    # Trace ID generation
    _trace_counter: int

    # Progress tracking
    _progress_callback: Callable[[float, str | None], None] | None
    _table_progress_callback: (
        Callable[[str, float, str | None, dict | None], None] | None
    )
    _last_progress_update_time: float
    _progress_lock: Lock
    _progress_history: list[tuple[float, float]]
    _progress_tracker: TableProgressTracker | None
    hourly_tracker: HourlyProgressTracker

    # Table filtering
    _included_tables: set[str] | None
    FACT_TABLES: list[str]

    # Generation state
    _generation_end_date: datetime | None
    _publish_to_outbox: bool

    # Stockout tracking
    _last_stockout_detection: dict[tuple[int, int], datetime]

    # Session (deprecated but still referenced)
    _session: Any

    # Numpy arrays for customer sampling optimization
    _store_customer_sampling_np: dict[int, tuple[Any, Any]]

    # Batch buffers for database writes
    _batch_buffers: dict[str, list[dict]]

    # Holiday date helpers (defined in seasonal_mixin)
    _thanksgiving_date: Callable[[int], datetime]
    _memorial_day: Callable[[int], datetime]
    _labor_day: Callable[[int], datetime]

    # ------------------------------------------------------------------------
    # Method stubs for cross-mixin calls
    # These are implemented in various mixins but called from others.
    # Actual implementations are in the respective mixin files.
    # ------------------------------------------------------------------------

    # Utils mixin methods
    def _generate_trace_id(self) -> str:
        """Generate a unique trace ID."""
        ...

    def _randomize_time_within_day(self, base_date: datetime) -> datetime:
        """Randomize time within a day."""
        ...

    def _to_decimal(self, value: float) -> Any:
        """Convert float to Decimal."""
        ...

    def _in_window(self, check_time: datetime, window_end: datetime) -> bool:
        """Check if a time is within a window."""
        ...

    # Progress reporting mixin methods
    def _send_throttled_progress_update(
        self, progress: float, message: str | None = None
    ) -> None:
        """Send a throttled progress update."""
        ...

    def _emit_table_progress(
        self, table: str, progress: float, message: str | None = None
    ) -> None:
        """Emit progress for a specific table."""
        ...

    def _reset_table_states(self) -> None:
        """Reset table generation states."""
        ...

    # Persistence mixin methods
    def _insert_hourly_to_db(
        self,
        table_name: str,
        records: list[dict],
        *,
        skip_validation: bool = False,
    ) -> None:
        """Insert records to DuckDB."""
        ...

    def _capture_and_drop_indexes(self, table_name: str) -> list[str]:
        """Capture and drop indexes for a table."""
        ...

    def _recreate_indexes(self, table_name: str, index_statements: list[str]) -> None:
        """Recreate indexes for a table."""
        ...

    def _cache_fact_counts(self) -> None:
        """Cache fact table counts."""
        ...

    def _update_watermarks_after_generation(self) -> None:
        """Update watermarks after generation."""
        ...

    def _get_model_for_table(self, table_name: str) -> Any:
        """Get the Pydantic model for a table."""
        ...

    def _export_hourly_facts(
        self, table_name: str, date: datetime, records: list[dict]
    ) -> None:
        """Export hourly facts to storage."""
        ...

    # Data loading mixin methods
    def load_master_data_from_duckdb(self) -> None:
        """Load master data from DuckDB."""
        ...

    def _get_available_products_for_date(self, date: datetime) -> list[ProductMaster]:
        """Get products available on a specific date."""
        ...

    def _build_store_customer_pools(self, customer_geographies: dict) -> None:
        """Build customer pools for stores."""
        ...

    # Core methods (defined in core.py)
    def _active_fact_tables(self) -> list[str]:
        """Get list of active fact tables."""
        ...

    # Seasonal mixin methods
    def _compute_marketing_multiplier(self, date: datetime) -> float:
        """Compute marketing multiplier for a date."""
        ...

    def _apply_holiday_overlay_to_basket(
        self, basket: list[dict], date: datetime
    ) -> list[dict]:
        """Apply holiday overlays to basket."""
        ...

    # Marketing mixin methods
    def _generate_marketing_activity(
        self, date: datetime, multiplier: float
    ) -> list[dict]:
        """Generate marketing activity for a date."""
        ...

    # Logistics mixin methods
    def _generate_truck_movements(
        self, date: datetime, multiplier: float
    ) -> tuple[list[dict], list[dict]]:
        """Generate truck movements for a date."""
        ...

    def _process_truck_lifecycle(
        self, truck: Truck, date: datetime
    ) -> tuple[list[dict], list[dict]]:
        """Process truck lifecycle for a date."""
        ...

    def _process_truck_deliveries(
        self, date: datetime
    ) -> tuple[list[dict], list[dict]]:
        """Process truck deliveries for a date."""
        ...

    # Online orders mixin methods
    def _generate_online_orders(
        self, date: datetime, multiplier: float
    ) -> tuple[list[dict], list[dict]]:
        """Generate online orders for a date."""
        ...

    # Payments mixin methods
    def _generate_payment_for_online_order(self, order: dict) -> dict:
        """Generate payment for an online order."""
        ...

    def _generate_payment_for_receipt(self, receipt: dict) -> dict:
        """Generate payment for a receipt."""
        ...

    # Store ops mixin methods
    def _generate_store_operations_for_day(
        self, date: datetime, multiplier: float
    ) -> list[dict]:
        """Generate store operations for a day."""
        ...

    def _generate_store_hour_activity(
        self, store: Store, hour_datetime: datetime, multiplier: float
    ) -> dict[str, list[dict]]:
        """Generate store hour activity."""
        ...

    # Stockouts mixin methods
    def _generate_stockouts_from_inventory_txns(
        self, txns: list[dict], date: datetime
    ) -> list[dict]:
        """Generate stockouts from inventory transactions."""
        ...

    # Promotions mixin methods
    def _generate_promotions_from_receipt(self, receipt: dict) -> list[dict]:
        """Generate promotions from a receipt."""
        ...

    # Receipts mixin methods
    def _generate_and_insert_returns_duckdb(
        self, date: datetime, multiplier: float
    ) -> None:
        """Generate and insert returns using DuckDB."""
        ...

    def _generate_and_insert_returns(self, date: datetime, multiplier: float) -> None:
        """Generate and insert returns."""
        ...

    # Sensors mixin methods
    def _generate_foot_traffic(
        self, store: Store, date: datetime, hour: int
    ) -> list[dict]:
        """Generate foot traffic for a store hour."""
        ...

    def _generate_ble_pings(
        self, store: Store, date: datetime, hour: int, customer_ids: list[int]
    ) -> list[dict]:
        """Generate BLE pings for customers in store."""
        ...

    def _generate_customer_zone_changes(
        self, store: Store, date: datetime, hour: int, customer_ids: list[int]
    ) -> list[dict]:
        """Generate customer zone changes."""
        ...
