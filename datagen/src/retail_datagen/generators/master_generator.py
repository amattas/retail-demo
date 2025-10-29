"""
Master data generation engine for retail data generator.

This module implements the MasterDataGenerator class that creates all dimension
tables from dictionary data as specified in AGENTS.md.
"""

import logging
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Type

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

from retail_datagen.config.models import RetailConfig
from retail_datagen.shared.cache import CacheManager
from retail_datagen.shared.dictionary_loader import DictionaryLoader
from retail_datagen.shared.models import (
    Customer,
    DCInventorySnapshot,
    DistributionCenter,
    GeographyDict,
    GeographyMaster,
    ProductBrandDict,
    ProductCompanyDict,
    ProductDict,
    ProductMaster,
    Store,
    StoreInventorySnapshot,
    Truck,
)
from retail_datagen.shared.validators import (
    ForeignKeyValidator,
    PricingCalculator,
    SyntheticDataValidator,
)

# Import SQLAlchemy models for database insertion
try:
    from retail_datagen.db.models.master import (
        Geography as GeographyModel,
        Store as StoreModel,
        DistributionCenter as DistributionCenterModel,
        Truck as TruckModel,
        Customer as CustomerModel,
        Product as ProductModel,
    )
    from retail_datagen.db.session import get_retail_session
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    GeographyModel = None
    StoreModel = None
    DistributionCenterModel = None
    TruckModel = None
    CustomerModel = None
    ProductModel = None
    get_retail_session = None

from .utils import (
    AddressGenerator,
    GeographicDistribution,
    IdentifierGenerator,
    ProgressReporter,
    SyntheticNameGenerator,
)

logger = logging.getLogger(__name__)

class MasterDataGenerator:
    """
    Main master data generation engine.

    Generates all 5 dimension tables from dictionary data:
    - geographies_master.csv
    - stores.csv
    - distribution_centers.csv
    - customers.csv
    - products_master.csv
    """

    def __init__(self, config: RetailConfig):
        """
        Initialize master data generator.

        Args:
            config: Retail configuration containing generation parameters
        """
        self.config = config

        # Initialize random number generator with seed
        self._rng = random.Random(config.seed)

        # Initialize utility classes
        self.dictionary_loader = DictionaryLoader(config.paths.dictionaries)
        self.pricing_calculator = PricingCalculator(config.seed)
        self.synthetic_validator = SyntheticDataValidator()
        self.fk_validator = ForeignKeyValidator()

        # Initialize data holders
        self._geography_data: list[GeographyDict] | None = None
        self._first_names: list[str] | None = None
        self._last_names: list[str] | None = None
        self._product_data: list[ProductDict] | None = None
        self._brand_data: list[ProductBrandDict] | None = None
        self._company_data: list[ProductCompanyDict] | None = None

        # Generated master data
        self.geography_master: list[GeographyMaster] = []
        self.stores: list[Store] = []
        self.distribution_centers: list[DistributionCenter] = []
        self.trucks: list[Truck] = []
        self.customers: list[Customer] = []
        self.products_master: list[ProductMaster] = []

        # Inventory snapshots
        self.dc_inventory_snapshots: list[DCInventorySnapshot] = []
        self.store_inventory_snapshots: list[StoreInventorySnapshot] = []

        # Progress callback for UI updates (table_name, progress, message)
        self._progress_callback: (
            Callable[[str, float, str | None], None] | None
        ) = None

        print(f"MasterDataGenerator initialized with seed {config.seed}")

    def set_progress_callback(
        self, callback: Callable[[str, float, str | None], None] | None
    ) -> None:
        """Register or clear a callback for incremental progress updates."""
        self._progress_callback = callback

    def _get_ui_table_name(self, model_class: Type[DeclarativeBase]) -> str | None:
        """Map ORM model to UI table key used by progress tiles."""
        try:
            tbl = model_class.__tablename__
        except Exception:
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
        session: AsyncSession,
        model_class: Type[DeclarativeBase],
        pydantic_models: list,
        batch_size: int = 10000,
        commit_every_batches: int = 5,
    ) -> None:
        """
        Insert Pydantic models into database table using bulk insertion.

        This method converts Pydantic models to dictionaries and performs
        efficient bulk inserts with progress logging.

        Args:
            session: Database session for insertion
            model_class: SQLAlchemy model class (e.g., GeographyModel)
            pydantic_models: List of Pydantic model instances to insert
            batch_size: Number of rows per batch insert (default: 10,000)

        Raises:
            Exception: If database insertion fails
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
                # Convert Pydantic model to dict
                record = model.model_dump()

                # Map Pydantic field names to SQLAlchemy column names
                mapped_record = self._map_pydantic_to_db_columns(record, model_class)
                records.append(mapped_record)

            # Insert in batches for performance and memory efficiency
            batch_index = 0
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]

                # Use bulk_insert_mappings for performance
                await session.execute(
                    model_class.__table__.insert(),
                    batch
                )
                await session.flush()  # Flush to database but don't commit yet

                records_inserted = min(i + batch_size, total_records)
                logger.info(
                    f"Inserted {records_inserted:,} / {total_records:,} rows into {table_name}"
                )

                # Emit incremental progress every batch (default 10,000)
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
                    # Periodic commit improves durability for very large tables
                    try:
                        await session.commit()
                        logger.info(
                            f"Committed after {batch_index} batches for {table_name}"
                        )
                    except Exception:
                        logger.warning(
                            f"Commit failed after batch {batch_index} for {table_name}; continuing with session context manager"
                        )

            logger.info(f"Successfully inserted all {total_records:,} records into {table_name}")

        except Exception as e:
            logger.error(f"Failed to insert data into {table_name}: {e}", exc_info=True)
            raise

    def _map_pydantic_to_db_columns(
        self,
        pydantic_dict: dict[str, Any],
        model_class: Type[DeclarativeBase],
    ) -> dict[str, Any]:
        """
        Map Pydantic model field names to SQLAlchemy column names.

        The Pydantic models use PascalCase field names (e.g., ID, City, State),
        while SQLAlchemy models use the same column names in the database.
        This method also handles type conversions as needed.

        Args:
            pydantic_dict: Dictionary from Pydantic model.model_dump()
            model_class: SQLAlchemy model class for column mapping

        Returns:
            Dictionary with database column names and properly typed values
        """
        mapped = {}

        # Get column mappings from SQLAlchemy model (not currently used for renaming,
        # but retained for potential future divergence between Pydantic and DB names)
        # The column 'name' attribute contains the actual database column name
        column_mappings = {col.key: col.name for col in model_class.__table__.columns}

        for key, value in pydantic_dict.items():
            # Convert Decimal to float for SQLite
            if isinstance(value, Decimal):
                value = float(value)

            # Convert datetime to date for SQLite Date columns (e.g., Product.LaunchDate)
            # SQLite's Date type only accepts Python date objects.
            elif isinstance(value, datetime):
                value = value.date()

            # Handle None values
            elif value is None:
                pass

            # Map to correct column name (Pydantic uses same names as DB columns)
            mapped[key] = value

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
            self._progress_callback(table_name, clamped, message, table_counts)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug(
                "Master progress callback failed for %s: %s", table_name, exc, exc_info=True
            )

    async def generate_all_master_data_async(
        self,
        session: AsyncSession,
        parallel: bool = True,
    ) -> None:
        """
        Generate all master data tables and write to SQLite database.

        Args:
            session: AsyncSession for retail.db (required)
            parallel: Enable parallel generation for independent tables (default True)
        """
        if not SQLALCHEMY_AVAILABLE:
            raise RuntimeError(
                "SQLAlchemy models not available. Cannot write to database."
            )

        print("Starting master data generation...")
        print(f"Parallel processing: {'enabled' if parallel else 'disabled'}")

        # Store session for later use
        self._db_session = session

        # Load dictionary data
        self._load_dictionary_data()

        # Read entity counts from configuration
        stores_count = self.config.volume.stores

        # Phase 1: Sequential (geographic dependencies)
        print("\nPhase 1: Generating geographic dependencies (sequential)...")
        await self.generate_geography_master_async()
        await self.generate_distribution_centers_async()
        await self.generate_stores_async(count=stores_count)
        await self.generate_trucks_async()

        # Phase 2: Parallel customer + product generation
        if parallel:
            print("\nPhase 2: Generating customers and products (parallel)...")
            max_workers = min(2, self.config.performance.get_max_workers())
            print(f"Using {max_workers} parallel workers (CPU limit: {self.config.performance.max_cpu_percent}%)")

            # Note: Running async methods in thread pool - they'll complete synchronously
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self._run_async_in_thread, self.generate_customers_async): 'customers',
                    executor.submit(self._run_async_in_thread, self.generate_products_master_async): 'products'
                }
                for future in as_completed(futures):
                    table = futures[future]
                    try:
                        future.result()
                        print(f"✓ {table} generation completed")
                    except Exception as e:
                        print(f"✗ {table} generation failed: {e}")
                        raise
        else:
            # Sequential fallback
            print("\nPhase 2: Generating customers and products (sequential)...")
            await self.generate_customers_async()
            await self.generate_products_master_async()

        # Phase 3: Parallel inventory snapshots
        if parallel:
            print("\nPhase 3: Generating inventory snapshots (parallel)...")
            max_workers = min(2, self.config.performance.get_max_workers())
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self._run_async_in_thread, self.generate_dc_inventory_snapshots_async): 'dc_inventory',
                    executor.submit(self._run_async_in_thread, self.generate_store_inventory_snapshots_async): 'store_inventory'
                }
                for future in as_completed(futures):
                    table = futures[future]
                    try:
                        future.result()
                        print(f"✓ {table} snapshot completed")
                    except Exception as e:
                        print(f"✗ {table} snapshot failed: {e}")
                        raise
        else:
            # Sequential fallback
            print("\nPhase 3: Generating inventory snapshots (sequential)...")
            await self.generate_dc_inventory_snapshots_async()
            await self.generate_store_inventory_snapshots_async()

        # Validate foreign key relationships
        self._validate_foreign_keys()

        # NOTE: Do NOT commit here - let the calling context manager handle commit
        # This prevents double-commit issues with get_retail_session() context manager

        # Flush to ensure all changes are in the transaction
        if session:
            await session.flush()
            logger.info("All master data flushed to session (commit will be handled by context manager)")

        # Cache counts
        self._cache_master_counts()

        print("Master data generation complete!")

    def _run_async_in_thread(self, coro):
        """Helper to run async coroutine in thread pool."""
        import asyncio
        return asyncio.run(coro)

    def _load_dictionary_data(self) -> None:
        """Load all required dictionary data from CSV files."""
        print("Loading dictionary data...")
        print(
            f"Dictionary path: {Path(self.dictionary_loader.dictionary_path).resolve()}"
        )

        self._geography_data = self.dictionary_loader.load_geographies()
        print(f"Loaded {len(self._geography_data)} geographies")

        # Load names
        first_name_dicts = self.dictionary_loader.load_first_names()
        self._first_names = [fn.FirstName for fn in first_name_dicts]
        print(f"Loaded {len(self._first_names)} first names")

        last_name_dicts = self.dictionary_loader.load_last_names()
        self._last_names = [ln.LastName for ln in last_name_dicts]
        print(f"Loaded {len(self._last_names)} last names")

        # Load product data
        self._product_data = self.dictionary_loader.load_products()
        print(f"Loaded {len(self._product_data)} products")

        raw_brand_data = self.dictionary_loader.load_product_brands()
        print(f"Loaded {len(raw_brand_data)} brands")
        if raw_brand_data:
            print(
                f"  Sample brand: '{raw_brand_data[0].Brand}' | Company={getattr(raw_brand_data[0], 'Company', None)} | Category='{raw_brand_data[0].Category}'"
            )
        # Use brands as provided by dictionaries. Synthetic safety is enforced at source data.
        self._brand_data = raw_brand_data

        self._company_data = self.dictionary_loader.load_product_companies()
        print(f"Loaded {len(self._company_data)} companies")
        if self._company_data:
            print(
                f"  Sample company: '{self._company_data[0].Company}' | Category='{self._company_data[0].Category}'"
            )

        print("Dictionary data loading complete")

    async def generate_geography_master_async(self) -> None:
        """Generate geographies_master data with optional database insertion."""
        print("Generating geography master data...")
        self._emit_progress("geographies_master", 0.0, "Generating geographies")

        if not self._geography_data:
            raise ValueError("Geography data not loaded")

        # Limit geographies to the configured number
        geography_count = self.config.volume.total_geographies
        available_count = len(self._geography_data)

        if geography_count > available_count:
            print(
                f"Warning: Requested {geography_count} geographies but only {available_count} available. Using all available."
            )
            geography_count = available_count

        # Select a subset of geographies using random sampling for consistency
        selected_geographies = self._rng.sample(self._geography_data, geography_count)

        # Store the selected geographies for use by other generators
        self._selected_geography_data = selected_geographies

        self.geography_master = []

        for i, geo_dict in enumerate(selected_geographies, 1):
            geography = GeographyMaster(
                ID=i,
                City=geo_dict.City,
                State=geo_dict.State,
                ZipCode=str(geo_dict.Zip),
                District=geo_dict.District,
                Region=geo_dict.Region,
            )
            self.geography_master.append(geography)

        # Register geography IDs with FK validator
        geography_ids = [geo.ID for geo in self.geography_master]
        self.fk_validator.register_geography_ids(geography_ids)

        print(f"Generated {len(self.geography_master)} geography master records")

        # Insert to database if session provided
        if hasattr(self, '_db_session') and self._db_session:
            await self._insert_to_db(
                self._db_session,
                GeographyModel,
                self.geography_master
            )

        self._emit_progress("geographies_master", 1.0, "Geographies complete")

    def generate_geography_master(self) -> None:
        """Generate geographies_master.csv from geography dictionary (legacy sync wrapper)."""
        import asyncio
        asyncio.run(self.generate_geography_master_async())

    def generate_stores(self, count: int | None = None) -> None:
        """Generate stores.csv with strategic geographic distribution.

        Args:
            count: Number of stores to generate. If None, uses config default.
        """
        print("Generating store data...")
        self._emit_progress("stores", 0.0, "Generating stores")

        if not self.geography_master:
            raise ValueError("Geography master data must be generated first")

        if not self.distribution_centers:
            raise ValueError(
                "Distribution centers must be generated before stores for supply chain constraint"
            )

        store_count = count if count is not None else self.config.volume.stores

        # Get states where DCs exist to constrain store placement
        dc_states = set()
        for dc in self.distribution_centers:
            dc_geo = next(gm for gm in self.geography_master if gm.ID == dc.GeographyID)
            dc_states.add(dc_geo.State)

        print(
            f"Constraining stores to {len(dc_states)} states with DCs: {sorted(dc_states)}"
        )

        # Filter geography data to only include states with DCs
        selected_geography_data = getattr(
            self, "_selected_geography_data", self._geography_data
        )
        dc_constrained_geo_data = [
            geo for geo in selected_geography_data if geo.State in dc_states
        ]

        if not dc_constrained_geo_data:
            raise ValueError(
                "No geography data found in states with distribution centers"
            )

        # Initialize geographic distribution using DC-constrained geographies
        geo_distribution = GeographicDistribution(
            dc_constrained_geo_data, self.config.seed
        )
        address_generator = AddressGenerator(dc_constrained_geo_data, self.config.seed)
        id_generator = IdentifierGenerator(self.config.seed)

        # Get strategic locations for stores (high-weight geographies in DC states only)
        strategic_geos = geo_distribution.get_strategic_locations(
            min(store_count, len(dc_constrained_geo_data))
        )

        # If we need more stores than strategic locations, distribute remainder
        if store_count > len(strategic_geos):
            remaining_stores = store_count - len(strategic_geos)
            additional_distribution = (
                geo_distribution.distribute_entities_across_geographies(
                    remaining_stores
                )
            )
        else:
            additional_distribution = []

        self.stores = []
        current_id = 1

        # Place at least one store in each strategic location
        for geo in strategic_geos:
            # Find matching geography master record
            geo_master = next(
                gm
                for gm in self.geography_master
                if (
                    gm.City == geo.City
                    and gm.State == geo.State
                    and gm.ZipCode == geo.Zip
                )
            )

            store = Store(
                ID=current_id,
                StoreNumber=id_generator.generate_store_number(current_id),
                Address=address_generator.generate_address(geo, "commercial"),
                GeographyID=geo_master.ID,
            )
            self.stores.append(store)
            current_id += 1

        # Add additional stores based on distribution
        for geo, count in additional_distribution:
            # Find matching geography master record
            geo_master = next(
                gm
                for gm in self.geography_master
                if (
                    gm.City == geo.City
                    and gm.State == geo.State
                    and gm.ZipCode == geo.Zip
                )
            )

            for _ in range(count):
                if current_id > store_count:
                    break

                store = Store(
                    ID=current_id,
                    StoreNumber=id_generator.generate_store_number(current_id),
                    Address=address_generator.generate_address(geo, "commercial"),
                    GeographyID=geo_master.ID,
                )
                self.stores.append(store)
                current_id += 1

        # Register store IDs with FK validator
        store_ids = [store.ID for store in self.stores]
        self.fk_validator.register_store_ids(store_ids)

        print(f"Generated {len(self.stores)} store records")
        # Do not mark complete here; async method will finalize after DB write

    async def generate_stores_async(self, count: int | None = None) -> None:
        """Generate stores data with optional database insertion (async version)."""
        print("Generating store data...")
        self._emit_progress("stores", 0.0, "Generating stores")

        if not self.geography_master:
            raise ValueError("Geography master data must be generated first")

        if not self.distribution_centers:
            raise ValueError(
                "Distribution centers must be generated before stores for supply chain constraint"
            )

        store_count = count if count is not None else self.config.volume.stores

        # Get states where DCs exist to constrain store placement
        dc_states = set()
        for dc in self.distribution_centers:
            dc_geo = next(gm for gm in self.geography_master if gm.ID == dc.GeographyID)
            dc_states.add(dc_geo.State)

        print(
            f"Constraining stores to {len(dc_states)} states with DCs: {sorted(dc_states)}"
        )

        # Filter geography data to only include states with DCs
        selected_geography_data = getattr(
            self, "_selected_geography_data", self._geography_data
        )
        dc_constrained_geo_data = [
            geo for geo in selected_geography_data if geo.State in dc_states
        ]

        if not dc_constrained_geo_data:
            raise ValueError(
                "No geography data found in states with distribution centers"
            )

        # Initialize geographic distribution using DC-constrained geographies
        geo_distribution = GeographicDistribution(
            dc_constrained_geo_data, self.config.seed
        )
        address_generator = AddressGenerator(dc_constrained_geo_data, self.config.seed)
        id_generator = IdentifierGenerator(self.config.seed)

        # Get strategic locations for stores (high-weight geographies in DC states only)
        strategic_geos = geo_distribution.get_strategic_locations(
            min(store_count, len(dc_constrained_geo_data))
        )

        # If we need more stores than strategic locations, distribute remainder
        if store_count > len(strategic_geos):
            remaining_stores = store_count - len(strategic_geos)
            additional_distribution = (
                geo_distribution.distribute_entities_across_geographies(
                    remaining_stores
                )
            )
        else:
            additional_distribution = []

        self.stores = []
        current_id = 1

        # Place at least one store in each strategic location
        for geo in strategic_geos:
            # Find matching geography master record
            geo_master = next(
                gm
                for gm in self.geography_master
                if (
                    gm.City == geo.City
                    and gm.State == geo.State
                    and gm.ZipCode == geo.Zip
                )
            )

            store = Store(
                ID=current_id,
                StoreNumber=id_generator.generate_store_number(current_id),
                Address=address_generator.generate_address(geo, "commercial"),
                GeographyID=geo_master.ID,
            )
            self.stores.append(store)
            current_id += 1

        # Add additional stores based on distribution
        for geo, count_per_geo in additional_distribution:
            # Find matching geography master record
            geo_master = next(
                gm
                for gm in self.geography_master
                if (
                    gm.City == geo.City
                    and gm.State == geo.State
                    and gm.ZipCode == geo.Zip
                )
            )

            for _ in range(count_per_geo):
                if current_id > store_count:
                    break

                store = Store(
                    ID=current_id,
                    StoreNumber=id_generator.generate_store_number(current_id),
                    Address=address_generator.generate_address(geo, "commercial"),
                    GeographyID=geo_master.ID,
                )
                self.stores.append(store)
                current_id += 1

        # Register store IDs with FK validator
        store_ids = [store.ID for store in self.stores]
        self.fk_validator.register_store_ids(store_ids)

        print(f"Generated {len(self.stores)} store records")

        # Insert to database if session provided
        if hasattr(self, '_db_session') and self._db_session:
            await self._insert_to_db(
                self._db_session,
                StoreModel,
                self.stores
            )
            # Now mark complete after DB write
            self._emit_progress("stores", 1.0, "Stores complete")

        self._emit_progress(
            "stores", 1.0, "Stores complete", {"stores": len(self.stores)}
        )

    def generate_distribution_centers(self) -> None:
        """Generate distribution_centers.csv with strategic placement."""
        print("Generating distribution center data...")
        self._emit_progress(
            "distribution_centers", 0.0, "Generating distribution centers"
        )

        if not self.geography_master:
            raise ValueError("Geography master data must be generated first")

        dc_count = self.config.volume.dcs

        # Initialize utilities using selected geographies
        selected_geography_data = getattr(
            self, "_selected_geography_data", self._geography_data
        )
        geo_distribution = GeographicDistribution(
            selected_geography_data, self.config.seed + 1000
        )
        address_generator = AddressGenerator(
            selected_geography_data, self.config.seed + 1000
        )
        id_generator = IdentifierGenerator(self.config.seed + 1000)

        # DCs should be strategically placed in highest-weight regions
        strategic_geos = geo_distribution.get_strategic_locations(dc_count)

        self.distribution_centers = []

        for i, geo in enumerate(strategic_geos, 1):
            # Find matching geography master record
            geo_master = next(
                gm
                for gm in self.geography_master
                if (
                    gm.City == geo.City
                    and gm.State == geo.State
                    and gm.ZipCode == geo.Zip
                )
            )

            dc = DistributionCenter(
                ID=i,
                DCNumber=id_generator.generate_dc_number(i),
                Address=address_generator.generate_address(geo, "industrial"),
                GeographyID=geo_master.ID,
            )
            self.distribution_centers.append(dc)

        # Register DC IDs with FK validator
        dc_ids = [dc.ID for dc in self.distribution_centers]
        self.fk_validator.register_dc_ids(dc_ids)

        print(f"Generated {len(self.distribution_centers)} distribution center records")
        self._emit_progress(
            "distribution_centers",
            1.0,
            "Distribution centers complete",
            {"distribution_centers": len(self.distribution_centers)},
        )

    async def generate_distribution_centers_async(self) -> None:
        """Generate distribution centers with optional database insertion (async version)."""
        # Call sync method for generation logic (without DB writes)
        self.generate_distribution_centers()

        # Insert to database if session provided
        if hasattr(self, '_db_session') and self._db_session:
            await self._insert_to_db(
                self._db_session,
                DistributionCenterModel,
                self.distribution_centers
            )

    def generate_trucks(self) -> None:
        """Generate trucks.csv with refrigeration capabilities."""
        try:
            print("Generating truck data...")
            self._emit_progress("trucks", 0.0, "Generating trucks")
            print(
                f"Distribution centers available: {len(self.distribution_centers) if self.distribution_centers else 0}"
            )

            if not self.distribution_centers:
                raise ValueError("Distribution center data must be generated first")

            refrigerated_count = self.config.volume.refrigerated_trucks
            non_refrigerated_count = self.config.volume.non_refrigerated_trucks
            total_trucks = refrigerated_count + non_refrigerated_count
            print(
                f"Trucks to generate: {total_trucks} ({refrigerated_count} refrigerated, {non_refrigerated_count} non-refrigerated)"
            )

            id_generator = IdentifierGenerator(self.config.seed + 3000)
            self.trucks = []

            # Distribute trucks evenly across DCs
            dc_count = len(self.distribution_centers)
            trucks_per_dc = total_trucks // dc_count
            remaining_trucks = total_trucks % dc_count

            current_id = 1

            # Generate refrigerated trucks first
            refrigerated_generated = 0
            for dc_idx, dc in enumerate(self.distribution_centers):
                # Calculate how many trucks this DC should get
                base_trucks = trucks_per_dc
                if dc_idx < remaining_trucks:
                    base_trucks += 1

                # Determine refrigerated vs non-refrigerated split for this DC
                dc_refrigerated = min(
                    refrigerated_count - refrigerated_generated,
                    max(1, int(base_trucks * (refrigerated_count / total_trucks))),
                )
                dc_non_refrigerated = base_trucks - dc_refrigerated

                # Create refrigerated trucks for this DC
                for _ in range(dc_refrigerated):
                    truck = Truck(
                        ID=current_id,
                        LicensePlate=id_generator.generate_license_plate(current_id),
                        Refrigeration=True,
                        DCID=dc.ID,
                    )
                    self.trucks.append(truck)
                    current_id += 1
                    refrigerated_generated += 1

                # Create non-refrigerated trucks for this DC
                for _ in range(dc_non_refrigerated):
                    truck = Truck(
                        ID=current_id,
                        LicensePlate=id_generator.generate_license_plate(current_id),
                        Refrigeration=False,
                        DCID=dc.ID,
                    )
                    self.trucks.append(truck)
                    current_id += 1

            # Generate additional supplier-to-DC trucks
            supplier_refrigerated_count = (
                self.config.volume.supplier_refrigerated_trucks
            )
            supplier_non_refrigerated_count = (
                self.config.volume.supplier_non_refrigerated_trucks
            )
            supplier_total_trucks = (
                supplier_refrigerated_count + supplier_non_refrigerated_count
            )

            print(
                f"Generating {supplier_total_trucks} supplier trucks ({supplier_refrigerated_count} refrigerated, {supplier_non_refrigerated_count} non-refrigerated)"
            )

            # Use DCID = None to represent supplier trucks (not assigned to specific DCs)
            supplier_dc_id = None

            # Generate supplier refrigerated trucks
            for _ in range(supplier_refrigerated_count):
                truck = Truck(
                    ID=current_id,
                    LicensePlate=id_generator.generate_license_plate(current_id),
                    Refrigeration=True,
                    DCID=supplier_dc_id,
                )
                self.trucks.append(truck)
                current_id += 1

            # Generate supplier non-refrigerated trucks
            for _ in range(supplier_non_refrigerated_count):
                truck = Truck(
                    ID=current_id,
                    LicensePlate=id_generator.generate_license_plate(current_id),
                    Refrigeration=False,
                    DCID=supplier_dc_id,
                )
                self.trucks.append(truck)
                current_id += 1

            # Register truck IDs with FK validator
            truck_ids = [truck.ID for truck in self.trucks]
            self.fk_validator.register_truck_ids(truck_ids)

            total_trucks = len(self.trucks)
            dc_trucks = refrigerated_count + non_refrigerated_count
            print(f"Generated {total_trucks} total truck records:")
            print(
                f"  - DC-to-Store trucks: {dc_trucks} ({refrigerated_count} refrigerated, {non_refrigerated_count} non-refrigerated)"
            )
            print(
                f"  - Supplier-to-DC trucks: {supplier_total_trucks} ({supplier_refrigerated_count} refrigerated, {supplier_non_refrigerated_count} non-refrigerated)"
            )
            # Do not mark complete here; async method will finalize after DB write
        except Exception as e:
            print(f"ERROR in generate_trucks: {e}")
            import traceback

            traceback.print_exc()
            raise

    async def generate_trucks_async(self) -> None:
        """Generate trucks with optional database insertion (async version)."""
        # Call sync method for generation logic
        self.generate_trucks()

        # Insert to database if session provided
        if hasattr(self, '_db_session') and self._db_session:
            await self._insert_to_db(
                self._db_session,
                TruckModel,
                self.trucks
            )
            # Mark complete after DB write
            self._emit_progress("trucks", 1.0, "Trucks complete")

    def generate_customers(self) -> None:
        """Generate customers.csv with realistic geographic distribution."""
        print("Generating customer data...")

        if not self.geography_master:
            raise ValueError("Geography master data must be generated first")

        if not self._first_names or not self._last_names:
            raise ValueError("Name data not loaded")

        # Use configured customer count
        customer_count = self.config.volume.total_customers

        # Initialize utilities using selected geographies
        selected_geography_data = getattr(
            self, "_selected_geography_data", self._geography_data
        )
        geo_distribution = GeographicDistribution(
            selected_geography_data, self.config.seed + 2000
        )
        address_generator = AddressGenerator(
            selected_geography_data, self.config.seed + 2000
        )
        name_generator = SyntheticNameGenerator(
            self._first_names, self._last_names, self.config.seed + 2000
        )
        id_generator = IdentifierGenerator(self.config.seed + 2000)

        # Distribute customers across geographies
        customer_distribution = geo_distribution.distribute_entities_across_geographies(
            customer_count
        )

        self.customers = []
        current_id = 1

        # We'll reflect progress during DB write phase to tiles; just seed initial state
        progress_reporter = ProgressReporter(customer_count, "Generating customers")
        self._emit_progress("customers", 0.0, "Generating customers")

        for geo, count in customer_distribution:
            # Find matching geography master record
            geo_master = next(
                gm
                for gm in self.geography_master
                if (
                    gm.City == geo.City
                    and gm.State == geo.State
                    and gm.ZipCode == geo.Zip
                )
            )

            for _ in range(count):
                first_name, last_name = name_generator.generate_name_pair()

                customer = Customer(
                    ID=current_id,
                    FirstName=first_name,
                    LastName=last_name,
                    Address=address_generator.generate_address(geo, "residential"),
                    GeographyID=geo_master.ID,
                    LoyaltyCard=id_generator.generate_loyalty_card(current_id),
                    Phone=id_generator.generate_phone_number(),
                    BLEId=id_generator.generate_ble_id(current_id),
                    AdId=id_generator.generate_ad_id(current_id),
                )

                self.customers.append(customer)
                current_id += 1

                # Update internal reporter; UI progress will reflect DB insert
                if len(self.customers) % 1000 == 0:
                    progress_reporter.update(1000)

        progress_reporter.complete()
        # Don't mark complete yet; DB write progress will follow

        # Register customer IDs with FK validator
        customer_ids = [customer.ID for customer in self.customers]
        self.fk_validator.register_customer_ids(customer_ids)

        print(f"Generated {len(self.customers)} customer records")

    async def generate_customers_async(self) -> None:
        """Generate customers with optional database insertion (async version)."""
        # Call sync method for generation logic
        self.generate_customers()

        # Insert to database if session provided
        if hasattr(self, '_db_session') and self._db_session:
            await self._insert_to_db(
                self._db_session,
                CustomerModel,
                self.customers,
                batch_size=5000,
                commit_every_batches=1,
            )
            # Verify DB count matches generated count for diagnostics
            try:
                from sqlalchemy import select, func
                result = await self._db_session.execute(
                    select(func.count()).select_from(CustomerModel)
                )
                db_count = int(result.scalar() or 0)
                logger.info(
                    f"Customer insert verification: expected={len(self.customers):,}, db={db_count:,}"
                )
            except Exception as e:
                logger.warning(f"Could not verify customer DB count: {e}")

    def generate_products_master(self) -> None:
        """Generate products_master.csv with realistic pricing and brand combinations."""
        print("Generating product master data with brand combinations...")
        self._emit_progress("products_master", 0.0, "Generating products")

        if not self._product_data or not self._brand_data or not self._company_data:
            raise ValueError("Product dictionary data not loaded")

        # Create brand-company mapping by category
        # Group companies by category for smart matching
        companies_by_category = {}
        for company in self._company_data:
            category = company.Category
            if category not in companies_by_category:
                companies_by_category[category] = []
            companies_by_category[category].append(company.Company)

        company_names = [company.Company for company in self._company_data]

        # Target product count
        target_product_count = self.config.volume.total_products

        print(f"Target products: {target_product_count}")
        print(f"Available base products: {len(self._product_data)}")
        print(f"Available brands: {len(self._brand_data)}")
        print(f"Available companies: {len(company_names)}")

        self.products_master = []
        product_id = 1

        # Create category-aware brand-product combinations
        print("Creating category-aware brand-product combinations...")

        # Group brands by category for smart pairing
        brands_by_category = {}
        for brand_idx, brand in enumerate(self._brand_data):
            category = brand.Category
            if category not in brands_by_category:
                brands_by_category[category] = []
            brands_by_category[category].append((brand_idx, brand))

        # Group products by category for smart pairing
        products_by_category = {}
        for product_idx, product in enumerate(self._product_data):
            # Map product categories to brand categories
            product_category = self._map_product_to_brand_category(
                product.Category, product.Department
            )
            if product_category not in products_by_category:
                products_by_category[product_category] = []
            products_by_category[product_category].append((product_idx, product))

        print(f"Brand categories: {sorted(brands_by_category.keys())}")
        print(f"Product categories mapped to: {sorted(products_by_category.keys())}")

        # Create realistic combinations within each category
        valid_combinations = []
        for category in brands_by_category.keys():
            if category in products_by_category:
                category_brands = brands_by_category[category]
                category_products = products_by_category[category]

                # Create combinations within this category
                for product_idx, product in category_products:
                    for brand_idx, brand in category_brands:
                        valid_combinations.append((product_idx, brand_idx))

                print(
                    f"Category '{category}': {len(category_brands)} brands × {len(category_products)} products = {len(category_brands) * len(category_products)} combinations"
                )

        print(f"Total valid category-matched combinations: {len(valid_combinations):,}")

        # Sample exactly target_product_count combinations from valid ones
        if len(valid_combinations) >= target_product_count:
            selected_combinations = self._rng.sample(
                valid_combinations, target_product_count
            )
        else:
            # If we don't have enough valid combinations, use all and fill the rest by sampling with replacement
            selected_combinations = valid_combinations.copy()
            additional_needed = target_product_count - len(valid_combinations)
            additional_combinations = self._rng.choices(
                valid_combinations, k=additional_needed
            )
            selected_combinations.extend(additional_combinations)

        print(f"Selected {len(selected_combinations)} category-matched combinations")

        progress_reporter = ProgressReporter(
            target_product_count, "Generating product combinations"
        )

        # Generate products from selected combinations - keep generating until we have exactly target_product_count
        combination_idx = 0
        successful_products = 0
        failed_validations = 0

        # Use infinite loop with break condition instead of relying on combination list length
        while successful_products < target_product_count:
            # If we've exhausted our combinations, generate more
            if combination_idx >= len(selected_combinations):
                remaining_needed = target_product_count - successful_products
                batch_size = min(
                    1000, remaining_needed * 2
                )  # Generate extra to account for validation failures
                print(
                    f"Exhausted combinations at {combination_idx} with {successful_products}/{target_product_count} successful products. Generating {batch_size} more combinations..."
                )
                additional_combinations = self._rng.choices(
                    valid_combinations, k=batch_size
                )
                selected_combinations.extend(additional_combinations)

            product_idx, brand_idx = selected_combinations[combination_idx]
            combination_idx += 1
            product = self._product_data[product_idx]
            brand = self._brand_data[brand_idx]

            # Match company to brand by category
            brand_category = brand.Category
            if (
                brand_category in companies_by_category
                and companies_by_category[brand_category]
            ):
                # Choose a company from the same category
                company = self._rng.choice(companies_by_category[brand_category])
            else:
                # Fallback to a random company if no category match
                company = self._rng.choice(company_names)

            # Calculate pricing structure with slight variation per brand
            # Base pricing on the product's BasePrice with brand-specific adjustments
            from decimal import Decimal

            base_price = float(product.BasePrice)

            # Add brand-specific price variation (±5-15%)
            price_variation = self._rng.uniform(0.85, 1.15)
            adjusted_base_price = base_price * price_variation

            # Convert to Decimal for pricing calculator
            pricing = self.pricing_calculator.calculate_full_pricing(
                Decimal(str(adjusted_base_price))
            )

            # Determine if product requires refrigeration based on category
            requires_refrigeration = self._requires_refrigeration(
                product.Category, product.Subcategory
            )

            # Calculate product launch date based on historical start date and product sequence
            launch_date = self._calculate_product_launch_date(
                product_id, target_product_count
            )

            # Use brand as provided without blocklist-based screening

            # Try to create ProductMaster with validation retry
            max_retries = 5
            product_master = None

            for retry in range(max_retries):
                try:
                    product_master = ProductMaster(
                        ID=product_id,
                        ProductName=product.ProductName,
                        Brand=brand.Brand,
                        Company=company,
                        Department=product.Department,
                        Category=product.Category,
                        Subcategory=product.Subcategory,
                        Cost=pricing["Cost"],
                        MSRP=pricing["MSRP"],
                        SalePrice=pricing["SalePrice"],
                        RequiresRefrigeration=requires_refrigeration,
                        LaunchDate=launch_date,
                    )
                    break  # Success - exit retry loop
                except ValueError as e:
                    if retry < max_retries - 1:
                        # Recalculate pricing with different variation
                        price_variation = self._rng.uniform(
                            0.9, 1.1
                        )  # Narrower range for retry
                        adjusted_base_price = base_price * price_variation
                        pricing = self.pricing_calculator.calculate_full_pricing(
                            Decimal(str(adjusted_base_price))
                        )
                    else:
                        # Final retry failed - log and skip this combination
                        print(
                            f"Warning: Failed to generate valid pricing for product {product.ProductName} + {brand.Brand} after {max_retries} attempts: {e}"
                        )
                        failed_validations += 1
                        break

            # Only add if we successfully created the product
            if product_master:
                self.products_master.append(product_master)
                successful_products += 1
            else:
                failed_validations += 1

            product_id += 1

            # Update internal progress reporter every 500
            if successful_products % 500 == 0 and successful_products > 0:
                progress_reporter.update(500)

        # Final progress update for internal reporter only
        remaining_progress = target_product_count - (successful_products // 500) * 500
        if remaining_progress > 0:
            progress_reporter.update(remaining_progress)

        progress_reporter.complete()
        # Don't mark complete yet; DB write progress will follow

        # Detailed generation summary
        print("\n=== Product Generation Summary ===")
        print(f"Target products: {target_product_count}")
        print(f"Total combinations processed: {combination_idx}")
        print(f"Successful products: {successful_products}")
        print(f"Failed validations: {failed_validations}")
        print(f"Final product count: {len(self.products_master)}")

        # The loop above should have guaranteed exactly target_product_count products
        assert (
            successful_products == target_product_count
        ), f"Expected {target_product_count} products, got {successful_products}"
        assert (
            len(self.products_master) == target_product_count
        ), f"Expected {target_product_count} products in list, got {len(self.products_master)}"

        # Register product IDs with FK validator
        product_ids = [product.ID for product in self.products_master]
        self.fk_validator.register_product_ids(product_ids)

        print(
            f"Generated {len(self.products_master)} product master records with brand combinations"
        )

    async def generate_products_master_async(self) -> None:
        """Generate products with optional database insertion (async version)."""
        # Call sync method for generation logic
        self.generate_products_master()

        # Insert to database if session provided
        if hasattr(self, '_db_session') and self._db_session:
            await self._insert_to_db(
                self._db_session,
                ProductModel,
                self.products_master,
                batch_size=2000,
                commit_every_batches=1,
            )
            # Verify DB count matches generated count for diagnostics
            try:
                from sqlalchemy import select, func
                result = await self._db_session.execute(
                    select(func.count()).select_from(ProductModel)
                )
                db_count = int(result.scalar() or 0)
                logger.info(
                    f"Product insert verification: expected={len(self.products_master):,}, db={db_count:,}"
                )
            except Exception as e:
                logger.warning(f"Could not verify product DB count: {e}")

    def _map_product_to_brand_category(
        self, product_category: str, product_department: str
    ) -> str:
        """Map product categories/departments to appropriate brand categories."""
        category_mapping = {
            # Food-related mappings
            "Fresh Produce": "Food",
            "Meat & Seafood": "Food",
            "Dairy & Eggs": "Food",
            "Frozen Foods": "Food",
            "Pantry Staples": "Food",
            "Beverages": "Food",
            "Snacks & Candy": "Food",
            "Bakery": "Food",
            "International Foods": "Food",
            "Organic & Natural": "Food",
            # Electronics mappings
            "Consumer Electronics": "Electronics",
            "Computers & Accessories": "Electronics",
            "Mobile Devices": "Electronics",
            "Gaming": "Electronics",
            "Audio & Video": "Electronics",
            "Smart Home": "Electronics",
            "Wearable Tech": "Electronics",
            # Clothing mappings
            "Men's Apparel": "Clothing",
            "Women's Apparel": "Clothing",
            "Kids' Clothing": "Clothing",
            "Athletic Wear": "Clothing",
            "Footwear": "Clothing",
            "Accessories": "Clothing",
            # Health & Personal Care mappings
            "Health & Wellness": "Health",
            "Personal Care": "Health",
            "Beauty": "Health",
            "Pharmacy": "Health",
            "Baby Care": "Health",
            # Pet mappings
            "Pet Food": "Pet",
            "Pet Supplies": "Pet",
            "Pet Care": "Pet",
            # Automotive mappings
            "Automotive": "Automotive",
            "Car Care": "Automotive",
            # Office mappings
            "Office Supplies": "Office",
            "School & Office": "Office",
            "Stationery": "Office",
            "Business Supplies": "Office",
            # Sports mappings
            "Sports & Outdoors": "Sports",
            "Exercise & Fitness": "Sports",
            "Outdoor Recreation": "Sports",
            "Team Sports": "Sports",
            # Home mappings
            "Home & Garden": "Home",
            "Household Essentials": "Home",
            "Home Improvement": "Home",
            "Furniture": "Home",
            "Kitchen & Dining": "Home",
            "Home Decor": "Home",
        }

        # First try exact category match
        if product_category in category_mapping:
            return category_mapping[product_category]

        # Then try department match
        if product_department in category_mapping:
            return category_mapping[product_department]

        # For unmapped categories, make intelligent guesses based on keywords
        category_lower = product_category.lower()
        department_lower = product_department.lower()

        # Food-related keywords
        if any(
            keyword in category_lower or keyword in department_lower
            for keyword in [
                "food",
                "grocery",
                "fresh",
                "frozen",
                "meat",
                "dairy",
                "produce",
                "beverage",
                "snack",
                "bakery",
            ]
        ):
            return "Food"

        # Electronics keywords
        elif any(
            keyword in category_lower or keyword in department_lower
            for keyword in [
                "electronic",
                "computer",
                "tech",
                "digital",
                "mobile",
                "phone",
                "gaming",
                "audio",
                "video",
            ]
        ):
            return "Electronics"

        # Clothing keywords
        elif any(
            keyword in category_lower or keyword in department_lower
            for keyword in [
                "apparel",
                "clothing",
                "fashion",
                "wear",
                "shoe",
                "footwear",
                "accessory",
            ]
        ):
            return "Clothing"

        # Default to Home for anything else
        else:
            return "Home"

    def _calculate_product_launch_date(
        self, product_id: int, total_products: int
    ) -> datetime:
        """
        Calculate product launch date for realistic product introduction over time.

        Products are introduced gradually:
        - First 60% of products: launched before historical start date (already established)
        - Next 30%: launched during first 6 months of historical period
        - Last 10%: launched during remaining historical period
        """
        from datetime import datetime, timedelta

        # Parse historical start date from config
        historical_start = datetime.strptime(
            self.config.historical.start_date, "%Y-%m-%d"
        )

        # Define product introduction windows
        established_products_pct = 0.60  # 60% already established
        early_launch_pct = 0.30  # 30% launch in first 6 months

        established_count = int(total_products * established_products_pct)
        early_launch_count = int(total_products * early_launch_pct)

        if product_id <= established_count:
            # Already established products - launched 6 months to 2 years before historical start
            days_before = self._rng.randint(180, 730)  # 6 months to 2 years
            return historical_start - timedelta(days=days_before)

        elif product_id <= established_count + early_launch_count:
            # Early launch products - launched in first 6 months of historical period
            days_after = self._rng.randint(0, 180)  # First 6 months
            return historical_start + timedelta(days=days_after)

        else:
            # Late launch products - launched during rest of historical period
            # Assume 1 year historical period total, so launch in months 6-12
            days_after = self._rng.randint(180, 365)  # Months 6-12
            return historical_start + timedelta(days=days_after)

    def _requires_refrigeration(self, category: str, subcategory: str) -> bool:
        """Determine if a product requires refrigeration based on category and subcategory."""
        # Categories that always require refrigeration
        refrigerated_categories = {"Dairy & Alternatives", "Meat & Poultry", "Seafood"}

        # Produce items that require refrigeration (most fresh items)
        if category == "Produce":
            # Most produce items require refrigeration except shelf-stable items
            # For simplicity, assume 80% of produce requires refrigeration
            return True

        # Baby food categories that require refrigeration
        if category == "Baby Food":
            return True

        # Specific health & beauty items that require refrigeration
        if category == "Medicine" and "refrigerated" in subcategory.lower():
            return True

        # All other non-grocery categories don't require refrigeration
        non_refrigerated_departments = {
            "Electronics",
            "Clothing",
            "Health & Beauty",
            "Baby & Kids",
            "Home & Garden",
            "Pet Supplies",
            "Automotive",
            "Office Supplies",
            "Seasonal",
            "Sports & Recreation",
        }

        # Get department from the first part of the category hierarchy
        # For new products, we need to check the Department field instead
        # But since this method only gets category/subcategory, we'll infer from category names
        for dept in non_refrigerated_departments:
            if category.startswith(dept) or dept in category:
                return False

        # Check if category is explicitly refrigerated
        return category in refrigerated_categories

    def _validate_foreign_keys(self) -> None:
        """Validate all foreign key relationships."""
        print("Validating foreign key relationships...")

        validation_errors = []

        # Validate store geography references
        for store in self.stores:
            if not self.fk_validator.validate_geography_fk(store.GeographyID):
                validation_errors.append(
                    f"Store {store.ID} has invalid GeographyID {store.GeographyID}"
                )

        # Validate DC geography references
        for dc in self.distribution_centers:
            if not self.fk_validator.validate_geography_fk(dc.GeographyID):
                validation_errors.append(
                    f"DC {dc.ID} has invalid GeographyID {dc.GeographyID}"
                )

        # Validate customer geography references (sample check for performance)
        sample_size = min(1000, len(self.customers))
        sample_customers = self._rng.sample(self.customers, sample_size)

        for customer in sample_customers:
            if not self.fk_validator.validate_geography_fk(customer.GeographyID):
                validation_errors.append(
                    f"Customer {customer.ID} has invalid GeographyID {customer.GeographyID}"
                )

        if validation_errors:
            raise ValueError(
                "Foreign key validation failed:\n" + "\n".join(validation_errors[:10])
            )

        # Print validation summary
        summary = self.fk_validator.get_validation_summary()
        print(f"FK validation passed: {summary}")

    def generate_dc_inventory_snapshots(self) -> None:
        """Generate realistic initial inventory snapshots for distribution centers."""
        print("Generating DC inventory snapshots...")
        self._emit_progress(
            "dc_inventory_snapshots", 0.0, "Generating DC inventory snapshots"
        )

        if not self.distribution_centers or not self.products_master:
            raise ValueError(
                "DCs and products must be generated before inventory snapshots"
            )

        self.dc_inventory_snapshots = []
        current_time = datetime.now()

        total_records = len(self.distribution_centers) * len(self.products_master)
        print(f"Generating {total_records:,} DC inventory records...")

        for dc in self.distribution_centers:
            for product in self.products_master:
                # DCs have higher inventory levels than stores (500-5000 vs 20-200)
                current_qty = random.randint(
                    self.config.volume.dc_initial_inventory_min,
                    self.config.volume.dc_initial_inventory_max,
                )

                # Reorder point is typically 10-20% of current inventory
                reorder_point = random.randint(
                    self.config.volume.dc_reorder_point_min,
                    min(self.config.volume.dc_reorder_point_max, current_qty // 5),
                )

                inventory_record = DCInventorySnapshot(
                    DCID=dc.ID,
                    ProductID=product.ID,
                    CurrentQuantity=current_qty,
                    ReorderPoint=reorder_point,
                    LastUpdated=current_time,
                )

                self.dc_inventory_snapshots.append(inventory_record)

        print(f"Generated {len(self.dc_inventory_snapshots):,} DC inventory records")
        self._emit_progress(
            "dc_inventory_snapshots",
            1.0,
            "DC inventory snapshots complete",
        )

    async def generate_dc_inventory_snapshots_async(self) -> None:
        """Generate DC inventory snapshots with optional database insertion (async version)."""
        # Call sync method for generation logic
        self.generate_dc_inventory_snapshots()

        # Note: Inventory snapshots don't have DB models yet - CSV only for now
        # When DB models are added, insert logic would go here

    def generate_store_inventory_snapshots(self) -> None:
        """Generate realistic initial inventory snapshots for stores."""
        print("Generating store inventory snapshots...")
        self._emit_progress(
            "store_inventory_snapshots", 0.0, "Generating store inventory snapshots"
        )

        if not self.stores or not self.products_master:
            raise ValueError(
                "Stores and products must be generated before inventory snapshots"
            )

        self.store_inventory_snapshots = []
        current_time = datetime.now()

        total_records = len(self.stores) * len(self.products_master)
        print(f"Generating {total_records:,} store inventory records...")

        for store in self.stores:
            for product in self.products_master:
                # Stores have lower inventory levels than DCs (20-200 vs 500-5000)
                current_qty = random.randint(
                    self.config.volume.store_initial_inventory_min,
                    self.config.volume.store_initial_inventory_max,
                )

                # Store reorder points are typically lower than DCs
                reorder_point = random.randint(
                    self.config.volume.store_reorder_point_min,
                    min(self.config.volume.store_reorder_point_max, current_qty // 3),
                )

                inventory_record = StoreInventorySnapshot(
                    StoreID=store.ID,
                    ProductID=product.ID,
                    CurrentQuantity=current_qty,
                    ReorderPoint=reorder_point,
                    LastUpdated=current_time,
                )

                self.store_inventory_snapshots.append(inventory_record)

        print(
            f"Generated {len(self.store_inventory_snapshots):,} store inventory records"
        )
        self._emit_progress(
            "store_inventory_snapshots",
            1.0,
            "Store inventory snapshots complete",
        )

    async def generate_store_inventory_snapshots_async(self) -> None:
        """Generate store inventory snapshots with optional database insertion (async version)."""
        # Call sync method for generation logic
        self.generate_store_inventory_snapshots()

        # Note: Inventory snapshots don't have DB models yet - CSV only for now
        # When DB models are added, insert logic would go here

    def _cache_master_counts(self) -> None:
        """Cache master table counts for dashboard performance."""
        try:
            cache_manager = CacheManager()

            # Cache counts for each master table
            cache_manager.update_master_table(
                "geographies_master", len(self.geography_master), "Master Data"
            )
            cache_manager.update_master_table("stores", len(self.stores), "Master Data")
            cache_manager.update_master_table(
                "distribution_centers", len(self.distribution_centers), "Master Data"
            )
            cache_manager.update_master_table("trucks", len(self.trucks), "Master Data")
            cache_manager.update_master_table(
                "customers", len(self.customers), "Master Data"
            )
            cache_manager.update_master_table(
                "products_master", len(self.products_master), "Master Data"
            )

            print("Master data counts cached successfully")
        except Exception as e:
            print(f"Warning: Failed to cache counts: {e}")

    def get_generation_summary(self) -> dict[str, Any]:
        """Get summary of generated master data."""
        return {
            "geographies": len(self.geography_master),
            "stores": len(self.stores),
            "distribution_centers": len(self.distribution_centers),
            "customers": len(self.customers),
            "products": len(self.products_master),
            "config": {
                "seed": self.config.seed,
                "target_stores": self.config.volume.stores,
                "target_dcs": self.config.volume.dcs,
                "customers_per_day": self.config.volume.customers_per_day,
            },
            "validation": self.fk_validator.get_validation_summary(),
        }
