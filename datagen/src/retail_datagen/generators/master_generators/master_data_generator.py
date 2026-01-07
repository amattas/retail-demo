"""
Master data generation orchestrator.

Coordinates all dimension table generation using modular mixins.
"""

import logging
import random
from decimal import Decimal
from typing import Any

import numpy as np

from retail_datagen.config.models import RetailConfig
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

from ..progress_tracker import TableProgressTracker
from .base_generator import (
    BaseGenerator,
    CustomerModel,
    DistributionCenterModel,
    GeographyModel,
    ProductModel,
    StoreModel,
    TruckModel,
)
from .customer_generator import CustomerGeneratorMixin
from .distribution_generator import DistributionGeneratorMixin
from .geography_generator import GeographyGeneratorMixin
from .inventory_generator import InventoryGeneratorMixin
from .product_generator import ProductGeneratorMixin
from .store_generator import StoreGeneratorMixin

logger = logging.getLogger(__name__)


class MasterDataGenerator(
    BaseGenerator,
    GeographyGeneratorMixin,
    StoreGeneratorMixin,
    DistributionGeneratorMixin,
    CustomerGeneratorMixin,
    ProductGeneratorMixin,
    InventoryGeneratorMixin,
):
    """
    Main master data generation engine.

    Generates all dimension tables from dictionary data:
    - geographies_master.csv
    - stores.csv
    - distribution_centers.csv
    - trucks.csv
    - customers.csv
    - products_master.csv
    """

    def __init__(self, config: RetailConfig):
        """
        Initialize master data generator.

        Args:
            config: Retail configuration containing generation parameters
        """
        super().__init__(config)

        # Initialize random number generators with seed
        self._rng = random.Random(config.seed)
        self._np_rng = np.random.default_rng(config.seed + 777)

        # Initialize dictionary loader
        self.dictionary_loader = DictionaryLoader(config.paths.dictionaries)

        # Initialize data holders
        self._geography_data: list[GeographyDict] | None = None
        self._selected_geography_data: list[GeographyDict] | None = None
        self._first_names: list[str] | None = None
        self._last_names: list[str] | None = None
        self._product_data: list[ProductDict] | None = None
        self._brand_data: list[ProductBrandDict] | None = None
        self._company_data: list[ProductCompanyDict] | None = None
        self._product_tags_overlay: dict[str, str] = {}
        self._tax_rate_mapping: dict[tuple[str, str], Decimal] = {}
        self._state_tax_avg: dict[str, Decimal] = {}

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

        # Database session (for backward compatibility)
        self._db_session = None

        print(f"MasterDataGenerator initialized with seed {config.seed}")

    def _load_dictionary_data(self) -> None:
        """Load all required dictionary data from CSV files."""
        from pathlib import Path

        print("Loading dictionary data...")
        print(
            f"Dictionary path: {Path(self.dictionary_loader.dictionary_path).resolve()}"
        )

        # Load geographies
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
                f"  Sample brand: '{raw_brand_data[0].Brand}' | "
                f"Company={getattr(raw_brand_data[0], 'Company', None)} | "
                f"Category='{raw_brand_data[0].Category}'"
            )
        self._brand_data = raw_brand_data

        self._company_data = self.dictionary_loader.load_product_companies()
        print(f"Loaded {len(self._company_data)} companies")
        if self._company_data:
            print(
                f"  Sample company: '{self._company_data[0].Company}' | "
                f"Category='{self._company_data[0].Category}'"
            )

        # Optional product tag overlay
        try:
            tag_result = self.dictionary_loader.load_dictionary("product_tags")
            overlay = {}
            for entry in tag_result.data:
                name = getattr(entry, "ProductName", None)
                t = getattr(entry, "Tags", None)
                if name and t:
                    overlay[name] = t
            self._product_tags_overlay = overlay
            if overlay:
                print(f"Loaded {len(overlay)} product tag overlays")
        except Exception as e:
            logger.warning(f"Failed to load product tag overlays: {e}")
            self._product_tags_overlay = {}

        # Load tax rates
        tax_jurisdictions = self.dictionary_loader.load_tax_rates()
        print(f"Loaded {len(tax_jurisdictions)} tax jurisdictions")

        # Create (StateCode, City) -> CombinedRate mapping
        for tax_jurisdiction in tax_jurisdictions:
            key = (tax_jurisdiction.StateCode, tax_jurisdiction.City)
            self._tax_rate_mapping[key] = tax_jurisdiction.CombinedRate

        print(f"Created tax rate mapping with {len(self._tax_rate_mapping)} entries")

        # Build state-level average rates
        if tax_jurisdictions:
            state_groups: dict[str, list[Decimal]] = {}
            for tj in tax_jurisdictions:
                state_groups.setdefault(tj.StateCode, []).append(tj.CombinedRate)
            for state, rates in state_groups.items():
                try:
                    avg = sum(rates) / Decimal(str(len(rates)))
                except (ZeroDivisionError, ArithmeticError) as e:
                    logger.warning(
                        f"Failed to calculate average tax rate for {state}: {e}, using default"
                    )
                    avg = Decimal("0.07407")
                self._state_tax_avg[state] = avg
            print(
                f"Computed state-level tax averages for {len(self._state_tax_avg)} states"
            )

        if self._tax_rate_mapping:
            sample_key = list(self._tax_rate_mapping.keys())[0]
            sample_rate = self._tax_rate_mapping[sample_key]
            print(f"  Sample: {sample_key[1]}, {sample_key[0]} -> {sample_rate:.4f}")

        print("Dictionary data loading complete")

    async def generate_all_master_data_async(self, session: Any | None) -> None:
        """
        Generate all master data tables and write to DuckDB.

        Args:
            session: Unused; retained for backward compatibility
        """
        print("Starting master data generation...")

        # Store session for backward compatibility
        self._db_session = session

        # Load dictionary data
        self._load_dictionary_data()

        # Initialize progress tracker
        master_table_names = [
            "geographies_master",
            "stores",
            "distribution_centers",
            "trucks",
            "customers",
            "products_master",
            "dc_inventory_snapshots",
            "store_inventory_snapshots",
        ]
        self._progress_tracker = TableProgressTracker(master_table_names)

        # Read entity counts from configuration
        stores_count = self.config.volume.stores

        # Phase 1: Sequential (geographic dependencies)
        print("\nPhase 1: Generating geographic dependencies (sequential)...")
        await self.generate_geography_master_async()
        await self.generate_distribution_centers_async()
        await self.generate_stores_async(count=stores_count)
        await self.generate_trucks_async()

        # Phase 2: Generate customers and products
        print("\nPhase 2: Generating customers and products...")
        await self.generate_customers_async()
        await self.generate_products_master_async()

        # Phase 3: Generate inventory snapshots
        print("\nPhase 3: Generating inventory snapshots...")
        await self.generate_dc_inventory_snapshots_async()
        await self.generate_store_inventory_snapshots_async()

        # Validate foreign key relationships
        self._validate_foreign_keys()

        # Mark all tables as completed
        if self._progress_tracker:
            self._progress_tracker.mark_generation_complete()
            logger.info("All master tables marked as completed")

        # Cache counts
        self._cache_master_counts(
            len(self.geography_master),
            len(self.stores),
            len(self.distribution_centers),
            len(self.trucks),
            len(self.customers),
            len(self.products_master),
        )

        print("Master data generation complete!")

    def generate_all_master_data(self, session: Any | None = None) -> None:
        """Synchronous entrypoint that delegates to the async implementation."""
        import asyncio

        return asyncio.run(self.generate_all_master_data_async(session))

    # -------------------------------------------------------------------------
    # Async wrappers for each table generation
    # -------------------------------------------------------------------------

    async def generate_geography_master_async(self) -> None:
        """Generate geography master data with database insertion."""
        if self._progress_tracker:
            self._progress_tracker.mark_table_started("geographies_master")
        self._emit_progress("geographies_master", 0.0, "Generating geographies")

        geography_count = self.config.volume.total_geographies
        self.geography_master, self._selected_geography_data = (
            self.generate_geography_master(
                self._geography_data, geography_count, self._rng
            )
        )

        # Register with FK validator
        geography_ids = [geo.ID for geo in self.geography_master]
        self.fk_validator.register_geography_ids(geography_ids)

        # Write to DuckDB
        await self._insert_to_db(None, GeographyModel, self.geography_master)

        if self._progress_tracker:
            self._progress_tracker.mark_table_completed("geographies_master")
        self._emit_progress("geographies_master", 1.0, "Geographies complete")

    async def generate_stores_async(self, count: int | None = None) -> None:
        """Generate stores data with database insertion."""
        if self._progress_tracker:
            self._progress_tracker.mark_table_started("stores")
        self._emit_progress("stores", 0.0, "Generating stores")

        store_count = count if count is not None else self.config.volume.stores
        self.stores = self.generate_stores(
            store_count,
            self.geography_master,
            self.distribution_centers,
            self._selected_geography_data,
            self._tax_rate_mapping,
            self._state_tax_avg,
            self.config.seed,
        )

        # Register with FK validator
        store_ids = [store.ID for store in self.stores]
        self.fk_validator.register_store_ids(store_ids)

        # Write to DuckDB
        await self._insert_to_db(None, StoreModel, self.stores)

        if self._progress_tracker:
            self._progress_tracker.mark_table_completed("stores")
        self._emit_progress(
            "stores", 1.0, "Stores complete", {"stores": len(self.stores)}
        )

    async def generate_distribution_centers_async(self) -> None:
        """Generate distribution centers with database insertion."""
        if self._progress_tracker:
            self._progress_tracker.mark_table_started("distribution_centers")
        self._emit_progress(
            "distribution_centers", 0.0, "Generating distribution centers"
        )

        dc_count = self.config.volume.dcs
        self.distribution_centers = self.generate_distribution_centers(
            dc_count,
            self.geography_master,
            self._selected_geography_data,
            self.config.seed,
        )

        # Register with FK validator
        dc_ids = [dc.ID for dc in self.distribution_centers]
        self.fk_validator.register_dc_ids(dc_ids)

        # Write to DuckDB
        await self._insert_to_db(
            None, DistributionCenterModel, self.distribution_centers
        )

        if self._progress_tracker:
            self._progress_tracker.mark_table_completed("distribution_centers")
        self._emit_progress(
            "distribution_centers",
            1.0,
            "Distribution centers complete",
            {"distribution_centers": len(self.distribution_centers)},
        )

    async def generate_trucks_async(self) -> None:
        """Generate trucks with database insertion."""
        if self._progress_tracker:
            self._progress_tracker.mark_table_started("trucks")
        self._emit_progress("trucks", 0.0, "Generating trucks")

        self.trucks = self.generate_trucks(
            self.config, self.distribution_centers, self.config.seed
        )

        # Register with FK validator
        truck_ids = [truck.ID for truck in self.trucks]
        self.fk_validator.register_truck_ids(truck_ids)

        # Write to DuckDB
        await self._insert_to_db(None, TruckModel, self.trucks)

        if self._progress_tracker:
            self._progress_tracker.mark_table_completed("trucks")
        self._emit_progress("trucks", 1.0, "Trucks complete")

    async def generate_customers_async(self) -> None:
        """Generate customers with database insertion."""
        if self._progress_tracker:
            self._progress_tracker.mark_table_started("customers")
        self._emit_progress("customers", 0.0, "Generating customers")

        customer_count = self.config.volume.total_customers
        self.customers = self.generate_customers(
            customer_count,
            self.geography_master,
            self._selected_geography_data,
            self._first_names,
            self._last_names,
            self.config.seed,
            self._np_rng,
        )

        # Register with FK validator
        customer_ids = [customer.ID for customer in self.customers]
        self.fk_validator.register_customer_ids(customer_ids)

        # Write to DuckDB
        await self._insert_to_db(
            None, CustomerModel, self.customers, batch_size=5000, commit_every_batches=1
        )

        if self._progress_tracker:
            self._progress_tracker.mark_table_completed("customers")
        self._emit_progress("customers", 1.0, "Customers complete")

    async def generate_products_master_async(self) -> None:
        """Generate products with database insertion."""
        if self._progress_tracker:
            self._progress_tracker.mark_table_started("products_master")
        self._emit_progress("products_master", 0.0, "Generating products")

        target_product_count = self.config.volume.total_products
        self.products_master = self.generate_products_master(
            target_product_count,
            self._product_data,
            self._brand_data,
            self._company_data,
            self._product_tags_overlay,
            self.pricing_calculator,
            self.config.historical.start_date,
            self._rng,
            self._np_rng,
        )

        # Register with FK validator
        product_ids = [product.ID for product in self.products_master]
        self.fk_validator.register_product_ids(product_ids)

        # Write to DuckDB
        await self._insert_to_db(
            None,
            ProductModel,
            self.products_master,
            batch_size=2000,
            commit_every_batches=1,
        )

        if self._progress_tracker:
            self._progress_tracker.mark_table_completed("products_master")
        self._emit_progress("products_master", 1.0, "Products master complete")

    async def generate_dc_inventory_snapshots_async(self) -> None:
        """Generate DC inventory snapshots."""
        if self._progress_tracker:
            self._progress_tracker.mark_table_started("dc_inventory_snapshots")
        self._emit_progress(
            "dc_inventory_snapshots", 0.0, "Generating DC inventory snapshots"
        )

        self.dc_inventory_snapshots = self.generate_dc_inventory_snapshots(
            self.distribution_centers, self.products_master, self.config, self._np_rng
        )

        # Note: Inventory snapshots don't have DB models yet - CSV only
        self._emit_progress(
            "dc_inventory_snapshots", 1.0, "DC inventory snapshots complete"
        )

    async def generate_store_inventory_snapshots_async(self) -> None:
        """Generate store inventory snapshots."""
        if self._progress_tracker:
            self._progress_tracker.mark_table_started("store_inventory_snapshots")
        self._emit_progress(
            "store_inventory_snapshots", 0.0, "Generating store inventory snapshots"
        )

        self.store_inventory_snapshots = self.generate_store_inventory_snapshots(
            self.stores, self.products_master, self.config, self._np_rng
        )

        # Note: Inventory snapshots don't have DB models yet - CSV only
        self._emit_progress(
            "store_inventory_snapshots", 1.0, "Store inventory snapshots complete"
        )

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

        # Validate customer geography references (sample check)
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
