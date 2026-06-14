"""
Master data loading and normalization methods.
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from retail_datagen.shared.customer_geography import GeographyAssigner, StoreSelector

from ..fact_generators.models import MasterTableSpec
from ..retail_patterns import (
    CustomerJourneySimulator,
    InventoryFlowSimulator,
    MarketingCampaignSimulator,
)
from .base_types import FactGeneratorBase

logger = logging.getLogger(__name__)


class DataLoadingMixin(FactGeneratorBase):
    """Master data loading and normalization methods."""

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
            truck_capacity=getattr(self.config.volume, "truck_capacity", 15000),
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

        # Build fast AdId -> CustomerID map for marketing join
        try:
            self._adid_to_customer_id = {
                c.AdId: c.ID for c in self.customers if getattr(c, "AdId", None)
            }
        except (AttributeError, TypeError) as e:
            logger.warning(f"Failed to build AdId to CustomerID map: {e}")
            self._adid_to_customer_id = {}

    # Backwards-compatible alias for tests expecting sync loader name

    def load_master_data(self) -> None:
        """Synchronous alias that loads master data from DuckDB."""
        return self.load_master_data_from_duckdb()

    def _master_table_specs(self) -> list[MasterTableSpec]:
        """Deprecated: No longer used (DuckDB-only)."""
        return []

    def _load_master_table(self, master_path: Path, spec: MasterTableSpec) -> list[Any]:
        """Deprecated: CSV master load removed."""
        return []
