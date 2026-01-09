"""
Store master data generation with profiles and geographic distribution.
"""

import logging
from decimal import Decimal

from retail_datagen.shared.models import (
    DistributionCenter,
    GeographyDict,
    GeographyMaster,
    Store,
)
from retail_datagen.shared.store_profiles import StoreProfiler

from ..utils import AddressGenerator, GeographicDistribution, IdentifierGenerator

logger = logging.getLogger(__name__)


class StoreGeneratorMixin:
    """Mixin for store master data generation."""

    def generate_stores(
        self,
        store_count: int,
        geography_master: list[GeographyMaster],
        distribution_centers: list[DistributionCenter],
        selected_geography_data: list[GeographyDict],
        tax_rate_mapping: dict[tuple[str, str], Decimal],
        state_tax_avg: dict[str, Decimal],
        seed: int,
    ) -> list[Store]:
        """
        Generate stores with strategic geographic distribution.

        Args:
            store_count: Number of stores to generate
            geography_master: Geography dimension records
            distribution_centers: DC records for supply chain constraints
            selected_geography_data: Geography dict subset
            tax_rate_mapping: (StateCode, City) -> CombinedRate
            state_tax_avg: StateCode -> average rate
            seed: Random seed

        Returns:
            List of Store records
        """
        print("Generating store data...")

        if not geography_master:
            raise ValueError("Geography master data must be generated first")

        if not distribution_centers:
            raise ValueError("Distribution centers must be generated before stores")

        # Fast lookups for geography
        geo_by_id = {gm.ID: gm for gm in geography_master}

        # Get states where DCs exist to constrain store placement
        dc_states = set()
        for dc in distribution_centers:
            dc_geo = geo_by_id.get(dc.GeographyID)
            if dc_geo:
                dc_states.add(dc_geo.State)

        print(
            f"Constraining stores to {len(dc_states)} states with DCs: "
            f"{sorted(dc_states)}"
        )

        # Filter geography data to only include states with DCs
        dc_constrained_geo_data = [
            geo for geo in selected_geography_data if geo.State in dc_states
        ]

        if not dc_constrained_geo_data:
            raise ValueError(
                "No geography data found in states with distribution centers"
            )

        # Initialize geographic distribution using DC-constrained geographies
        geo_distribution = GeographicDistribution(dc_constrained_geo_data, seed)
        address_generator = AddressGenerator(dc_constrained_geo_data, seed)
        id_generator = IdentifierGenerator(seed)

        # Get strategic locations for stores
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

        stores = []
        current_id = 1

        # Build a fast key index for (City, State, Zip)
        geo_key_index = {
            (gm.City, gm.State, str(gm.ZipCode)): gm for gm in geography_master
        }

        # Place at least one store in each strategic location
        for geo in strategic_geos:
            geo_master = geo_key_index.get((geo.City, geo.State, str(geo.Zip)))
            if geo_master is None:
                raise ValueError("Geography key not found for store placement")

            # Look up tax rate
            tax_rate_key = (geo_master.State, geo_master.City)
            tax_rate = tax_rate_mapping.get(tax_rate_key)
            if tax_rate is None:
                tax_rate = state_tax_avg.get(geo_master.State, Decimal("0.07407"))

            store = Store(
                ID=current_id,
                StoreNumber=id_generator.generate_store_number(current_id),
                Address=address_generator.generate_address(geo, "commercial"),
                GeographyID=geo_master.ID,
                tax_rate=tax_rate,
            )
            stores.append(store)
            current_id += 1

        # Add additional stores based on distribution
        for geo, count in additional_distribution:
            geo_master = geo_key_index.get((geo.City, geo.State, str(geo.Zip)))
            if geo_master is None:
                raise ValueError(
                    "Geography key not found for additional store placement"
                )

            for _ in range(count):
                if current_id > store_count:
                    break

                tax_rate_key = (geo_master.State, geo_master.City)
                tax_rate = tax_rate_mapping.get(tax_rate_key)
                if tax_rate is None:
                    tax_rate = state_tax_avg.get(geo_master.State, Decimal("0.07407"))

                store = Store(
                    ID=current_id,
                    StoreNumber=id_generator.generate_store_number(current_id),
                    Address=address_generator.generate_address(geo, "commercial"),
                    GeographyID=geo_master.ID,
                    tax_rate=tax_rate,
                )
                stores.append(store)
                current_id += 1

        print(f"Generated {len(stores)} store records")

        # Assign store profiles for realistic variability
        print("Assigning store profiles for realistic variability...")
        profiler = StoreProfiler(stores, geography_master, seed)
        store_profiles = profiler.assign_profiles()

        # Update store records with profile information
        for store in stores:
            if store.ID in store_profiles:
                profile = store_profiles[store.ID]
                store.volume_class = profile.volume_class.value
                store.store_format = profile.store_format.value
                store.operating_hours = profile.operating_hours.value
                store.daily_traffic_multiplier = profile.daily_traffic_multiplier

        print(f"Assigned profiles to {len(store_profiles)} stores")
        return stores
