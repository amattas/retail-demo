"""
Customer master data generation with geographic distribution.
"""

import logging
from typing import Any

from retail_datagen.shared.models import Customer, GeographyDict, GeographyMaster

from ..utils import (
    AddressGenerator,
    GeographicDistribution,
    IdentifierGenerator,
    ProgressReporter,
    SyntheticNameGenerator,
)

logger = logging.getLogger(__name__)


class CustomerGeneratorMixin:
    """Mixin for customer master data generation."""

    def generate_customers(
        self,
        customer_count: int,
        geography_master: list[GeographyMaster],
        selected_geography_data: list[GeographyDict],
        first_names: list[str],
        last_names: list[str],
        seed: int,
        np_rng: Any,
    ) -> list[Customer]:
        """
        Generate customers with realistic geographic distribution.

        Args:
            customer_count: Number of customers to generate
            geography_master: Geography dimension records
            selected_geography_data: Geography dict subset
            first_names: List of first names
            last_names: List of last names
            seed: Random seed
            np_rng: NumPy random generator

        Returns:
            List of Customer records
        """
        print("Generating customer data...")

        if not geography_master:
            raise ValueError("Geography master data must be generated first")

        if not first_names or not last_names:
            raise ValueError("Name data not loaded")

        # Initialize utilities
        geo_distribution = GeographicDistribution(selected_geography_data, seed + 2000)
        address_generator = AddressGenerator(selected_geography_data, seed + 2000)
        SyntheticNameGenerator(first_names, last_names, seed + 2000)
        id_generator = IdentifierGenerator(seed + 2000)

        # Build a fast key index
        geo_key_index = {
            (gm.City, gm.State, str(gm.ZipCode)): gm for gm in geography_master
        }

        # Distribute customers across geographies
        customer_distribution = geo_distribution.distribute_entities_across_geographies(
            customer_count
        )

        customers = []
        current_id = 1

        progress_reporter = ProgressReporter(customer_count, "Generating customers")

        for geo, count in customer_distribution:
            # Find matching geography master record
            geo_master = geo_key_index.get((geo.City, geo.State, str(geo.Zip)))
            if geo_master is None:
                raise ValueError("Geography key not found for customer placement")

            if count <= 0:
                continue

            # Vectorized first/last name sampling
            firsts = np_rng.choice(first_names, size=count, replace=True)
            lasts = np_rng.choice(last_names, size=count, replace=True)

            # Pre-allocate IDs
            ids = list(range(current_id, current_id + count))

            # Addresses
            addresses = [
                address_generator.generate_address(geo, "residential")
                for _ in range(count)
            ]

            # Loyalty/Phone/BLE/Ad IDs
            loyalty_cards = [id_generator.generate_loyalty_card(cid) for cid in ids]
            phones = [id_generator.generate_phone_number() for _ in ids]
            ble_ids = [id_generator.generate_ble_id(cid) for cid in ids]
            ad_ids = [id_generator.generate_ad_id(cid) for cid in ids]

            # Build Customer objects
            customers.extend(
                [
                    Customer(
                        ID=cid,
                        FirstName=str(firsts[i]),
                        LastName=str(lasts[i]),
                        Address=addresses[i],
                        GeographyID=geo_master.ID,
                        LoyaltyCard=loyalty_cards[i],
                        Phone=phones[i],
                        BLEId=ble_ids[i],
                        AdId=ad_ids[i],
                    )
                    for i, cid in enumerate(ids)
                ]
            )

            current_id += count

            # Update progress
            if len(customers) % 5000 == 0:
                progress_reporter.update(5000)

        progress_reporter.complete()

        print(f"Generated {len(customers)} customer records")
        return customers
