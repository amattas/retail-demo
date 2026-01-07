"""
Geography master data generation.

Handles selection and creation of geography dimension records.
"""

import logging
from typing import Any

from retail_datagen.shared.dictionary_loader import DictionaryLoader
from retail_datagen.shared.models import GeographyDict, GeographyMaster

logger = logging.getLogger(__name__)


class GeographyGeneratorMixin:
    """Mixin for geography master data generation."""

    def _load_geography_data(
        self, dictionary_loader: DictionaryLoader
    ) -> list[GeographyDict]:
        """Load geography dictionary data."""
        geography_data = dictionary_loader.load_geographies()
        print(f"Loaded {len(geography_data)} geographies")
        return geography_data

    def generate_geography_master(
        self,
        geography_data: list[GeographyDict],
        geography_count: int,
        rng: Any,
    ) -> tuple[list[GeographyMaster], list[GeographyDict]]:
        """
        Generate geography master data.

        Args:
            geography_data: All available geography records
            geography_count: Number of geographies to generate
            rng: Random number generator

        Returns:
            Tuple of (geography_master list, selected geography data subset)
        """
        print("Generating geography master data...")

        available_count = len(geography_data)
        if geography_count > available_count:
            print(
                f"Warning: Requested {geography_count} geographies but only {available_count} available. Using all."
            )
            geography_count = available_count

        # Select a subset using random sampling for consistency
        selected_geographies = rng.sample(geography_data, geography_count)

        geography_master = []
        for i, geo_dict in enumerate(selected_geographies, 1):
            geography = GeographyMaster(
                ID=i,
                City=geo_dict.City,
                State=geo_dict.State,
                ZipCode=str(geo_dict.Zip),
                District=geo_dict.District,
                Region=geo_dict.Region,
            )
            geography_master.append(geography)

        print(f"Generated {len(geography_master)} geography master records")
        return geography_master, selected_geographies
