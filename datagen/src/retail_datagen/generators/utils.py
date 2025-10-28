"""
Utility classes and functions for data generation.

This module provides common utilities for synthetic data generation including
address generation, identifier generation, and geographic distribution
management.
"""

import math
import random
import string
from pathlib import Path

from retail_datagen.shared.models import GeographyDict
from retail_datagen.shared.validators import SyntheticDataValidator


class AddressGenerator:
    """Generates synthetic addresses using geographic data."""

    def __init__(self, geography_data: list[GeographyDict], seed: int = 42):
        """
        Initialize address generator.

        Args:
            geography_data: List of geography dictionary entries
            seed: Random seed for reproducibility
        """
        self.geography_data = geography_data
        self._rng = random.Random(seed)

        # Synthetic street types and names
        self.street_types = [
            "St",
            "Ave",
            "Blvd",
            "Dr",
            "Ln",
            "Ct",
            "Pl",
            "Way",
            "Rd",
            "Cir",
        ]

        self.street_prefixes = ["N", "S", "E", "W", "NE", "NW", "SE", "SW"]

        # Synthetic street name components
        self.street_names = [
            "Maple",
            "Oak",
            "Pine",
            "Cedar",
            "Elm",
            "Birch",
            "Willow",
            "Cherry",
            "Main",
            "First",
            "Second",
            "Third",
            "Fourth",
            "Fifth",
            "Sixth",
            "Park",
            "Church",
            "School",
            "Mill",
            "Hill",
            "Valley",
            "Lake",
            "River",
            "Meadow",
            "Forest",
            "Garden",
            "Spring",
            "Summit",
            "Ridge",
            "Creek",
            "Sunset",
            "Sunrise",
            "Highland",
            "Fairway",
            "Liberty",
            "Lincoln",
            "Washington",
            "Franklin",
            "Madison",
            "Jefferson",
            "Adams",
            "Jackson",
        ]

        # Initialize validator for safety checks
        self.validator = SyntheticDataValidator()

    def generate_address(
        self, geography: GeographyDict, address_type: str = "residential"
    ) -> str:
        """
        Generate a synthetic address for the given geography.

        Args:
            geography: Geography data to use for city/state/zip
            address_type: Type of address (residential, commercial, industrial)

        Returns:
            Synthetic address string
        """
        # Generate street number (100-9999 for residential, higher for commercial)
        if address_type == "residential":
            street_number = self._rng.randint(100, 9999)
        elif address_type == "commercial":
            street_number = self._rng.randint(1000, 99999)
        else:  # industrial
            street_number = self._rng.randint(1, 999)

        # Optionally add prefix
        prefix = ""
        if self._rng.random() < 0.3:  # 30% chance of prefix
            prefix = self._rng.choice(self.street_prefixes) + " "

        # Generate street name
        if self._rng.random() < 0.7:  # 70% chance of single word
            street_name = self._rng.choice(self.street_names)
        else:  # 30% chance of compound
            street_name = (
                self._rng.choice(self.street_names)
                + " "
                + self._rng.choice(self.street_names)
            )

        # Street type
        street_type = self._rng.choice(self.street_types)

        # Compose address
        address = f"{street_number} {prefix}{street_name} {street_type}, {geography.City}, {geography.State} {geography.Zip}"

        # Validate synthetic safety
        if not self.validator.is_synthetic_address(address):
            # Fallback to safe synthetic pattern
            safe_number = self._rng.randint(1000, 9999)
            safe_name = f"Synthetic{safe_number % 1000}"
            address = f"{safe_number} {safe_name} {street_type}, {geography.City}, {geography.State} {geography.Zip}"

        return address


class IdentifierGenerator:
    """Generates synthetic identifiers for various entity types."""

    def __init__(self, seed: int = 42):
        """
        Initialize identifier generator.

        Args:
            seed: Random seed for reproducibility
        """
        self._rng = random.Random(seed)
        self.validator = SyntheticDataValidator()

    def generate_store_number(self, store_id: int) -> str:
        """Generate store number in format S000001."""
        return f"S{store_id:06d}"

    def generate_dc_number(self, dc_id: int) -> str:
        """Generate DC number in format DC001."""
        return f"DC{dc_id:03d}"

    def generate_loyalty_card(self, customer_id: int) -> str:
        """Generate loyalty card number in format LC123456789."""
        # Use customer ID + random padding for uniqueness
        card_number = f"{customer_id:06d}{self._rng.randint(100, 999)}"
        return f"LC{card_number}"

    def generate_phone_number(self) -> str:
        """Generate synthetic phone number."""
        # Use synthetic area codes to ensure safety
        synthetic_area_codes = ["555", "800", "888", "877", "866"]
        area_code = self._rng.choice(synthetic_area_codes)

        # Generate exchange and number
        exchange = f"{self._rng.randint(200, 999)}"
        number = f"{self._rng.randint(1000, 9999)}"

        return f"({area_code}) {exchange}-{number}"

    def generate_ble_id(self, customer_id: int) -> str:
        """Generate BLE ID in format BLE followed by 6 alphanumeric chars.

        Uses customer_id to guarantee uniqueness via base-36 encoding.
        Supports up to 36^6 = 2,176,782,336 unique IDs.
        """
        # Encode customer ID as base-36 (0-9, A-Z) to create unique suffix
        # IMPORTANT: Digits first, then uppercase letters for proper base-36
        chars = string.digits + string.ascii_uppercase

        # Convert customer_id to base-36
        base36 = ""
        num = customer_id
        if num == 0:
            base36 = "0"
        else:
            while num > 0:
                base36 = chars[num % 36] + base36
                num //= 36

        # Left-pad with zeros to reach exactly 6 characters
        base36 = base36.zfill(6)

        # For IDs exceeding 6 chars (>2.1B customers), use modulo to wrap
        if len(base36) > 6:
            num = customer_id % (36 ** 6)
            base36 = ""
            while num > 0:
                base36 = chars[num % 36] + base36
                num //= 36
            base36 = base36.zfill(6)

        return f"BLE{base36}"

    def generate_ad_id(self, customer_id: int) -> str:
        """Generate advertising ID in format AD followed by 6 alphanumeric chars.

        Uses customer_id to guarantee uniqueness via base-36 encoding.
        Supports up to 36^6 = 2,176,782,336 unique IDs.
        """
        # Encode customer ID as base-36 (0-9, A-Z) to create unique suffix
        # IMPORTANT: Digits first, then uppercase letters for proper base-36
        chars = string.digits + string.ascii_uppercase

        # Convert customer_id to base-36
        base36 = ""
        num = customer_id
        if num == 0:
            base36 = "0"
        else:
            while num > 0:
                base36 = chars[num % 36] + base36
                num //= 36

        # Left-pad with zeros to reach exactly 6 characters
        base36 = base36.zfill(6)

        # For IDs exceeding 6 chars (>2.1B customers), use modulo to wrap
        if len(base36) > 6:
            # Use modulo to keep within 6-char space while maintaining determinism
            num = customer_id % (36 ** 6)
            base36 = ""
            while num > 0:
                base36 = chars[num % 36] + base36
                num //= 36
            base36 = base36.zfill(6)

        return f"AD{base36}"

    def generate_license_plate(self, truck_id: int) -> str:
        """Generate synthetic license plate in format ABC1234."""
        # Use truck ID as seed for consistency
        id_rng = random.Random(truck_id)

        # Generate 3 random letters followed by 4 random digits
        letters = "".join(id_rng.choice(string.ascii_uppercase) for _ in range(3))
        digits = "".join(id_rng.choice(string.digits) for _ in range(4))

        return f"{letters}{digits}"


class SyntheticNameGenerator:
    """Generates combinations of synthetic names with safety validation."""

    def __init__(self, first_names: list[str], last_names: list[str], seed: int = 42):
        """
        Initialize name generator.

        Args:
            first_names: List of synthetic first names
            last_names: List of synthetic last names
            seed: Random seed for reproducibility
        """
        self.first_names = first_names
        self.last_names = last_names
        self._rng = random.Random(seed)
        self.validator = SyntheticDataValidator()

        # Validate all names are synthetic
        self._validate_synthetic_names()

    def _validate_synthetic_names(self):
        """Validate that all provided names are synthetic."""
        for first_name in self.first_names:
            if not self.validator.is_synthetic_first_name(first_name):
                raise ValueError(
                    f"First name '{first_name}' appears to be real, not synthetic"
                )

        for last_name in self.last_names:
            if not self.validator.is_synthetic_last_name(last_name):
                raise ValueError(
                    f"Last name '{last_name}' appears to be real, not synthetic"
                )

    def generate_name_pair(self) -> tuple[str, str]:
        """Generate a synthetic first name, last name pair."""
        first_name = self._rng.choice(self.first_names)
        last_name = self._rng.choice(self.last_names)

        # Double-check synthetic safety (paranoid)
        if not (
            self.validator.is_synthetic_first_name(first_name)
            and self.validator.is_synthetic_last_name(last_name)
        ):
            raise ValueError("Generated name failed synthetic validation")

        return first_name, last_name


class GeographicDistribution:
    """Manages geographic distribution of entities based on population patterns."""

    def __init__(self, geography_data: list[GeographyDict], seed: int = 42):
        """
        Initialize geographic distribution manager.

        Args:
            geography_data: List of geography dictionary entries
            seed: Random seed for reproducibility
        """
        self.geography_data = geography_data
        self._rng = random.Random(seed)

        # Create weights based on simplified population model
        self._geography_weights = self._calculate_geography_weights()

    def _calculate_geography_weights(self) -> list[float]:
        """Calculate weights for each geography based on simulated characteristics."""
        weights = []

        for geo in self.geography_data:
            # Simple heuristic: larger regions get more weight
            # In practice, this could use real population data
            base_weight = 1.0

            # Regions with higher population potential
            if geo.Region.lower() in ["west", "south", "northeast"]:
                base_weight *= 1.5

            # Metro areas (simplified detection)
            if any(term in geo.District.lower() for term in ["metro", "urban", "city"]):
                base_weight *= 2.0

            # Add some randomness to avoid completely predictable patterns
            base_weight *= self._rng.uniform(0.8, 1.2)

            weights.append(base_weight)

        # Normalize weights
        total_weight = sum(weights)
        return [w / total_weight for w in weights]

    def select_geography(self) -> GeographyDict:
        """Select a geography based on weighted distribution."""
        return self._rng.choices(self.geography_data, weights=self._geography_weights)[
            0
        ]

    def select_multiple_geographies(
        self, count: int, allow_duplicates: bool = True
    ) -> list[GeographyDict]:
        """
        Select multiple geographies based on weighted distribution.

        Args:
            count: Number of geographies to select
            allow_duplicates: Whether to allow the same geography multiple times

        Returns:
            List of selected geographies
        """
        if allow_duplicates:
            return self._rng.choices(
                self.geography_data, weights=self._geography_weights, k=count
            )
        else:
            # Sample without replacement (up to available geographies)
            max_count = min(count, len(self.geography_data))
            selected_indices = self._rng.choices(
                range(len(self.geography_data)),
                weights=self._geography_weights,
                k=max_count,
            )
            return [self.geography_data[i] for i in selected_indices]

    def distribute_entities_across_geographies(
        self, total_entities: int
    ) -> list[tuple[GeographyDict, int]]:
        """
        Distribute a number of entities across geographies based on weights.

        Args:
            total_entities: Total number of entities to distribute

        Returns:
            List of (geography, entity_count) tuples
        """
        distribution = []
        remaining_entities = total_entities

        for i, geography in enumerate(self.geography_data):
            if i == len(self.geography_data) - 1:
                # Last geography gets all remaining entities
                entity_count = remaining_entities
            else:
                # Allocate based on weight
                expected_count = total_entities * self._geography_weights[i]
                entity_count = max(0, int(round(expected_count)))
                remaining_entities -= entity_count

            if entity_count > 0:
                distribution.append((geography, entity_count))

        return distribution

    def get_strategic_locations(self, count: int) -> list[GeographyDict]:
        """
        Select strategic locations (highest weighted geographies) for stores/DCs.

        Args:
            count: Number of strategic locations to select

        Returns:
            List of strategically selected geographies
        """
        # Sort geographies by weight (descending) and take top N
        geo_weight_pairs = list(zip(self.geography_data, self._geography_weights))
        geo_weight_pairs.sort(key=lambda x: x[1], reverse=True)

        selected_count = min(count, len(geo_weight_pairs))
        return [geo for geo, weight in geo_weight_pairs[:selected_count]]


# DataFrameExporter removed - use SQLAlchemy for data writes, db.migration for CSV reading


class ProgressReporter:
    """Utility for reporting progress during long-running operations."""

    def __init__(self, total_items: int, description: str = "Processing"):
        """
        Initialize progress reporter.

        Args:
            total_items: Total number of items to process
            description: Description of the operation
        """
        self.total_items = total_items
        self.description = description
        self.processed_items = 0
        self.last_reported_percent = 0

    def update(self, increment: int = 1) -> None:
        """
        Update progress and report if significant progress made.

        Args:
            increment: Number of items processed in this update
        """
        self.processed_items += increment

        # Report every 10% or at completion
        current_percent = (self.processed_items / self.total_items) * 100

        if (
            current_percent >= self.last_reported_percent + 10
            or self.processed_items >= self.total_items
        ):
            print(
                f"{self.description}: {self.processed_items}/{self.total_items} "
                f"({current_percent:.1f}%)"
            )

            self.last_reported_percent = math.floor(current_percent / 10) * 10

    def complete(self) -> None:
        """Mark progress as complete and report final status."""
        self.processed_items = self.total_items
        print(f"{self.description}: Complete ({self.total_items}/{self.total_items})")
