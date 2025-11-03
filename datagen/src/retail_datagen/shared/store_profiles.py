"""
Store profiling system for realistic store-level variability.

This module provides comprehensive store profiling to create realistic
variability in transaction volumes, operating hours, basket sizes, and
geographic patterns across the retail network.
"""

import logging
import random
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

logger = logging.getLogger(__name__)


class StoreVolumeClass(str, Enum):
    """Store traffic volume classification."""

    FLAGSHIP = "flagship"  # Top 5% - major metro flagship stores
    HIGH_VOLUME = "high_volume"  # Top 20% - busy urban/suburban
    MEDIUM_VOLUME = "medium_volume"  # Middle 50% - typical suburban
    LOW_VOLUME = "low_volume"  # Bottom 25% - rural/small town
    KIOSK = "kiosk"  # Bottom 5% - express/convenience


class StoreFormat(str, Enum):
    """Physical store format."""

    HYPERMARKET = "hypermarket"  # 150k+ sq ft, full selection
    SUPERSTORE = "superstore"  # 80-150k sq ft, large selection
    STANDARD = "standard"  # 40-80k sq ft, standard selection
    NEIGHBORHOOD = "neighborhood"  # 15-40k sq ft, essentials
    EXPRESS = "express"  # <15k sq ft, convenience


class OperatingHours(str, Enum):
    """Store operating hour patterns."""

    ALWAYS_OPEN = "24/7"  # 24 hours, 7 days
    EXTENDED = "6am-midnight"  # 18 hours
    STANDARD = "8am-10pm"  # 14 hours
    LIMITED = "9am-9pm"  # 12 hours
    REDUCED = "9am-6pm"  # 9 hours (Sundays/holidays)


@dataclass
class StoreProfile:
    """Complete store operating profile."""

    volume_class: StoreVolumeClass
    store_format: StoreFormat
    operating_hours: OperatingHours

    # Traffic multipliers relative to baseline (1.0)
    daily_traffic_multiplier: Decimal  # 0.3 to 3.0

    # Basket characteristics
    avg_basket_size: float  # Items per transaction
    avg_basket_value: Decimal  # Dollar value

    # Operating patterns
    peak_hour_multiplier: Decimal  # Rush hour boost (1.5-2.5)
    weekend_multiplier: Decimal  # Weekend traffic change (0.8-1.3)

    # Geographic context
    is_urban: bool
    is_destination: bool  # Destination shopping vs convenience

    def get_hourly_multiplier(self, hour: int, is_weekend: bool) -> Decimal:
        """
        Get traffic multiplier for specific hour.

        Args:
            hour: Hour of day (0-23)
            is_weekend: Whether it's a weekend day

        Returns:
            Decimal multiplier for the hour
        """
        base_multiplier = Decimal("1.0")

        # Apply weekend adjustment
        if is_weekend:
            base_multiplier *= self.weekend_multiplier

        # Apply hourly patterns based on format
        if self.store_format in [StoreFormat.HYPERMARKET, StoreFormat.SUPERSTORE]:
            # Destination stores - peaks on weekends, evenings
            if 10 <= hour <= 14 or 17 <= hour <= 20:
                base_multiplier *= self.peak_hour_multiplier
            elif hour < 8 or hour > 22:
                base_multiplier *= Decimal("0.3")  # Very quiet early/late
        elif self.store_format in [StoreFormat.NEIGHBORHOOD, StoreFormat.STANDARD]:
            # Neighborhood stores - morning and evening peaks
            if 7 <= hour <= 9 or 17 <= hour <= 19:
                base_multiplier *= self.peak_hour_multiplier
            elif hour < 7 or hour > 21:
                base_multiplier *= Decimal("0.4")
        elif self.store_format == StoreFormat.EXPRESS:
            # Convenience stores - more consistent traffic
            if 7 <= hour <= 9 or 12 <= hour <= 13 or 17 <= hour <= 19:
                base_multiplier *= Decimal("1.3")  # Smaller peaks
            # Express stores don't have as much off-peak drop

        return base_multiplier

    def get_effective_operating_hours(self) -> int:
        """Get effective operating hours per day."""
        hours_map = {
            OperatingHours.ALWAYS_OPEN: 24,
            OperatingHours.EXTENDED: 18,
            OperatingHours.STANDARD: 14,
            OperatingHours.LIMITED: 12,
            OperatingHours.REDUCED: 9,
        }
        return hours_map.get(self.operating_hours, 14)


class StoreProfiler:
    """Assigns profiles to stores based on geographic and demographic characteristics."""

    # Volume class distribution targets (should sum to 1.0)
    VOLUME_DISTRIBUTION = {
        StoreVolumeClass.FLAGSHIP: 0.05,  # 5%
        StoreVolumeClass.HIGH_VOLUME: 0.15,  # 15%
        StoreVolumeClass.MEDIUM_VOLUME: 0.50,  # 50%
        StoreVolumeClass.LOW_VOLUME: 0.25,  # 25%
        StoreVolumeClass.KIOSK: 0.05,  # 5%
    }

    # Traffic multiplier ranges by volume class
    TRAFFIC_MULTIPLIERS = {
        StoreVolumeClass.FLAGSHIP: (Decimal("2.5"), Decimal("3.0")),
        StoreVolumeClass.HIGH_VOLUME: (Decimal("1.8"), Decimal("2.4")),
        StoreVolumeClass.MEDIUM_VOLUME: (Decimal("0.8"), Decimal("1.2")),
        StoreVolumeClass.LOW_VOLUME: (Decimal("0.4"), Decimal("0.7")),
        StoreVolumeClass.KIOSK: (Decimal("0.25"), Decimal("0.35")),
    }

    # Basket size ranges by format
    BASKET_SIZES = {
        StoreFormat.HYPERMARKET: (12.0, 15.0),
        StoreFormat.SUPERSTORE: (8.0, 10.0),
        StoreFormat.STANDARD: (5.0, 7.0),
        StoreFormat.NEIGHBORHOOD: (3.0, 5.0),
        StoreFormat.EXPRESS: (1.5, 3.0),
    }

    # Basket value ranges by format (average transaction)
    BASKET_VALUES = {
        StoreFormat.HYPERMARKET: (Decimal("120"), Decimal("180")),
        StoreFormat.SUPERSTORE: (Decimal("80"), Decimal("120")),
        StoreFormat.STANDARD: (Decimal("40"), Decimal("70")),
        StoreFormat.NEIGHBORHOOD: (Decimal("25"), Decimal("45")),
        StoreFormat.EXPRESS: (Decimal("15"), Decimal("30")),
    }

    def __init__(self, stores, geographies, seed: int = 42):
        """
        Initialize store profiler.

        Args:
            stores: List of Store objects (must have ID, GeographyID)
            geographies: List of GeographyMaster objects (must have ID, Region, State, City)
            seed: Random seed for reproducible profiling
        """
        self.stores = stores
        self.geographies = geographies
        self._rng = random.Random(seed)

        # Build geography lookup
        self._geo_lookup = {geo.ID: geo for geo in geographies}

        # Classify geographies by urban/suburban/rural
        self._urban_classification = self._classify_geographies()

        logger.info(
            f"StoreProfiler initialized for {len(stores)} stores across {len(geographies)} geographies"
        )

    def _classify_geographies(self) -> dict[int, str]:
        """
        Classify geographies as urban, suburban, or rural.

        Uses region and heuristics to determine classification.
        Returns dict mapping geography ID to classification.
        """
        classification = {}

        # Major urban regions (example - this would be more sophisticated in production)
        urban_regions = ["Northeast", "West", "Pacific"]
        suburban_regions = ["Midwest", "South", "Southeast"]

        for geo in self.geographies:
            geo_id = geo.ID

            # Simple heuristic: use region as proxy for density
            if geo.Region in urban_regions:
                # Higher chance of urban in these regions
                urban_prob = 0.5
                suburban_prob = 0.4
            elif geo.Region in suburban_regions:
                urban_prob = 0.2
                suburban_prob = 0.6
            else:
                urban_prob = 0.1
                suburban_prob = 0.5

            rural_prob = 1.0 - urban_prob - suburban_prob

            classification[geo_id] = self._rng.choices(
                ["urban", "suburban", "rural"],
                weights=[urban_prob, suburban_prob, rural_prob],
            )[0]

        return classification

    def _assign_volume_class(self, is_urban: bool) -> StoreVolumeClass:
        """
        Assign volume class with geographic bias.

        Urban stores have higher chance of high volume.
        Rural stores have higher chance of low volume.
        """
        if is_urban:
            # Urban distribution - skew higher
            weights = {
                StoreVolumeClass.FLAGSHIP: 0.10,  # 10% (double base rate)
                StoreVolumeClass.HIGH_VOLUME: 0.25,  # 25%
                StoreVolumeClass.MEDIUM_VOLUME: 0.45,  # 45%
                StoreVolumeClass.LOW_VOLUME: 0.15,  # 15%
                StoreVolumeClass.KIOSK: 0.05,  # 5%
            }
        else:
            # Suburban/rural distribution - skew lower
            weights = {
                StoreVolumeClass.FLAGSHIP: 0.02,  # 2%
                StoreVolumeClass.HIGH_VOLUME: 0.10,  # 10%
                StoreVolumeClass.MEDIUM_VOLUME: 0.53,  # 53%
                StoreVolumeClass.LOW_VOLUME: 0.30,  # 30%
                StoreVolumeClass.KIOSK: 0.05,  # 5%
            }

        volume_classes = list(weights.keys())
        volume_weights = list(weights.values())

        return self._rng.choices(volume_classes, weights=volume_weights)[0]

    def _assign_format(
        self, volume_class: StoreVolumeClass, is_urban: bool
    ) -> StoreFormat:
        """
        Assign store format based on volume class and location.

        Higher volume stores tend to be larger formats.
        Urban stores tend to be smaller formats due to space constraints.
        """
        # Format distribution by volume class
        if volume_class == StoreVolumeClass.FLAGSHIP:
            if is_urban:
                # Urban flagships may be superstore size due to space
                formats = [
                    StoreFormat.SUPERSTORE,
                    StoreFormat.HYPERMARKET,
                    StoreFormat.STANDARD,
                ]
                weights = [0.6, 0.3, 0.1]
            else:
                # Suburban/rural flagships can be hypermarkets
                formats = [
                    StoreFormat.HYPERMARKET,
                    StoreFormat.SUPERSTORE,
                ]
                weights = [0.7, 0.3]

        elif volume_class == StoreVolumeClass.HIGH_VOLUME:
            formats = [
                StoreFormat.SUPERSTORE,
                StoreFormat.STANDARD,
                StoreFormat.HYPERMARKET,
            ]
            weights = [0.6, 0.3, 0.1]

        elif volume_class == StoreVolumeClass.MEDIUM_VOLUME:
            formats = [
                StoreFormat.STANDARD,
                StoreFormat.SUPERSTORE,
                StoreFormat.NEIGHBORHOOD,
            ]
            weights = [0.6, 0.2, 0.2]

        elif volume_class == StoreVolumeClass.LOW_VOLUME:
            formats = [
                StoreFormat.NEIGHBORHOOD,
                StoreFormat.STANDARD,
                StoreFormat.EXPRESS,
            ]
            weights = [0.5, 0.3, 0.2]

        else:  # KIOSK
            formats = [
                StoreFormat.EXPRESS,
                StoreFormat.NEIGHBORHOOD,
            ]
            weights = [0.8, 0.2]

        return self._rng.choices(formats, weights=weights)[0]

    def _assign_operating_hours(
        self, store_format: StoreFormat, is_urban: bool
    ) -> OperatingHours:
        """
        Assign operating hours based on format and location.

        Urban stores and larger formats tend to have extended hours.
        """
        if store_format == StoreFormat.EXPRESS:
            # Express stores often have extended hours
            if is_urban:
                return self._rng.choices(
                    [OperatingHours.ALWAYS_OPEN, OperatingHours.EXTENDED],
                    weights=[0.3, 0.7],
                )[0]
            else:
                return self._rng.choices(
                    [
                        OperatingHours.EXTENDED,
                        OperatingHours.STANDARD,
                        OperatingHours.LIMITED,
                    ],
                    weights=[0.4, 0.4, 0.2],
                )[0]

        elif store_format in [StoreFormat.HYPERMARKET, StoreFormat.SUPERSTORE]:
            # Large stores - typically extended or standard hours
            if is_urban:
                return self._rng.choices(
                    [OperatingHours.EXTENDED, OperatingHours.STANDARD],
                    weights=[0.6, 0.4],
                )[0]
            else:
                return self._rng.choices(
                    [
                        OperatingHours.EXTENDED,
                        OperatingHours.STANDARD,
                        OperatingHours.LIMITED,
                    ],
                    weights=[0.3, 0.5, 0.2],
                )[0]

        else:  # STANDARD, NEIGHBORHOOD
            # Standard stores - mostly standard hours
            return self._rng.choices(
                [
                    OperatingHours.STANDARD,
                    OperatingHours.EXTENDED,
                    OperatingHours.LIMITED,
                ],
                weights=[0.6, 0.2, 0.2],
            )[0]

    def assign_profiles(self) -> dict[int, StoreProfile]:
        """
        Assign profile to each store based on location and characteristics.

        Returns:
            Dictionary mapping store ID to StoreProfile
        """
        profiles = {}

        logger.info(f"Assigning profiles to {len(self.stores)} stores...")

        for store in self.stores:
            # Get geography classification
            geo = self._geo_lookup.get(store.GeographyID)
            if not geo:
                logger.warning(f"Store {store.ID} has invalid GeographyID, skipping")
                continue

            geo_class = self._urban_classification.get(store.GeographyID, "suburban")
            is_urban = geo_class == "urban"

            # Assign volume class
            volume_class = self._assign_volume_class(is_urban)

            # Assign format based on volume
            store_format = self._assign_format(volume_class, is_urban)

            # Assign operating hours
            operating_hours = self._assign_operating_hours(store_format, is_urban)

            # Calculate traffic multiplier
            min_mult, max_mult = self.TRAFFIC_MULTIPLIERS[volume_class]
            traffic_mult = Decimal(
                str(self._rng.uniform(float(min_mult), float(max_mult)))
            )

            # Calculate basket characteristics
            min_size, max_size = self.BASKET_SIZES[store_format]
            avg_basket_size = self._rng.uniform(min_size, max_size)

            min_value, max_value = self.BASKET_VALUES[store_format]
            avg_basket_value = Decimal(
                str(self._rng.uniform(float(min_value), float(max_value)))
            )

            # Calculate peak hour multiplier (higher for destination stores)
            is_destination = store_format in [
                StoreFormat.HYPERMARKET,
                StoreFormat.SUPERSTORE,
            ]
            if is_destination:
                peak_mult = Decimal(str(self._rng.uniform(2.0, 2.5)))
            else:
                peak_mult = Decimal(str(self._rng.uniform(1.5, 2.0)))

            # Calculate weekend multiplier
            if is_destination:
                # Destination stores busier on weekends
                weekend_mult = Decimal(str(self._rng.uniform(1.1, 1.3)))
            elif store_format == StoreFormat.EXPRESS:
                # Convenience stores less busy on weekends
                weekend_mult = Decimal(str(self._rng.uniform(0.8, 0.9)))
            else:
                # Standard stores similar on weekends
                weekend_mult = Decimal(str(self._rng.uniform(0.95, 1.1)))

            profile = StoreProfile(
                volume_class=volume_class,
                store_format=store_format,
                operating_hours=operating_hours,
                daily_traffic_multiplier=traffic_mult,
                avg_basket_size=avg_basket_size,
                avg_basket_value=avg_basket_value,
                peak_hour_multiplier=peak_mult,
                weekend_multiplier=weekend_mult,
                is_urban=is_urban,
                is_destination=is_destination,
            )

            profiles[store.ID] = profile

        # Log distribution summary
        self._log_profile_distribution(profiles)

        return profiles

    def _log_profile_distribution(self, profiles: dict[int, StoreProfile]) -> None:
        """Log summary statistics about profile distribution."""
        if not profiles:
            return

        # Count by volume class
        volume_counts = {}
        for profile in profiles.values():
            vol_class = profile.volume_class.value
            volume_counts[vol_class] = volume_counts.get(vol_class, 0) + 1

        # Count by format
        format_counts = {}
        for profile in profiles.values():
            fmt = profile.store_format.value
            format_counts[fmt] = format_counts.get(fmt, 0) + 1

        # Count by hours
        hours_counts = {}
        for profile in profiles.values():
            hrs = profile.operating_hours.value
            hours_counts[hrs] = hours_counts.get(hrs, 0) + 1

        total = len(profiles)

        logger.info("=== Store Profile Distribution ===")
        logger.info(f"Total stores profiled: {total}")
        logger.info("\nVolume Class Distribution:")
        for vol_class, count in sorted(volume_counts.items()):
            pct = (count / total) * 100
            logger.info(f"  {vol_class}: {count} ({pct:.1f}%)")

        logger.info("\nStore Format Distribution:")
        for fmt, count in sorted(format_counts.items()):
            pct = (count / total) * 100
            logger.info(f"  {fmt}: {count} ({pct:.1f}%)")

        logger.info("\nOperating Hours Distribution:")
        for hrs, count in sorted(hours_counts.items()):
            pct = (count / total) * 100
            logger.info(f"  {hrs}: {count} ({pct:.1f}%)")

        # Traffic multiplier stats
        multipliers = [float(p.daily_traffic_multiplier) for p in profiles.values()]
        avg_mult = sum(multipliers) / len(multipliers)
        min_mult = min(multipliers)
        max_mult = max(multipliers)
        logger.info(
            f"\nTraffic Multipliers: min={min_mult:.2f}, avg={avg_mult:.2f}, max={max_mult:.2f}"
        )

        # Basket size stats
        basket_sizes = [p.avg_basket_size for p in profiles.values()]
        avg_basket = sum(basket_sizes) / len(basket_sizes)
        min_basket = min(basket_sizes)
        max_basket = max(basket_sizes)
        logger.info(
            f"Basket Sizes: min={min_basket:.1f}, avg={avg_basket:.1f}, max={max_basket:.1f} items"
        )
