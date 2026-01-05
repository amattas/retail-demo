"""
Unit tests for Store Profile Variability.

Tests ensure that:
1. Store profiles are assigned with realistic distribution
2. Traffic multipliers create variability in transaction volumes
3. Coefficient of variation indicates non-uniform distribution
4. Different store volume classes exist in the dataset
5. Master data generator successfully assigns profiles
"""

from collections import Counter
from decimal import Decimal

import pytest

from retail_datagen.shared.models import GeographyMaster, Store
from retail_datagen.shared.store_profiles import (
    StoreFormat,
    StoreProfiler,
    StoreVolumeClass,
)


class TestStoreProfileAssignment:
    """Test store profile assignment logic."""

    @pytest.fixture
    def sample_geographies(self):
        """Create sample geographies for testing."""
        return [
            GeographyMaster(
                ID=1,
                City="Techburg",
                State="CA",
                ZipCode="90001",
                District="Metro",
                Region="West",
            ),
            GeographyMaster(
                ID=2,
                City="Smalltown",
                State="IA",
                ZipCode="50001",
                District="Rural",
                Region="Midwest",
            ),
            GeographyMaster(
                ID=3,
                City="Suburbia",
                State="TX",
                ZipCode="75001",
                District="Metro",
                Region="South",
            ),
            GeographyMaster(
                ID=4,
                City="Villageton",
                State="VT",
                ZipCode="05001",
                District="Rural",
                Region="Northeast",
            ),
        ]

    @pytest.fixture
    def sample_stores(self, sample_geographies):
        """Create sample stores for testing."""
        stores = []
        for i in range(1, 21):  # 20 stores
            geo_id = ((i - 1) % len(sample_geographies)) + 1
            stores.append(
                Store(
                    ID=i,
                    StoreNumber=f"ST{i:03d}",
                    Address=f"{i}00 Main St",
                    GeographyID=geo_id,
                )
            )
        return stores

    @pytest.fixture
    def profiler(self, sample_stores, sample_geographies):
        """Create StoreProfiler instance."""
        return StoreProfiler(sample_stores, sample_geographies, seed=42)

    def test_profiler_initialization(self, profiler, sample_stores, sample_geographies):
        """Test that profiler initializes correctly."""
        assert profiler.stores == sample_stores
        assert profiler.geographies == sample_geographies
        assert len(profiler._geo_lookup) == len(sample_geographies)

    def test_all_stores_get_profiles(self, profiler):
        """Test that all stores receive profile assignments."""
        profiles = profiler.assign_profiles()

        assert len(profiles) == len(profiler.stores)
        assert all(store.ID in profiles for store in profiler.stores)

    def test_profile_has_required_fields(self, profiler):
        """Test that profiles contain all required fields."""
        profiles = profiler.assign_profiles()

        for store_id, profile in profiles.items():
            assert hasattr(profile, 'volume_class')
            assert hasattr(profile, 'store_format')
            assert hasattr(profile, 'operating_hours')
            assert hasattr(profile, 'daily_traffic_multiplier')
            assert hasattr(profile, 'avg_basket_size')
            assert hasattr(profile, 'avg_basket_value')
            assert hasattr(profile, 'peak_hour_multiplier')
            assert hasattr(profile, 'weekend_multiplier')
            assert hasattr(profile, 'is_urban')
            assert hasattr(profile, 'is_destination')

    def test_volume_class_distribution(self, profiler):
        """Test that volume classes are distributed across stores."""
        profiles = profiler.assign_profiles()

        volume_classes = [p.volume_class for p in profiles.values()]
        unique_classes = set(volume_classes)

        # Should have multiple volume classes (not all the same)
        assert len(unique_classes) >= 2, "Should have at least 2 different volume classes"

    def test_traffic_multiplier_variability(self, profiler):
        """Test that traffic multipliers vary across stores."""
        profiles = profiler.assign_profiles()

        multipliers = [float(p.daily_traffic_multiplier) for p in profiles.values()]

        # Should have variation (not all the same)
        assert len(set(multipliers)) > 1, "Traffic multipliers should vary across stores"

        # Should be within expected range (0.25 to 3.0)
        assert all(0.2 <= m <= 3.5 for m in multipliers), "All multipliers should be in valid range"

        # Calculate coefficient of variation (should be > 0.3 for good variability)
        mean_mult = sum(multipliers) / len(multipliers)
        variance = sum((m - mean_mult) ** 2 for m in multipliers) / len(multipliers)
        std_dev = variance ** 0.5
        cv = std_dev / mean_mult

        assert cv > 0.3, f"Coefficient of variation ({cv:.2f}) should be > 0.3 for sufficient variability"

    def test_flagship_stores_have_high_multipliers(self, profiler):
        """Test that flagship stores have higher traffic multipliers."""
        profiles = profiler.assign_profiles()

        flagship_profiles = [
            p for p in profiles.values()
            if p.volume_class == StoreVolumeClass.FLAGSHIP
        ]

        if flagship_profiles:  # Only test if we have flagship stores
            flagship_mults = [float(p.daily_traffic_multiplier) for p in flagship_profiles]

            # All flagship stores should have multipliers >= 2.0
            assert all(m >= 2.0 for m in flagship_mults), "Flagship stores should have high traffic multipliers (>= 2.0)"

    def test_kiosk_stores_have_low_multipliers(self, profiler):
        """Test that kiosk stores have lower traffic multipliers."""
        profiles = profiler.assign_profiles()

        kiosk_profiles = [
            p for p in profiles.values()
            if p.volume_class == StoreVolumeClass.KIOSK
        ]

        if kiosk_profiles:  # Only test if we have kiosk stores
            kiosk_mults = [float(p.daily_traffic_multiplier) for p in kiosk_profiles]

            # All kiosk stores should have multipliers <= 0.5
            assert all(m <= 0.5 for m in kiosk_mults), "Kiosk stores should have low traffic multipliers (<= 0.5)"

    def test_basket_sizes_vary_by_format(self, profiler):
        """Test that basket sizes vary by store format."""
        profiles = profiler.assign_profiles()

        # Group profiles by format
        by_format = {}
        for profile in profiles.values():
            fmt = profile.store_format
            if fmt not in by_format:
                by_format[fmt] = []
            by_format[fmt].append(profile.avg_basket_size)

        # If we have multiple formats, larger formats should have larger baskets
        if StoreFormat.HYPERMARKET in by_format and StoreFormat.EXPRESS in by_format:
            avg_hypermarket = sum(by_format[StoreFormat.HYPERMARKET]) / len(by_format[StoreFormat.HYPERMARKET])
            avg_express = sum(by_format[StoreFormat.EXPRESS]) / len(by_format[StoreFormat.EXPRESS])

            assert avg_hypermarket > avg_express, "Hypermarkets should have larger average basket sizes than express stores"

    def test_operating_hours_assigned(self, profiler):
        """Test that operating hours are assigned to all stores."""
        profiles = profiler.assign_profiles()

        operating_hours = [p.operating_hours for p in profiles.values()]

        # All should have operating hours
        assert all(oh is not None for oh in operating_hours)

        # Should have at least some variety
        unique_hours = set(operating_hours)
        assert len(unique_hours) >= 2, "Should have at least 2 different operating hour patterns"


class TestStoreProfileVariabilityIntegration:
    """Integration tests for store profile variability in fact generation."""

    def test_stores_with_profiles_have_multipliers(self):
        """Test that stores can have profile data attached."""
        # Create a sample store with profile data
        store = Store(
            ID=1,
            StoreNumber="ST001",
            Address="123 Main St",
            GeographyID=1,
            volume_class="flagship",
            store_format="hypermarket",
            operating_hours="extended",
            daily_traffic_multiplier=Decimal("2.5"),
        )

        assert store.daily_traffic_multiplier == Decimal("2.5")
        assert store.volume_class == "flagship"
        assert store.store_format == "hypermarket"

    def test_store_without_profile_has_none_multiplier(self):
        """Test that stores without profiles have None for multiplier."""
        store = Store(
            ID=1,
            StoreNumber="ST001",
            Address="123 Main St",
            GeographyID=1,
        )

        assert store.daily_traffic_multiplier is None
        assert store.volume_class is None


class TestTrafficMultiplierRanges:
    """Test that traffic multiplier ranges are appropriate for each volume class."""

    @pytest.fixture
    def sample_geographies(self):
        """Create sample geographies."""
        return [
            GeographyMaster(
                ID=i,
                City=f"City{i}",
                State="CA",
                ZipCode=f"9000{i}",
                District="Metro",
                Region="West",
            )
            for i in range(1, 101)
        ]

    @pytest.fixture
    def large_store_set(self, sample_geographies):
        """Create a large set of stores for statistical testing."""
        stores = []
        for i in range(1, 201):  # 200 stores
            geo_id = ((i - 1) % len(sample_geographies)) + 1
            stores.append(
                Store(
                    ID=i,
                    StoreNumber=f"ST{i:03d}",
                    Address=f"{i}00 Main St",
                    GeographyID=geo_id,
                )
            )
        return stores

    def test_large_dataset_variability(self, large_store_set, sample_geographies):
        """Test variability across a large dataset of stores."""
        profiler = StoreProfiler(large_store_set, sample_geographies, seed=42)
        profiles = profiler.assign_profiles()

        multipliers = [float(p.daily_traffic_multiplier) for p in profiles.values()]

        # Statistical checks
        mean_mult = sum(multipliers) / len(multipliers)
        min_mult = min(multipliers)
        max_mult = max(multipliers)

        # Should span a wide range
        assert max_mult - min_mult >= 2.0, f"Range ({max_mult - min_mult:.2f}) should be at least 2.0"

        # Mean should be reasonable (around 1.0-1.5)
        assert 0.8 <= mean_mult <= 1.8, f"Mean multiplier ({mean_mult:.2f}) should be in reasonable range"

        # Coefficient of variation
        variance = sum((m - mean_mult) ** 2 for m in multipliers) / len(multipliers)
        std_dev = variance ** 0.5
        cv = std_dev / mean_mult

        assert cv >= 0.5, f"Coefficient of variation ({cv:.2f}) should be >= 0.5 for good variability"

    def test_volume_class_percentages(self, large_store_set, sample_geographies):
        """Test that volume class distribution is roughly as expected."""
        profiler = StoreProfiler(large_store_set, sample_geographies, seed=42)
        profiles = profiler.assign_profiles()

        volume_classes = [p.volume_class for p in profiles.values()]
        counts = Counter(volume_classes)
        total = len(volume_classes)

        # Check rough percentages (allow for randomness)
        # FLAGSHIP: ~5%, HIGH: ~15%, MEDIUM: ~50%, LOW: ~25%, KIOSK: ~5%

        if StoreVolumeClass.FLAGSHIP in counts:
            flagship_pct = counts[StoreVolumeClass.FLAGSHIP] / total
            assert 0.02 <= flagship_pct <= 0.15, f"Flagship percentage ({flagship_pct:.2%}) should be 2-15%"

        if StoreVolumeClass.MEDIUM_VOLUME in counts:
            medium_pct = counts[StoreVolumeClass.MEDIUM_VOLUME] / total
            assert 0.30 <= medium_pct <= 0.70, f"Medium volume percentage ({medium_pct:.2%}) should be 30-70%"
