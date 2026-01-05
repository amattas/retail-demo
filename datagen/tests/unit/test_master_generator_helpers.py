"""Unit tests for MasterDataGenerator helper methods.

Tests the refactored helper methods for truck and product generation
to ensure correct behavior and edge case handling.
"""

import pytest
from unittest.mock import MagicMock, patch
from retail_datagen.generators.master_generator import (
    MasterDataGenerator,
    TruckAllocationStrategy,
    AssignedTrucksResult,
    PoolTrucksResult,
    ProductCategoryData,
)


@pytest.fixture
def mock_config():
    """Create a mock configuration for testing."""
    config = MagicMock()
    config.seed = 42
    config.volume.refrigerated_trucks = 60
    config.volume.non_refrigerated_trucks = 40
    config.volume.trucks_per_dc = 10
    config.volume.truck_dc_assignment_rate = 0.5
    config.volume.supplier_refrigerated_trucks = 5
    config.volume.supplier_non_refrigerated_trucks = 5
    config.paths.dictionaries = "data/dictionaries"
    return config


@pytest.fixture
def generator(mock_config):
    """Create a MasterDataGenerator instance for testing with mocked dependencies."""
    with patch.object(MasterDataGenerator, '__init__', lambda self, config: None):
        gen = MasterDataGenerator(mock_config)
        gen.config = mock_config
        gen.distribution_centers = [
            MagicMock(ID=1),
            MagicMock(ID=2),
            MagicMock(ID=3),
        ]
        gen.trucks = []
        gen._rng = MagicMock()
        return gen


class TestTruckAllocationStrategy:
    """Tests for _calculate_truck_allocation_strategy."""

    def test_fixed_allocation_strategy(self, generator):
        """Test fixed allocation with trucks_per_dc configured."""
        result = generator._calculate_truck_allocation_strategy()

        assert isinstance(result, TruckAllocationStrategy)
        assert result.assignment_strategy == "fixed"
        assert result.trucks_per_dc == 10
        # 10 trucks per DC * 3 DCs = 30 assigned
        assert result.num_assigned_trucks == 30
        # Total is 100 (60 + 40), so pool = 100 - 30 = 70
        assert result.num_pool_trucks == 70
        assert result.total_dc_trucks == 100

    def test_percentage_allocation_strategy(self, mock_config):
        """Test percentage-based allocation without trucks_per_dc."""
        mock_config.volume.trucks_per_dc = None
        mock_config.volume.truck_dc_assignment_rate = 0.5

        with patch.object(MasterDataGenerator, '__init__', lambda self, config: None):
            gen = MasterDataGenerator(mock_config)
            gen.config = mock_config
            gen.distribution_centers = [MagicMock(ID=1), MagicMock(ID=2)]

            result = gen._calculate_truck_allocation_strategy()

            assert result.assignment_strategy == "percentage"
            assert result.trucks_per_dc == 0
            # 50% of 100 trucks = 50 assigned
            assert result.num_assigned_trucks == 50
            assert result.num_pool_trucks == 50

    def test_refrigeration_split_accuracy(self, generator):
        """Test that refrigeration split uses round() for accuracy."""
        result = generator._calculate_truck_allocation_strategy()

        # With 60 refrigerated out of 100 total, and 30 assigned trucks,
        # we expect round(30 * 0.6) = 18 refrigerated assigned
        assert result.assigned_refrigerated == 18
        assert result.assigned_non_refrigerated == 12
        # Pool gets the rest: 60 - 18 = 42 refrigerated, 40 - 12 = 28 non-refrigerated
        assert result.pool_refrigerated == 42
        assert result.pool_non_refrigerated == 28

    def test_zero_assigned_trucks(self, mock_config):
        """Test edge case with zero assigned trucks."""
        mock_config.volume.trucks_per_dc = 0

        with patch.object(MasterDataGenerator, '__init__', lambda self, config: None):
            gen = MasterDataGenerator(mock_config)
            gen.config = mock_config
            gen.distribution_centers = [MagicMock(ID=1)]

            result = gen._calculate_truck_allocation_strategy()

            assert result.num_assigned_trucks == 0
            assert result.assigned_refrigerated == 0
            assert result.assigned_non_refrigerated == 0

    def test_zero_total_trucks_no_division_error(self, mock_config):
        """Test that zero total trucks doesn't cause division by zero."""
        mock_config.volume.refrigerated_trucks = 0
        mock_config.volume.non_refrigerated_trucks = 0
        mock_config.volume.trucks_per_dc = None
        mock_config.volume.truck_dc_assignment_rate = 0.5

        with patch.object(MasterDataGenerator, '__init__', lambda self, config: None):
            gen = MasterDataGenerator(mock_config)
            gen.config = mock_config
            gen.distribution_centers = [MagicMock(ID=1)]

            # Should not raise ZeroDivisionError
            result = gen._calculate_truck_allocation_strategy()

            assert result.total_dc_trucks == 0
            assert result.assigned_refrigerated == 0
            assert result.assigned_non_refrigerated == 0


class TestCreateTruck:
    """Tests for _create_truck helper."""

    def test_create_refrigerated_truck(self, generator):
        """Test creating a refrigerated truck."""
        id_gen = MagicMock()
        id_gen.generate_license_plate.return_value = "ABC-1234"

        truck = generator._create_truck(
            truck_id=1,
            id_generator=id_gen,
            is_refrigerated=True,
            dc_id=5,
        )

        assert truck.ID == 1
        assert truck.LicensePlate == "ABC-1234"
        assert truck.Refrigeration is True
        assert truck.DCID == 5

    def test_create_pool_truck(self, generator):
        """Test creating a pool truck with no DC assignment."""
        id_gen = MagicMock()
        id_gen.generate_license_plate.return_value = "XYZ-9999"

        truck = generator._create_truck(
            truck_id=42,
            id_generator=id_gen,
            is_refrigerated=False,
            dc_id=None,
        )

        assert truck.ID == 42
        assert truck.Refrigeration is False
        assert truck.DCID is None


class TestAssignedTrucksGeneration:
    """Tests for _generate_assigned_trucks."""

    def test_fixed_assignment_generates_correct_count(self, generator):
        """Test that fixed assignment generates correct truck counts."""
        id_gen = MagicMock()
        id_gen.generate_license_plate.side_effect = lambda x: f"TRUCK-{x}"

        strategy = TruckAllocationStrategy(
            assignment_strategy="fixed",
            num_assigned_trucks=30,
            num_pool_trucks=70,
            assigned_refrigerated=18,
            assigned_non_refrigerated=12,
            pool_refrigerated=42,
            pool_non_refrigerated=28,
            trucks_per_dc=10,
            total_dc_trucks=100,
        )

        generator.trucks = []
        result = generator._generate_assigned_trucks(id_gen, 1, strategy)

        assert isinstance(result, AssignedTrucksResult)
        assert result.trucks_generated == 30
        assert len(generator.trucks) == 30
        assert len(result.dc_assignment_list) == 30

    def test_percentage_assignment_round_robin(self, generator):
        """Test percentage-based assignment distributes across DCs."""
        id_gen = MagicMock()
        id_gen.generate_license_plate.side_effect = lambda x: f"TRUCK-{x}"

        strategy = TruckAllocationStrategy(
            assignment_strategy="percentage",
            num_assigned_trucks=6,
            num_pool_trucks=4,
            assigned_refrigerated=3,
            assigned_non_refrigerated=3,
            pool_refrigerated=2,
            pool_non_refrigerated=2,
            trucks_per_dc=0,
            total_dc_trucks=10,
        )

        generator.trucks = []
        result = generator._generate_assigned_trucks(id_gen, 1, strategy)

        # Verify round-robin distribution across 3 DCs
        dc_counts = {}
        for _, dc_id, _ in result.dc_assignment_list:
            dc_counts[dc_id] = dc_counts.get(dc_id, 0) + 1

        # Each DC should get 2 trucks (6 trucks / 3 DCs)
        assert all(count == 2 for count in dc_counts.values())


class TestPoolTrucksGeneration:
    """Tests for _generate_pool_trucks."""

    def test_generates_pool_trucks_with_null_dcid(self, generator):
        """Test that pool trucks have DCID=None."""
        id_gen = MagicMock()
        id_gen.generate_license_plate.side_effect = lambda x: f"POOL-{x}"

        strategy = TruckAllocationStrategy(
            assignment_strategy="fixed",
            num_assigned_trucks=0,
            num_pool_trucks=10,
            assigned_refrigerated=0,
            assigned_non_refrigerated=0,
            pool_refrigerated=6,
            pool_non_refrigerated=4,
            trucks_per_dc=0,
            total_dc_trucks=10,
        )

        generator.trucks = []
        result = generator._generate_pool_trucks(id_gen, 1, strategy)

        assert isinstance(result, PoolTrucksResult)
        assert result.trucks_generated == 10
        assert all(truck.DCID is None for truck in generator.trucks)

        # Verify refrigeration counts
        refrigerated = sum(1 for t in generator.trucks if t.Refrigeration)
        assert refrigerated == 6


class TestProductCategoryData:
    """Tests for _organize_products_and_brands_by_category."""

    def test_organizes_data_by_category(self, generator):
        """Test that product and brand data is organized by category."""
        # Mock the data
        generator._company_data = [
            MagicMock(Category="Electronics", Company="TechCorp"),
            MagicMock(Category="Food", Company="FoodCo"),
        ]
        generator._brand_data = [
            MagicMock(Category="Electronics", Brand="TechBrand"),
            MagicMock(Category="Food", Brand="FoodBrand"),
        ]
        generator._product_data = [
            MagicMock(Category="Electronics", Department="Tech", ProductName="Widget"),
            MagicMock(Category="Food", Department="Grocery", ProductName="Snack"),
        ]

        # Mock the category mapping
        generator._map_product_to_brand_category = lambda cat, dept: cat

        result = generator._organize_products_and_brands_by_category()

        assert isinstance(result, ProductCategoryData)
        assert "Electronics" in result.companies_by_category
        assert "Food" in result.companies_by_category
        assert len(result.company_names) == 2
        assert "Electronics" in result.brands_by_category
        assert "Food" in result.brands_by_category


class TestValidBrandProductCombinations:
    """Tests for _create_valid_brand_product_combinations."""

    def test_creates_category_matched_combinations(self, generator):
        """Test that only matching categories create combinations."""
        category_data = ProductCategoryData(
            companies_by_category={"Electronics": ["TechCorp"]},
            company_names=["TechCorp"],
            brands_by_category={
                "Electronics": [(0, MagicMock()), (1, MagicMock())],
                "Food": [(2, MagicMock())],
            },
            products_by_category={
                "Electronics": [(0, MagicMock()), (1, MagicMock()), (2, MagicMock())],
            },
        )

        result = generator._create_valid_brand_product_combinations(category_data)

        # 2 Electronics brands * 3 Electronics products = 6 combinations
        assert len(result) == 6

        # Food brand should not combine with Electronics products
        for product_idx, brand_idx in result:
            assert brand_idx in [0, 1]  # Only Electronics brands


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_distribution_centers(self, mock_config):
        """Test behavior with no distribution centers."""
        with patch.object(MasterDataGenerator, '__init__', lambda self, config: None):
            gen = MasterDataGenerator(mock_config)
            gen.config = mock_config
            gen.distribution_centers = []

            strategy = gen._calculate_truck_allocation_strategy()

            # With 0 DCs, fixed assignment = 10 * 0 = 0 assigned
            assert strategy.num_assigned_trucks == 0

    def test_single_dc(self, mock_config):
        """Test with only one distribution center."""
        with patch.object(MasterDataGenerator, '__init__', lambda self, config: None):
            gen = MasterDataGenerator(mock_config)
            gen.config = mock_config
            gen.distribution_centers = [MagicMock(ID=1)]

            strategy = gen._calculate_truck_allocation_strategy()

            # 10 trucks per DC * 1 DC = 10 assigned
            assert strategy.num_assigned_trucks == 10


class TestProductGenerationSafeguards:
    """Tests for product generation error handling and safeguards."""

    @pytest.fixture
    def product_generator(self, mock_config):
        """Create a generator with all required attributes for product generation."""
        import numpy as np

        with patch.object(MasterDataGenerator, '__init__', lambda self, config: None):
            gen = MasterDataGenerator(mock_config)
            gen.config = mock_config
            gen.config.volume.total_products = 100
            gen._progress_tracker = None
            gen._emit_progress = MagicMock()
            gen._product_data = [MagicMock()]
            gen._brand_data = [MagicMock()]
            gen._company_data = [MagicMock()]
            gen._np_rng = np.random.default_rng(42)
            gen.products_master = []
            gen.fk_validator = MagicMock()
            return gen

    def test_empty_valid_combinations_raises_error(self, product_generator):
        """Test that empty valid_combinations raises ValueError."""
        # Mock the helper methods to return empty combinations
        product_generator._organize_products_and_brands_by_category = MagicMock(
            return_value=ProductCategoryData(
                companies_by_category={},
                company_names=[],
                brands_by_category={},
                products_by_category={},
            )
        )
        product_generator._create_valid_brand_product_combinations = MagicMock(return_value=[])

        with pytest.raises(ValueError, match="No valid brand-product combinations"):
            product_generator.generate_products_master()

    def test_max_attempts_safeguard(self, product_generator):
        """Test that max attempts prevents infinite loop."""
        # Set a small target so the test runs quickly
        product_generator.config.volume.total_products = 10

        # Mock to return valid combinations but always fail product generation
        product_generator._organize_products_and_brands_by_category = MagicMock(
            return_value=ProductCategoryData(
                companies_by_category={"Test": ["TestCo"]},
                company_names=["TestCo"],
                brands_by_category={"Test": [(0, MagicMock())]},
                products_by_category={"Test": [(0, MagicMock())]},
            )
        )
        product_generator._create_valid_brand_product_combinations = MagicMock(
            return_value=[(0, 0)]  # One valid combination
        )
        product_generator._generate_single_product = MagicMock(return_value=None)  # Always fail

        with pytest.raises(RuntimeError, match="Failed to generate .* products after"):
            product_generator.generate_products_master()


class TestSupplierTrucksGeneration:
    """Tests for _generate_supplier_trucks helper."""

    def test_generates_supplier_trucks_with_null_dcid(self, generator):
        """Test that supplier trucks have DCID=None."""
        id_gen = MagicMock()
        id_gen.generate_license_plate.side_effect = lambda x: f"SUPPLIER-{x}"

        generator.trucks = []
        next_id = generator._generate_supplier_trucks(id_gen, 1)

        # 5 refrigerated + 5 non-refrigerated = 10 total
        assert len(generator.trucks) == 10
        assert next_id == 11
        # All supplier trucks should have DCID=None
        assert all(truck.DCID is None for truck in generator.trucks)

    def test_supplier_trucks_refrigeration_split(self, generator):
        """Test that supplier trucks have correct refrigeration split."""
        id_gen = MagicMock()
        id_gen.generate_license_plate.side_effect = lambda x: f"SUPPLIER-{x}"

        generator.trucks = []
        generator._generate_supplier_trucks(id_gen, 1)

        refrigerated = sum(1 for t in generator.trucks if t.Refrigeration)
        non_refrigerated = sum(1 for t in generator.trucks if not t.Refrigeration)

        assert refrigerated == 5  # config.volume.supplier_refrigerated_trucks
        assert non_refrigerated == 5  # config.volume.supplier_non_refrigerated_trucks


class TestGenerateTrucksIntegration:
    """Integration tests for complete generate_trucks workflow."""

    @pytest.fixture
    def full_generator(self, mock_config):
        """Create a generator with full setup for integration testing."""
        import numpy as np

        with patch.object(MasterDataGenerator, '__init__', lambda self, config: None):
            gen = MasterDataGenerator(mock_config)
            gen.config = mock_config
            gen.distribution_centers = [
                MagicMock(ID=1),
                MagicMock(ID=2),
                MagicMock(ID=3),
            ]
            gen.trucks = []
            gen._rng = MagicMock()
            gen._np_rng = np.random.default_rng(42)
            gen._progress_tracker = None
            gen._emit_progress = MagicMock()
            gen.fk_validator = MagicMock()
            return gen

    def test_generate_trucks_total_count(self, full_generator):
        """Test that generate_trucks produces correct total count."""
        full_generator.generate_trucks()

        # Total = DC trucks (60 + 40) + supplier trucks (5 + 5) = 110
        expected_total = 60 + 40 + 5 + 5
        assert len(full_generator.trucks) == expected_total

    def test_generate_trucks_refrigeration_totals(self, full_generator):
        """Test that refrigeration totals match config."""
        full_generator.generate_trucks()

        refrigerated = sum(1 for t in full_generator.trucks if t.Refrigeration)
        non_refrigerated = sum(1 for t in full_generator.trucks if not t.Refrigeration)

        # DC refrigerated (60) + supplier refrigerated (5) = 65
        expected_refrigerated = 60 + 5
        # DC non-refrigerated (40) + supplier non-refrigerated (5) = 45
        expected_non_refrigerated = 40 + 5

        assert refrigerated == expected_refrigerated
        assert non_refrigerated == expected_non_refrigerated

    def test_generate_trucks_unique_ids(self, full_generator):
        """Test that all trucks have unique IDs."""
        full_generator.generate_trucks()

        truck_ids = [t.ID for t in full_generator.trucks]
        assert len(truck_ids) == len(set(truck_ids)), "Truck IDs should be unique"

    def test_generate_trucks_valid_license_plates(self, full_generator):
        """Test that all trucks have non-empty license plates."""
        full_generator.generate_trucks()

        for truck in full_generator.trucks:
            assert truck.LicensePlate is not None
            assert len(truck.LicensePlate) > 0
