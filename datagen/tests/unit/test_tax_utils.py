"""Unit tests for tax_utils module."""

from decimal import Decimal
from pathlib import Path

import pytest

from retail_datagen.shared.tax_utils import TaxCalculator


@pytest.fixture
def tax_rates_path():
    """Path to tax rates CSV."""
    return Path("data/dictionaries/tax_rates.csv")


@pytest.fixture
def tax_calculator(tax_rates_path):
    """Create TaxCalculator instance."""
    return TaxCalculator(tax_rates_path)


class TestTaxCalculatorInit:
    """Test TaxCalculator initialization."""

    def test_init_loads_tax_rates(self, tax_rates_path):
        """Test that TaxCalculator loads tax rates on init."""
        calc = TaxCalculator(tax_rates_path)
        assert len(calc.rate_cache) > 0
        assert calc.default_rate == Decimal("0.07407")

    def test_init_with_custom_default(self, tax_rates_path):
        """Test initialization with custom default rate."""
        custom_default = Decimal("0.08")
        calc = TaxCalculator(tax_rates_path, default_rate=custom_default)
        assert calc.default_rate == custom_default

    def test_init_missing_file(self):
        """Test that FileNotFoundError is raised for missing file."""
        with pytest.raises(FileNotFoundError):
            TaxCalculator("nonexistent/path/tax_rates.csv")


class TestGetTaxRate:
    """Test get_tax_rate method."""

    def test_get_tax_rate_california_cities(self, tax_calculator):
        """Test tax rate lookups for California cities."""
        # Los Angeles
        la_rate = tax_calculator.get_tax_rate("CA", city="Los Angeles")
        assert la_rate == Decimal("0.0950")

        # San Francisco
        sf_rate = tax_calculator.get_tax_rate("CA", city="San Francisco")
        assert sf_rate == Decimal("0.0863")

        # San Diego
        sd_rate = tax_calculator.get_tax_rate("CA", city="San Diego")
        assert sd_rate == Decimal("0.0775")

    def test_get_tax_rate_texas_cities(self, tax_calculator):
        """Test tax rate lookups for Texas cities."""
        # Houston
        houston_rate = tax_calculator.get_tax_rate("TX", city="Houston")
        assert houston_rate == Decimal("0.0825")

        # Dallas
        dallas_rate = tax_calculator.get_tax_rate("TX", city="Dallas")
        assert dallas_rate == Decimal("0.0825")

    def test_get_tax_rate_new_york_cities(self, tax_calculator):
        """Test tax rate lookups for New York cities."""
        # NYC
        nyc_rate = tax_calculator.get_tax_rate("NY", city="New York City")
        assert nyc_rate == Decimal("0.0875")

        # Brooklyn (different entry in CSV)
        brooklyn_rate = tax_calculator.get_tax_rate("NY", city="Brooklyn")
        assert brooklyn_rate == Decimal("0.0875")

    def test_get_tax_rate_case_insensitive_state(self, tax_calculator):
        """Test that state code lookup is case insensitive."""
        rate_upper = tax_calculator.get_tax_rate("CA", city="Los Angeles")
        rate_lower = tax_calculator.get_tax_rate("ca", city="Los Angeles")
        rate_mixed = tax_calculator.get_tax_rate("Ca", city="Los Angeles")

        assert rate_upper == rate_lower == rate_mixed

    def test_get_tax_rate_unknown_city(self, tax_calculator):
        """Test that unknown city falls back to state average rate."""
        rate = tax_calculator.get_tax_rate("CA", city="NonexistentCity")
        # Falls back to CA state average, not default rate
        assert rate > Decimal("0")
        assert rate != tax_calculator.default_rate  # Should be state average

    def test_get_tax_rate_unknown_state(self, tax_calculator):
        """Test that unknown state returns default rate."""
        rate = tax_calculator.get_tax_rate("ZZ", city="SomeCity")
        assert rate == tax_calculator.default_rate

    def test_get_tax_rate_no_city_provided(self, tax_calculator):
        """Test that no city provided returns state average rate."""
        rate = tax_calculator.get_tax_rate("CA")
        # Falls back to CA state average, not default rate
        assert rate > Decimal("0")
        # CA state average should be around 8-9%
        assert Decimal("0.07") < rate < Decimal("0.11")

    def test_get_tax_rate_zero_tax_states(self, tax_calculator):
        """Test states with zero sales tax."""
        # Alaska - Anchorage
        ak_rate = tax_calculator.get_tax_rate("AK", city="Anchorage")
        assert ak_rate == Decimal("0.0000")

        # Delaware - Wilmington
        de_rate = tax_calculator.get_tax_rate("DE", city="Wilmington")
        assert de_rate == Decimal("0.0000")

        # Oregon - Portland
        or_rate = tax_calculator.get_tax_rate("OR", city="Portland")
        assert or_rate == Decimal("0.0000")


class TestCalculateTax:
    """Test calculate_tax method."""

    def test_calculate_tax_basic(self, tax_calculator):
        """Test basic tax calculation."""
        amount = Decimal("100.00")
        rate = Decimal("0.0950")
        tax = tax_calculator.calculate_tax(amount, rate)
        assert tax == Decimal("9.50")

    def test_calculate_tax_rounding(self, tax_calculator):
        """Test that tax calculation rounds properly."""
        amount = Decimal("17.99")
        rate = Decimal("0.0825")
        tax = tax_calculator.calculate_tax(amount, rate)
        # 17.99 * 0.0825 = 1.484175, rounds to 1.48
        assert tax == Decimal("1.48")

    def test_calculate_tax_zero_amount(self, tax_calculator):
        """Test tax calculation with zero amount."""
        tax = tax_calculator.calculate_tax(Decimal("0.00"), Decimal("0.0950"))
        assert tax == Decimal("0.00")

    def test_calculate_tax_zero_rate(self, tax_calculator):
        """Test tax calculation with zero rate."""
        tax = tax_calculator.calculate_tax(Decimal("100.00"), Decimal("0.0000"))
        assert tax == Decimal("0.00")

    def test_calculate_tax_precision(self, tax_calculator):
        """Test that tax amounts are always 2 decimal places."""
        amounts = [
            Decimal("99.99"),
            Decimal("1.23"),
            Decimal("1234.56"),
            Decimal("0.01"),
        ]
        rate = Decimal("0.0863")

        for amount in amounts:
            tax = tax_calculator.calculate_tax(amount, rate)
            # Check that result has exactly 2 decimal places
            assert tax == tax.quantize(Decimal("0.01"))


class TestGetAllRatesForState:
    """Test get_all_rates_for_state method."""

    def test_get_all_rates_california(self, tax_calculator):
        """Test getting all rates for California."""
        ca_rates = tax_calculator.get_all_rates_for_state("CA")

        # Should have multiple cities
        assert len(ca_rates) >= 7  # At least 7 CA cities in CSV

        # Check specific cities
        assert "Los Angeles" in ca_rates
        assert "San Francisco" in ca_rates
        assert ca_rates["Los Angeles"] == Decimal("0.0950")

    def test_get_all_rates_texas(self, tax_calculator):
        """Test getting all rates for Texas."""
        tx_rates = tax_calculator.get_all_rates_for_state("TX")

        # Should have multiple cities
        assert len(tx_rates) >= 5

        # Check specific cities
        assert "Houston" in tx_rates
        assert "Dallas" in tx_rates

    def test_get_all_rates_case_insensitive(self, tax_calculator):
        """Test that state lookup is case insensitive."""
        rates_upper = tax_calculator.get_all_rates_for_state("CA")
        rates_lower = tax_calculator.get_all_rates_for_state("ca")

        assert rates_upper == rates_lower

    def test_get_all_rates_unknown_state(self, tax_calculator):
        """Test that unknown state returns empty dict."""
        rates = tax_calculator.get_all_rates_for_state("ZZ")
        assert rates == {}


class TestGetRateStatistics:
    """Test get_rate_statistics method."""

    def test_get_rate_statistics(self, tax_calculator):
        """Test rate statistics calculation."""
        stats = tax_calculator.get_rate_statistics()

        # Check all keys exist
        assert "min" in stats
        assert "max" in stats
        assert "mean" in stats
        assert "median" in stats
        assert "count" in stats

        # Validate ranges
        assert Decimal("0") <= stats["min"] <= Decimal("0.15")
        assert Decimal("0") <= stats["max"] <= Decimal("0.15")
        assert stats["min"] <= stats["mean"] <= stats["max"]
        assert stats["min"] <= stats["median"] <= stats["max"]
        assert stats["count"] > 0


class TestTaxCalculatorIntegration:
    """Integration tests for TaxCalculator."""

    def test_realistic_receipt_calculation(self, tax_calculator):
        """Test calculating tax for a realistic receipt."""
        # Receipt items
        items = [
            Decimal("12.99"),  # Item 1
            Decimal("5.49"),   # Item 2
            Decimal("23.75"),  # Item 3
        ]
        subtotal = sum(items)

        # Get tax rate for Chicago, IL
        tax_rate = tax_calculator.get_tax_rate("IL", city="Chicago")
        assert tax_rate == Decimal("0.1025")  # Chicago has 10.25% tax

        # Calculate tax
        tax_amount = tax_calculator.calculate_tax(subtotal, tax_rate)

        # Total
        total = subtotal + tax_amount

        # Verify
        assert subtotal == Decimal("42.23")
        assert tax_amount == Decimal("4.33")  # 42.23 * 0.1025 = 4.328575
        assert total == Decimal("46.56")

    def test_multiple_locations(self, tax_calculator):
        """Test tax calculations for multiple store locations."""
        purchase_amount = Decimal("100.00")

        locations = [
            ("CA", "Los Angeles", Decimal("9.50")),
            ("TX", "Houston", Decimal("8.25")),
            ("IL", "Chicago", Decimal("10.25")),
            ("WA", "Seattle", Decimal("10.10")),
            ("AK", "Anchorage", Decimal("0.00")),  # No sales tax
        ]

        for state, city, expected_tax in locations:
            rate = tax_calculator.get_tax_rate(state, city=city)
            tax = tax_calculator.calculate_tax(purchase_amount, rate)
            assert tax == expected_tax
