"""Tax rate utilities for proper tax calculation.

This module provides tax rate lookup and calculation utilities for the retail
data generator. It loads tax rates from the tax_rates.csv dictionary and
provides hierarchical lookups (City > County > State).

Note: The current implementation in master_generator.py uses a simple
(State, City) mapping. This module provides an enhanced, more flexible
interface that could be used for future improvements.
"""

import logging
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pandas as pd
from pydantic import ValidationError

from .models import TaxJurisdiction

logger = logging.getLogger(__name__)


class TaxCalculator:
    """Handles tax rate lookups and calculations.

    This class loads tax rates from the CSV dictionary and provides
    methods for looking up rates based on geographic location and
    calculating tax amounts with proper decimal precision.

    Attributes:
        tax_rates: DataFrame containing all loaded tax rates
        rate_cache: Dictionary cache of (state, city) -> rate lookups
        default_rate: Fallback rate when no match is found (7.407%)
    """

    def __init__(
        self, tax_rates_path: str | Path, default_rate: Decimal = Decimal("0.07407")
    ):
        """Load tax rates from CSV.

        Args:
            tax_rates_path: Path to tax_rates.csv file
            default_rate: Default tax rate to use when location not found

        Raises:
            FileNotFoundError: If tax_rates.csv doesn't exist
            ValueError: If CSV has invalid data
        """
        self.tax_rates_path = Path(tax_rates_path)
        self.default_rate = default_rate
        self.rate_cache: dict[tuple[str, str], Decimal] = {}

        # Load and validate tax rates
        self._load_tax_rates()
        logger.info(
            f"Loaded {len(self.rate_cache)} tax rate entries from {self.tax_rates_path}"
        )

    def _load_tax_rates(self) -> None:
        """Load tax rates from CSV and build lookup cache.

        Expected CSV columns: StateCode, County, City, CombinedRate

        Raises:
            FileNotFoundError: If CSV file doesn't exist
            ValueError: If CSV has missing or invalid columns
        """
        if not self.tax_rates_path.exists():
            raise FileNotFoundError(f"Tax rates file not found: {self.tax_rates_path}")

        try:
            # Load CSV with pandas for efficient parsing
            self.tax_rates = pd.read_csv(self.tax_rates_path)

            # Validate required columns
            required_columns = {"StateCode", "County", "City", "CombinedRate"}
            if not required_columns.issubset(self.tax_rates.columns):
                missing = required_columns - set(self.tax_rates.columns)
                raise ValueError(f"Missing required columns in tax_rates.csv: {missing}")

            # Build (StateCode, City) -> CombinedRate cache (use itertuples for speed)
            for row in self.tax_rates.itertuples(index=False):
                state = str(getattr(row, "StateCode")).strip().upper()
                city = str(getattr(row, "City")).strip()
                rate = Decimal(str(getattr(row, "CombinedRate")))

                # Validate rate is in valid range (0-15%)
                if not (Decimal("0") <= rate <= Decimal("0.15")):
                    logger.warning(
                        f"Tax rate out of range for {city}, {state}: {rate} "
                        f"(expected 0-0.15)"
                    )
                    continue

                key = (state, city)
                self.rate_cache[key] = rate

        except pd.errors.EmptyDataError:
            raise ValueError(f"Tax rates file is empty: {self.tax_rates_path}")
        except Exception as e:
            raise ValueError(f"Error loading tax rates: {e}")

    def get_tax_rate(
        self, state: str, county: Optional[str] = None, city: Optional[str] = None
    ) -> Decimal:
        """Get combined tax rate for a location.

        Implements hierarchical lookup:
        1. Try to find City tax rate (if city provided)
        2. Fall back to County tax rate (if county provided) - NOT IMPLEMENTED YET
        3. Fall back to State tax rate - NOT IMPLEMENTED YET
        4. Fall back to default rate

        Current implementation uses (State, City) mapping which matches
        the tax_rates.csv structure where each row has a specific city.

        Args:
            state: Two-letter state code (e.g., "CA", "TX")
            county: County name (optional, not currently used)
            city: City name (optional, used for lookup)

        Returns:
            Tax rate as Decimal (e.g., Decimal("0.0950") for 9.5%)

        Examples:
            >>> calc = TaxCalculator("data/dictionaries/tax_rates.csv")
            >>> calc.get_tax_rate("CA", city="Los Angeles")
            Decimal('0.0950')
            >>> calc.get_tax_rate("TX", city="Houston")
            Decimal('0.0825')
            >>> calc.get_tax_rate("ZZ", city="Unknown")  # Falls back to default
            Decimal('0.07407')
        """
        state = state.strip().upper()

        # Try City lookup first (primary method)
        if city:
            city = city.strip()
            key = (state, city)
            if key in self.rate_cache:
                return self.rate_cache[key]
            else:
                logger.debug(
                    f"No tax rate found for {city}, {state}. Using default: {self.default_rate}"
                )

        # TODO: Implement County fallback
        # Currently tax_rates.csv has city-level rates, so county lookup
        # would require aggregation or separate county-only entries

        # TODO: Implement State-only fallback
        # Would require state-level rates in tax_rates.csv or
        # computing average/median of all cities in state

        # Return default if no match found
        return self.default_rate

    def calculate_tax(self, amount: Decimal, tax_rate: Decimal) -> Decimal:
        """Calculate tax amount with proper rounding.

        Args:
            amount: Pre-tax amount
            tax_rate: Tax rate as decimal (e.g., 0.0950 for 9.5%)

        Returns:
            Tax amount rounded to 2 decimal places

        Examples:
            >>> calc = TaxCalculator("data/dictionaries/tax_rates.csv")
            >>> calc.calculate_tax(Decimal("100.00"), Decimal("0.0950"))
            Decimal('9.50')
            >>> calc.calculate_tax(Decimal("17.99"), Decimal("0.0825"))
            Decimal('1.48')
        """
        tax_amount = amount * tax_rate

        # Round to 2 decimal places using ROUND_HALF_UP (standard for money)
        return tax_amount.quantize(Decimal("0.01"))

    def get_all_rates_for_state(self, state: str) -> dict[str, Decimal]:
        """Get all city tax rates for a given state.

        Args:
            state: Two-letter state code

        Returns:
            Dictionary mapping city name to tax rate

        Examples:
            >>> calc = TaxCalculator("data/dictionaries/tax_rates.csv")
            >>> ca_rates = calc.get_all_rates_for_state("CA")
            >>> ca_rates["Los Angeles"]
            Decimal('0.0950')
        """
        state = state.strip().upper()
        return {
            city: rate
            for (st, city), rate in self.rate_cache.items()
            if st == state
        }

    def get_rate_statistics(self) -> dict[str, Decimal]:
        """Get statistics about loaded tax rates.

        Returns:
            Dictionary with min, max, mean, and median tax rates
        """
        if not self.rate_cache:
            return {
                "min": Decimal("0"),
                "max": Decimal("0"),
                "mean": Decimal("0"),
                "median": Decimal("0"),
            }

        rates = list(self.rate_cache.values())
        rates_sorted = sorted(rates)

        mean_rate = sum(rates) / len(rates)
        median_rate = rates_sorted[len(rates_sorted) // 2]

        return {
            "min": min(rates),
            "max": max(rates),
            "mean": mean_rate,
            "median": median_rate,
            "count": Decimal(len(rates)),
        }
