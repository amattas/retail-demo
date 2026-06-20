"""Tax rate utilities for proper tax calculation.

This module provides tax rate lookup and calculation utilities for the retail
data generator. It loads tax rates from the Python dictionary in sourcedata
and provides hierarchical lookups (City > County > State).

Note: The current implementation in master_generator.py uses a simple
(State, City) mapping. This module provides an enhanced, more flexible
interface that could be used for future improvements.
"""

import logging
from decimal import Decimal

from retail_datagen.sourcedata.supercenter.tax_rates import TAX_RATES

logger = logging.getLogger(__name__)


class TaxCalculator:
    """Handles tax rate lookups and calculations.

    This class loads tax rates from the Python dictionary and provides
    methods for looking up rates based on geographic location and
    calculating tax amounts with proper decimal precision.

    Implements hierarchical tax rate fallback:
    1. Exact match: State + County + City
    2. County fallback: State + County (average of cities in county)
    3. State fallback: State only (average of all cities in state)
    4. Default fallback: Uses default_rate

    Attributes:
        rate_cache: Dictionary cache of (state, city) -> rate lookups
        county_cache: Dictionary cache of (state, county) -> average rate
        state_cache: Dictionary cache of state -> average rate
        default_rate: Fallback rate when no match is found (7.407%)
    """

    def __init__(
        self,
        tax_rates: list[dict] | None = None,
        default_rate: Decimal = Decimal("0.07407"),
    ):
        """Load tax rates from Python dictionary.

        Args:
            tax_rates: List of tax rate dictionaries (defaults to TAX_RATES)
            default_rate: Default tax rate to use when location not found

        Raises:
            ValueError: If tax_rates has invalid data
        """
        self.tax_rates_data = tax_rates if tax_rates is not None else TAX_RATES
        self.default_rate = default_rate
        self.rate_cache: dict[tuple[str, str], Decimal] = {}
        self.county_cache: dict[tuple[str, str], Decimal] = {}
        self.state_cache: dict[str, Decimal] = {}

        # Load and validate tax rates
        self._load_tax_rates()
        logger.info(
            f"Loaded {len(self.rate_cache)} city rates, "
            f"{len(self.county_cache)} county rates, "
            f"{len(self.state_cache)} state rates"
        )

    def _load_tax_rates(self) -> None:
        """Load tax rates from Python dictionary and build lookup cache.

        Expected dict keys: StateCode, County, City, CombinedRate

        Raises:
            ValueError: If data has missing or invalid columns
        """
        if not self.tax_rates_data:
            raise ValueError("Tax rates data is empty")

        try:
            # Validate required keys in first record
            required_keys = {"StateCode", "County", "City", "CombinedRate"}
            if self.tax_rates_data:
                first_record = self.tax_rates_data[0]
                if not required_keys.issubset(first_record.keys()):
                    missing = required_keys - set(first_record.keys())
                    raise ValueError(f"Missing required keys in tax_rates: {missing}")

            # Build (StateCode, City) -> CombinedRate cache
            # Also collect rates by county and state for fallback calculations
            county_rates: dict[tuple[str, str], list[Decimal]] = {}
            state_rates: dict[str, list[Decimal]] = {}

            for row in self.tax_rates_data:
                state = str(row["StateCode"]).strip().upper()
                county = str(row["County"]).strip()
                city = str(row["City"]).strip()
                rate = Decimal(str(row["CombinedRate"]))

                # Validate rate is in valid range (0-15%)
                if not (Decimal("0") <= rate <= Decimal("0.15")):
                    logger.warning(
                        f"Tax rate out of range for {city}, {state}: {rate} "
                        f"(expected 0-0.15)"
                    )
                    continue

                # City-level cache (primary lookup)
                city_key = (state, city)
                self.rate_cache[city_key] = rate

                # Collect rates for county aggregation
                if county:
                    county_key = (state, county)
                    if county_key not in county_rates:
                        county_rates[county_key] = []
                    county_rates[county_key].append(rate)

                # Collect rates for state aggregation
                if state not in state_rates:
                    state_rates[state] = []
                state_rates[state].append(rate)

            # Build county cache (average of city rates in each county)
            # Use Decimal.quantize() for consistent precision without float
            # conversion. 5 decimal places: tax rates are typically 4 decimals
            # (e.g., 0.0825 = 8.25%) but averaging can produce more; extra
            # precision avoids rounding artifacts
            precision = Decimal("0.00001")
            for county_key, rates in county_rates.items():
                avg_rate = sum(rates) / len(rates)
                self.county_cache[county_key] = avg_rate.quantize(precision)

            # Build state cache (average of all city rates in each state)
            for state_code, rates in state_rates.items():
                avg_rate = sum(rates) / len(rates)
                self.state_cache[state_code] = avg_rate.quantize(precision)

        except Exception as e:
            raise ValueError(f"Error loading tax rates: {e}")

    def get_tax_rate(
        self, state: str, county: str | None = None, city: str | None = None
    ) -> Decimal:
        """Get combined tax rate for a location.

        Implements hierarchical lookup with fallback chain:
        1. Try exact City match (State + City)
        2. Fall back to County average (State + County)
        3. Fall back to State average
        4. Fall back to default rate

        Args:
            state: Two-letter state code (e.g., "CA", "TX")
            county: County name (optional, used for fallback)
            city: City name (optional, primary lookup key)

        Returns:
            Tax rate as Decimal (e.g., Decimal("0.0950") for 9.5%)

        Examples:
            >>> calc = TaxCalculator()
            >>> calc.get_tax_rate("CA", city="Los Angeles")
            Decimal('0.0950')
            >>> calc.get_tax_rate("TX", city="Houston")
            Decimal('0.0825')
            >>> calc.get_tax_rate("CA", county="Los Angeles")  # County fallback
            Decimal('0.09250')  # Average of LA county cities
            >>> calc.get_tax_rate("CA")  # State fallback
            Decimal('0.08500')  # Average of all CA cities
            >>> calc.get_tax_rate("ZZ", city="Unknown")  # Falls back to default
            Decimal('0.07407')
        """
        state = state.strip().upper()

        # Step 1: Try City lookup first (primary method - exact match)
        if city:
            city = city.strip()
            city_key = (state, city)
            if city_key in self.rate_cache:
                logger.debug(
                    f"Tax rate found for {city}, {state}: {self.rate_cache[city_key]}"
                )
                return self.rate_cache[city_key]
            logger.debug(
                f"No city-level tax rate for {city}, {state}. Trying county fallback."
            )

        # Step 2: Try County fallback (average of cities in county)
        if county:
            county = county.strip()
            county_key = (state, county)
            if county_key in self.county_cache:
                rate = self.county_cache[county_key]
                logger.debug(
                    f"Using county-level tax rate for {county}, {state}: {rate}"
                )
                return self.county_cache[county_key]
            logger.debug(
                f"No county-level tax rate for {county}, {state}. "
                "Trying state fallback."
            )

        # Step 3: Try State-only fallback (average of all cities in state)
        if state in self.state_cache:
            logger.debug(
                f"Using state-level tax rate for {state}: {self.state_cache[state]}"
            )
            return self.state_cache[state]

        # Step 4: Return default if no match found at any level
        logger.debug(
            f"No tax rate found for {state}. Using default: {self.default_rate}"
        )
        return self.default_rate

    def calculate_tax(self, amount: Decimal, tax_rate: Decimal) -> Decimal:
        """Calculate tax amount with proper rounding.

        Args:
            amount: Pre-tax amount
            tax_rate: Tax rate as decimal (e.g., 0.0950 for 9.5%)

        Returns:
            Tax amount rounded to 2 decimal places

        Examples:
            >>> calc = TaxCalculator()
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
            >>> calc = TaxCalculator()
            >>> ca_rates = calc.get_all_rates_for_state("CA")
            >>> ca_rates["Los Angeles"]
            Decimal('0.0950')
        """
        state = state.strip().upper()
        return {
            city: rate for (st, city), rate in self.rate_cache.items() if st == state
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
