"""
Pricing validators and calculators.

Handles pricing calculations and validation according to AGENTS.md specifications:
- MSRP = BasePrice +/-15%
- SalePrice = MSRP (60% of time) OR discounted 5-35% (40% of time)
- Cost = 50-85% of SalePrice
- Always ensure: Cost < SalePrice <= MSRP
"""

import random
from decimal import Decimal


class PricingCalculator:
    """
    Handles pricing calculations according to AGENTS.md specifications:

    - MSRP = BasePrice +/-15%
    - SalePrice = MSRP (60% of time) OR discounted 5-35% (40% of time)
    - Cost = 50-85% of SalePrice
    - Always ensure: Cost < SalePrice <= MSRP
    """

    def __init__(self, seed: int = 42):
        """Initialize with random seed for reproducibility."""
        self._rng = random.Random(seed)

    def calculate_msrp(self, base_price: Decimal) -> Decimal:
        """
        Calculate MSRP as BasePrice +/-15%.

        Args:
            base_price: Base price from product dictionary

        Returns:
            MSRP within +/-15% of base price

        Raises:
            ValueError: If base price is <= 0
        """
        if base_price <= 0:
            raise ValueError("Base price must be positive")

        # Generate random percentage between -15% and +15%
        variance_percent = Decimal(str(self._rng.uniform(-0.15, 0.15)))
        msrp = base_price * (Decimal("1.0") + variance_percent)

        # Ensure MSRP is positive and round to 2 decimal places
        msrp = max(Decimal("0.01"), msrp.quantize(Decimal("0.01")))

        return msrp

    def calculate_sale_price(self, msrp: Decimal) -> Decimal:
        """
        Calculate sale price with 60/40 distribution:
        - 60%: SalePrice = MSRP
        - 40%: SalePrice = MSRP discounted 5-35%

        Args:
            msrp: Manufacturer suggested retail price

        Returns:
            Sale price according to distribution rules
        """
        if msrp <= 0:
            raise ValueError("MSRP must be positive")

        # 60% chance to keep MSRP as sale price
        if self._rng.random() < 0.60:
            return msrp

        # 40% chance to apply discount of 5-35%
        discount_percent = Decimal(str(self._rng.uniform(0.05, 0.35)))
        sale_price = msrp * (Decimal("1.0") - discount_percent)

        # Ensure sale price is positive and round to 2 decimal places
        sale_price = max(Decimal("0.01"), sale_price.quantize(Decimal("0.01")))

        return sale_price

    def calculate_cost(self, sale_price: Decimal) -> Decimal:
        """
        Calculate cost as 50-85% of sale price.

        Args:
            sale_price: Sale price

        Returns:
            Cost between 50-85% of sale price
        """
        if sale_price <= 0:
            raise ValueError("Sale price must be positive")

        # Generate random percentage between 50% and 85%
        cost_percent = Decimal(str(self._rng.uniform(0.50, 0.85)))
        cost = sale_price * cost_percent

        # Ensure cost is less than sale price and round to 2 decimal places
        cost = min(cost, sale_price - Decimal("0.01"))
        cost = max(Decimal("0.01"), cost.quantize(Decimal("0.01")))

        return cost

    def calculate_full_pricing(self, base_price: Decimal) -> dict[str, Decimal]:
        """
        Calculate complete pricing structure from base price.

        Args:
            base_price: Base price from product dictionary

        Returns:
            Dictionary with Cost, MSRP, and SalePrice

        Raises:
            ValueError: If pricing constraints are violated
        """
        msrp = self.calculate_msrp(base_price)
        sale_price = self.calculate_sale_price(msrp)
        cost = self.calculate_cost(sale_price)

        # Validate final constraints
        if not (cost < sale_price <= msrp):
            raise ValueError(
                f"Pricing constraint violation: "
                f"Cost ({cost}) < SalePrice ({sale_price}) <= MSRP ({msrp})"
            )

        return {"Cost": cost, "MSRP": msrp, "SalePrice": sale_price}

    def calculate_batch_pricing(
        self, base_prices: list[Decimal]
    ) -> list[dict[str, Decimal]]:
        """
        Calculate pricing for multiple products efficiently.

        Args:
            base_prices: List of base prices

        Returns:
            List of pricing dictionaries
        """
        return [self.calculate_full_pricing(price) for price in base_prices]
