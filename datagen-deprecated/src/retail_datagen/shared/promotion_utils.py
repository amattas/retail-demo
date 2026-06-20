"""
Promotion system for retail data generator.

Implements realistic promotional discount patterns including seasonal promotions,
category-based discounts, customer segment targeting, and minimum purchase thresholds.
"""

import random
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum


class PromotionType(str, Enum):
    """Types of promotional campaigns."""

    SAVE10 = "SAVE10"  # 10% off
    SAVE15 = "SAVE15"  # 15% off $25+
    SAVE20 = "SAVE20"  # 20% off $50+
    SAVE25 = "SAVE25"  # 25% off $75+
    BOGO50 = "BOGO50"  # Buy one get one 50% off
    CLEARANCE30 = "CLEARANCE30"  # 30% clearance
    CLEARANCE50 = "CLEARANCE50"  # 50% clearance
    BFRIDAY30 = "BFRIDAY30"  # Black Friday 30% off
    BFRIDAY40 = "BFRIDAY40"  # Black Friday 40% off
    SUMMER25 = "SUMMER25"  # Summer sale 25% off
    BACKTOSCHOOL = "BACKTOSCHOOL"  # Back to school 20% off
    HOLIDAY20 = "HOLIDAY20"  # Holiday season 20% off
    NEWYEAR15 = "NEWYEAR15"  # New Year clearance 15% off


@dataclass
class PromotionConfig:
    """Configuration for a promotion type."""

    code: str
    discount_pct: Decimal
    min_purchase: Decimal
    description: str
    eligible_months: list[int] | None = None  # None = all year
    eligible_categories: list[str] | None = None  # None = all categories
    max_discount_amount: Decimal | None = None  # Cap on total discount


# Promotion configurations
PROMOTION_CONFIGS = {
    PromotionType.SAVE10: PromotionConfig(
        code="SAVE10",
        discount_pct=Decimal("0.10"),
        min_purchase=Decimal("0.00"),
        description="10% off your purchase",
    ),
    PromotionType.SAVE15: PromotionConfig(
        code="SAVE15",
        discount_pct=Decimal("0.15"),
        min_purchase=Decimal("25.00"),
        description="15% off $25 or more",
    ),
    PromotionType.SAVE20: PromotionConfig(
        code="SAVE20",
        discount_pct=Decimal("0.20"),
        min_purchase=Decimal("50.00"),
        description="20% off $50 or more",
    ),
    PromotionType.SAVE25: PromotionConfig(
        code="SAVE25",
        discount_pct=Decimal("0.25"),
        min_purchase=Decimal("75.00"),
        description="25% off $75 or more",
    ),
    PromotionType.BOGO50: PromotionConfig(
        code="BOGO50",
        discount_pct=Decimal("0.50"),
        min_purchase=Decimal("0.00"),
        description="Buy one get one 50% off",
    ),
    PromotionType.CLEARANCE30: PromotionConfig(
        code="CLEARANCE30",
        discount_pct=Decimal("0.30"),
        min_purchase=Decimal("0.00"),
        description="Clearance: 30% off select items",
        eligible_categories=["clothing", "electronics", "home_garden"],
    ),
    PromotionType.CLEARANCE50: PromotionConfig(
        code="CLEARANCE50",
        discount_pct=Decimal("0.50"),
        min_purchase=Decimal("0.00"),
        description="Final clearance: 50% off",
        eligible_categories=["clothing", "electronics"],
        eligible_months=[1, 2, 7, 8],  # January, February, July, August
    ),
    PromotionType.BFRIDAY30: PromotionConfig(
        code="BFRIDAY30",
        discount_pct=Decimal("0.30"),
        min_purchase=Decimal("0.00"),
        description="Black Friday: 30% off",
        eligible_months=[11],  # November only
    ),
    PromotionType.BFRIDAY40: PromotionConfig(
        code="BFRIDAY40",
        discount_pct=Decimal("0.40"),
        min_purchase=Decimal("100.00"),
        description="Black Friday: 40% off $100+",
        eligible_months=[11],  # November only
    ),
    PromotionType.SUMMER25: PromotionConfig(
        code="SUMMER25",
        discount_pct=Decimal("0.25"),
        min_purchase=Decimal("0.00"),
        description="Summer sale: 25% off",
        eligible_months=[6, 7, 8],  # June, July, August
    ),
    PromotionType.BACKTOSCHOOL: PromotionConfig(
        code="BACKTOSCHOOL",
        discount_pct=Decimal("0.20"),
        min_purchase=Decimal("0.00"),
        description="Back to school: 20% off",
        eligible_months=[8, 9],  # August, September
        eligible_categories=["clothing", "electronics"],
    ),
    PromotionType.HOLIDAY20: PromotionConfig(
        code="HOLIDAY20",
        discount_pct=Decimal("0.20"),
        min_purchase=Decimal("0.00"),
        description="Holiday savings: 20% off",
        eligible_months=[12],  # December only
    ),
    PromotionType.NEWYEAR15: PromotionConfig(
        code="NEWYEAR15",
        discount_pct=Decimal("0.15"),
        min_purchase=Decimal("0.00"),
        description="New Year clearance: 15% off",
        eligible_months=[1, 2],  # January, February
    ),
}


class PromotionEngine:
    """
    Engine for applying promotional discounts to shopping baskets.

    Handles promotion selection based on temporal patterns, customer segments,
    product categories, and minimum purchase requirements.
    """

    def __init__(self, seed: int = 42):
        """
        Initialize promotion engine.

        Args:
            seed: Random seed for reproducible promotion selection
        """
        self._rng = random.Random(seed)

        # Base promotion application rate (10-15% of receipts)
        self._base_promotion_rate = 0.125  # 12.5% baseline

        # Customer segment promotion usage rates
        self._segment_promotion_rates = {
            "BUDGET_CONSCIOUS": 0.30,  # Budget shoppers actively seek deals
            "CONVENIENCE_FOCUSED": 0.08,  # Convenience shoppers rarely use promos
            "QUALITY_SEEKER": 0.10,  # Quality shoppers occasionally use promos
            "BRAND_LOYAL": 0.15,  # Brand loyal moderate promo usage
        }

        # Category-specific promotion rates (relative to base)
        self._category_promotion_multipliers = {
            "food": 0.6,  # Lower promotion rate for groceries
            "beverages": 0.7,
            "household": 0.8,
            "personal_care": 0.9,
            "electronics": 1.2,  # Higher promotion rate
            "clothing": 1.8,  # Much higher for clothing
            "home_garden": 1.1,
            "other": 1.0,
        }

        # Seasonal promotion rate multipliers
        self._seasonal_multipliers = {
            1: 1.3,  # January - New Year clearance
            2: 1.2,  # February - Post-holiday clearance
            3: 0.9,  # March - Lower activity
            4: 1.0,  # April - Normal
            5: 1.0,  # May - Normal
            6: 1.1,  # June - Summer sales start
            7: 1.3,  # July - Peak summer sales
            8: 1.4,  # August - Back to school + clearance
            9: 1.1,  # September - Back to school tail
            10: 1.0,  # October - Normal
            11: 2.0,  # November - Black Friday / Cyber Monday
            12: 1.6,  # December - Holiday sales
        }

    def should_apply_promotion(
        self,
        customer_segment: str,
        transaction_date: datetime,
        basket_categories: list[str],
    ) -> bool:
        """
        Determine if a promotion should be applied to a transaction.

        Args:
            customer_segment: Customer segment (BUDGET_CONSCIOUS, etc.)
            transaction_date: Date of transaction
            basket_categories: List of product categories in basket

        Returns:
            True if promotion should be applied
        """
        # Get base rate for customer segment
        segment_rate = self._segment_promotion_rates.get(
            customer_segment, self._base_promotion_rate
        )

        # Apply seasonal multiplier
        month = transaction_date.month
        seasonal_multiplier = self._seasonal_multipliers.get(month, 1.0)

        # Calculate average category multiplier for basket
        if basket_categories:
            category_multipliers = [
                self._category_promotion_multipliers.get(cat, 1.0)
                for cat in basket_categories
            ]
            avg_category_multiplier = sum(category_multipliers) / len(
                category_multipliers
            )
        else:
            avg_category_multiplier = 1.0

        # Calculate final promotion probability
        final_rate = segment_rate * seasonal_multiplier * avg_category_multiplier

        # Cap at 60% to maintain realism
        final_rate = min(0.60, final_rate)

        return self._rng.random() < final_rate

    def select_promotion(
        self,
        transaction_date: datetime,
        basket_subtotal: Decimal,
        basket_categories: list[str],
    ) -> PromotionConfig | None:
        """
        Select an appropriate promotion for a transaction.

        Args:
            transaction_date: Date of transaction
            basket_subtotal: Total basket value before discount
            basket_categories: List of product categories in basket

        Returns:
            Selected promotion config or None if no eligible promotion
        """
        month = transaction_date.month

        # Filter eligible promotions
        eligible_promos = []
        for promo_type, config in PROMOTION_CONFIGS.items():
            # Check month eligibility
            if config.eligible_months and month not in config.eligible_months:
                continue

            # Check minimum purchase requirement
            if basket_subtotal < config.min_purchase:
                continue

            # Check category eligibility
            if config.eligible_categories:
                # At least one basket item must be in eligible category
                if not any(
                    cat in config.eligible_categories for cat in basket_categories
                ):
                    continue

            eligible_promos.append(config)

        if not eligible_promos:
            return None

        # Weight promotions by discount percentage (prefer larger discounts)
        weights = [float(promo.discount_pct) for promo in eligible_promos]
        return self._rng.choices(eligible_promos, weights=weights)[0]

    def apply_promotion_to_basket(
        self,
        basket_items: list[dict],
        promo_config: PromotionConfig,
        product_categories: dict[int, str],
    ) -> tuple[Decimal, list[dict]]:
        """
        Apply promotion to basket items and calculate discounts.

        Args:
            basket_items: List of basket items (dicts with product, qty, subtotal)
            promo_config: Promotion configuration to apply
            product_categories: Mapping of product IDs to categories

        Returns:
            Tuple of (total_discount_amount, list of items with promo codes applied)
        """
        total_discount = Decimal("0.00")
        items_with_promos = []

        for item in basket_items:
            product_id = item["product"].ID
            category = product_categories.get(product_id, "other")
            line_subtotal = Decimal(str(item["subtotal"]))

            # Check if item is eligible for this promotion
            if promo_config.eligible_categories:
                if category not in promo_config.eligible_categories:
                    # Item not eligible, no promo code
                    items_with_promos.append(
                        {**item, "promo_code": None, "discount": Decimal("0.00")}
                    )
                    continue

            # BOGO50 special logic: apply 50% to every other item
            if promo_config.code == "BOGO50":
                qty = item["qty"]
                # If quantity is even, discount half of items
                # If odd, discount floor(qty/2) items
                discounted_qty = qty // 2
                if discounted_qty > 0:
                    unit_price = line_subtotal / qty
                    line_discount = (
                        unit_price
                        * Decimal(str(discounted_qty))
                        * promo_config.discount_pct
                    ).quantize(Decimal("0.01"))
                else:
                    line_discount = Decimal("0.00")
            else:
                # Standard percentage discount
                line_discount = (line_subtotal * promo_config.discount_pct).quantize(
                    Decimal("0.01")
                )

            total_discount += line_discount

            items_with_promos.append(
                {
                    **item,
                    "promo_code": promo_config.code,
                    "discount": line_discount,
                }
            )

        # Apply max discount cap if configured
        if (
            promo_config.max_discount_amount
            and total_discount > promo_config.max_discount_amount
        ):
            # Proportionally reduce all line discounts
            reduction_factor = promo_config.max_discount_amount / total_discount
            total_discount = promo_config.max_discount_amount

            # Adjust individual line discounts
            for item in items_with_promos:
                if item.get("discount", Decimal("0.00")) > Decimal("0.00"):
                    item["discount"] = (item["discount"] * reduction_factor).quantize(
                        Decimal("0.01")
                    )

        return total_discount, items_with_promos

    def get_promotion_metadata(
        self, promo_config: PromotionConfig, discount_amount: Decimal
    ) -> dict:
        """
        Generate metadata for promotion event (for future streaming).

        Args:
            promo_config: Promotion configuration applied
            discount_amount: Total discount amount applied

        Returns:
            Dictionary with promotion event metadata
        """
        return {
            "promo_code": promo_config.code,
            "discount_pct": str(promo_config.discount_pct),
            "discount_amount": str(discount_amount),
            "description": promo_config.description,
            "min_purchase": str(promo_config.min_purchase),
        }
