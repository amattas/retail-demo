"""
Customer journey simulation for realistic shopping behavior.

This module models different customer segments, shopping behaviors, and realistic
product combinations based on customer preferences and store patterns.
"""

import logging
import random
from datetime import datetime
from decimal import Decimal

from retail_datagen.shared.models import Customer, ProductMaster, Store
from retail_datagen.shared.promotion_utils import PromotionEngine

from .common import (
    CAMPAIGN_START_PROBABILITY,
    CustomerSegment,
    ShoppingBasket,
    ShoppingBehaviorType,
)

logger = logging.getLogger(__name__)


class CustomerJourneySimulator:
    """
    Simulates realistic customer shopping journeys and basket composition.

    Models different customer segments, shopping behaviors, and realistic
    product combinations based on customer preferences and store patterns.
    """

    def __init__(
        self,
        customers: list[Customer],
        products: list[ProductMaster],
        stores: list[Store],
        seed: int = 42,
    ):
        """
        Initialize customer journey simulator.

        Args:
            customers: List of customer dimension records
            products: List of product master records
            stores: List of store dimension records
            seed: Random seed for reproducible simulations
        """
        self.customers = customers
        self.products = products
        self.stores = stores
        self._rng = random.Random(seed)

        # Build product categories for realistic basket composition
        self._product_categories = self._categorize_products()

        # Initialize promotion engine
        self._promotion_engine = PromotionEngine(seed=seed)

        # Build product ID to category mapping for promotions
        self._product_category_map = {
            product.ID: self._get_product_category(product)
            for product in self.products
        }

        # Customer segment distribution
        self._segment_distribution = {
            CustomerSegment.BUDGET_CONSCIOUS: 0.35,
            CustomerSegment.CONVENIENCE_FOCUSED: 0.25,
            CustomerSegment.QUALITY_SEEKER: 0.20,
            CustomerSegment.BRAND_LOYAL: 0.20,
        }

        # Shopping behavior probabilities by segment
        self._behavior_by_segment = {
            CustomerSegment.BUDGET_CONSCIOUS: {
                ShoppingBehaviorType.QUICK_TRIP: 0.2,
                ShoppingBehaviorType.GROCERY_RUN: 0.4,
                ShoppingBehaviorType.FAMILY_SHOPPING: 0.3,
                ShoppingBehaviorType.BULK_SHOPPING: 0.1,
            },
            CustomerSegment.CONVENIENCE_FOCUSED: {
                ShoppingBehaviorType.QUICK_TRIP: 0.5,
                ShoppingBehaviorType.GROCERY_RUN: 0.3,
                ShoppingBehaviorType.FAMILY_SHOPPING: 0.15,
                ShoppingBehaviorType.BULK_SHOPPING: 0.05,
            },
            CustomerSegment.QUALITY_SEEKER: {
                ShoppingBehaviorType.QUICK_TRIP: 0.3,
                ShoppingBehaviorType.GROCERY_RUN: 0.35,
                ShoppingBehaviorType.FAMILY_SHOPPING: 0.25,
                ShoppingBehaviorType.BULK_SHOPPING: 0.1,
            },
            CustomerSegment.BRAND_LOYAL: {
                ShoppingBehaviorType.QUICK_TRIP: 0.25,
                ShoppingBehaviorType.GROCERY_RUN: 0.4,
                ShoppingBehaviorType.FAMILY_SHOPPING: 0.25,
                ShoppingBehaviorType.BULK_SHOPPING: 0.1,
            },
        }

        # Cache customer segments for consistency
        self._customer_segments = self._assign_customer_segments()

    def _get_product_category(self, product: ProductMaster) -> str:
        """Get category for a single product."""
        product_name_lower = product.ProductName.lower()

        category_keywords = {
            "food": [
                "bread",
                "milk",
                "cheese",
                "meat",
                "fruit",
                "vegetable",
                "snack",
                "cereal",
                "pasta",
            ],
            "beverages": ["juice", "soda", "water", "coffee", "tea", "beer", "wine"],
            "household": [
                "detergent",
                "soap",
                "paper",
                "towel",
                "cleaner",
                "trash",
                "dish",
            ],
            "personal_care": [
                "shampoo",
                "toothpaste",
                "deodorant",
                "lotion",
                "cosmetic",
            ],
            "electronics": [
                "phone",
                "computer",
                "tablet",
                "tv",
                "speaker",
                "headphone",
                "charger",
            ],
            "clothing": [
                "shirt",
                "pants",
                "dress",
                "shoe",
                "jacket",
                "underwear",
                "sock",
            ],
            "home_garden": ["plant", "tool", "furniture", "decor", "garden", "outdoor"],
        }

        for category, keywords in category_keywords.items():
            if any(keyword in product_name_lower for keyword in keywords):
                return category

        return "other"

    def _categorize_products(self) -> dict[str, list[ProductMaster]]:
        """Categorize products based on their names for realistic grouping."""
        categories = {
            "food": [],
            "beverages": [],
            "household": [],
            "personal_care": [],
            "electronics": [],
            "clothing": [],
            "home_garden": [],
            "other": [],
        }

        for product in self.products:
            category = self._get_product_category(product)
            categories[category].append(product)

        return categories

    def _assign_customer_segments(self) -> dict[int, CustomerSegment]:
        """Assign customer segments consistently."""
        segments = {}
        segment_options = list(self._segment_distribution.keys())
        segment_weights = list(self._segment_distribution.values())

        for customer in self.customers:
            # Use customer ID as seed for consistency
            customer_rng = random.Random(customer.ID)
            segment = customer_rng.choices(segment_options, weights=segment_weights)[0]
            segments[customer.ID] = segment

        return segments

    def get_customer_segment(self, customer_id: int) -> CustomerSegment:
        """Get customer segment for a given customer ID."""
        return self._customer_segments.get(
            customer_id, CustomerSegment.CONVENIENCE_FOCUSED
        )

    def select_shopping_behavior(self, customer_id: int) -> ShoppingBehaviorType:
        """Select shopping behavior based on customer segment."""
        segment = self.get_customer_segment(customer_id)
        behavior_probs = self._behavior_by_segment[segment]

        behaviors = list(behavior_probs.keys())
        weights = list(behavior_probs.values())

        return self._rng.choices(behaviors, weights=weights)[0]

    def generate_shopping_basket(
        self, customer_id: int, behavior_type: ShoppingBehaviorType | None = None, store: Store | None = None
    ) -> ShoppingBasket:
        """
        Generate a realistic shopping basket for a customer.

        Args:
            customer_id: Customer ID
            behavior_type: Override shopping behavior (optional)
            store: Store where shopping is occurring (optional, for format-based adjustment)

        Returns:
            ShoppingBasket with realistic product combinations

        Raises:
            ValueError: If no products are available to generate a basket
        """
        segment = self.get_customer_segment(customer_id)
        if behavior_type is None:
            behavior_type = self.select_shopping_behavior(customer_id)

        # Determine basket size based on behavior
        basket_sizes = {
            ShoppingBehaviorType.QUICK_TRIP: (1, 3),
            ShoppingBehaviorType.GROCERY_RUN: (5, 15),
            ShoppingBehaviorType.FAMILY_SHOPPING: (10, 30),
            ShoppingBehaviorType.BULK_SHOPPING: (20, 50),
        }

        min_items, max_items = basket_sizes[behavior_type]

        # Adjust basket size based on store format if provided
        if store and hasattr(store, 'store_format') and store.store_format:
            # Clamp express explicitly to 1-3 items per profile spec
            if store.store_format == 'express':
                min_items, max_items = 1, 3
            else:
                format_multipliers = {
                    'hypermarket': 1.3,   # Larger baskets in hypermarkets
                    'superstore': 1.1,    # Slightly larger in superstores
                    'standard': 1.0,      # Baseline
                    'neighborhood': 0.8,  # Smaller in neighborhood stores
                }
                multiplier = format_multipliers.get(store.store_format, 1.0)
                min_items = max(1, int(min_items * multiplier))
                max_items = max(min_items, int(max_items * multiplier))

        target_items = self._rng.randint(min_items, max_items)

        # Select products based on behavior and segment
        basket_items = self._select_basket_products(
            segment, behavior_type, target_items
        )

        # CRITICAL: Validate that basket has at least 1 item
        # This prevents empty receipts which violate business rules
        if not basket_items or len(basket_items) == 0:
            # Fallback: Add a random product if basket is somehow empty
            if not self.products:
                raise ValueError("Cannot generate basket: no products available")

            # Add at least one random product
            random_product = self._rng.choice(self.products)
            basket_items = [(random_product, 1)]
            logger.warning(
                f"Empty basket generated for customer {customer_id}, "
                f"added fallback product {random_product.ID}"
            )

        # Calculate totals
        total_items = sum(qty for _, qty in basket_items)
        estimated_total = sum(product.SalePrice * qty for product, qty in basket_items)

        return ShoppingBasket(
            items=basket_items,
            behavior_type=behavior_type,
            customer_segment=segment,
            total_items=total_items,
            estimated_total=estimated_total,
        )

    def _select_basket_products(
        self,
        segment: CustomerSegment,
        behavior: ShoppingBehaviorType,
        target_items: int,
    ) -> list[tuple[ProductMaster, int]]:
        """Select products for basket based on segment and behavior."""
        basket_items = []
        remaining_items = target_items

        # Category preferences by behavior type
        category_preferences = {
            ShoppingBehaviorType.QUICK_TRIP: {
                "food": 0.4,
                "beverages": 0.3,
                "personal_care": 0.2,
                "household": 0.1,
            },
            ShoppingBehaviorType.GROCERY_RUN: {
                "food": 0.5,
                "beverages": 0.2,
                "household": 0.15,
                "personal_care": 0.15,
            },
            ShoppingBehaviorType.FAMILY_SHOPPING: {
                "food": 0.4,
                "household": 0.2,
                "personal_care": 0.15,
                "clothing": 0.1,
                "electronics": 0.05,
                "beverages": 0.1,
            },
            ShoppingBehaviorType.BULK_SHOPPING: {
                "food": 0.3,
                "household": 0.3,
                "personal_care": 0.2,
                "beverages": 0.1,
                "other": 0.1,
            },
        }

        # Segment price preferences - used to filter product selection
        # Values < 1.0: prefer lower prices (use bottom X% of price range)
        # Values = 1.0: no price filtering (all products equally likely)
        # Values > 1.0: prefer higher prices (use top X% of price range)
        price_modifiers = {
            CustomerSegment.BUDGET_CONSCIOUS: 0.7,  # Prefer bottom 70% of prices
            CustomerSegment.CONVENIENCE_FOCUSED: 1.0,  # No price filtering
            CustomerSegment.QUALITY_SEEKER: 1.3,  # Prefer top 30% of prices
            CustomerSegment.BRAND_LOYAL: 1.1,  # Prefer top 50% of prices
        }

        price_modifier = price_modifiers[segment]
        preferences = category_preferences[behavior]

        # Select products from preferred categories
        for category, preference in preferences.items():
            if remaining_items <= 0:
                break

            category_items = max(1, int(target_items * preference))
            category_items = min(category_items, remaining_items)

            if (
                category in self._product_categories
                and self._product_categories[category]
            ):
                available_products = self._product_categories[category]

                # Filter products by price preference using the price_modifier
                if price_modifier < 1.0:
                    # Budget-conscious: prefer lower-priced products
                    # Use bottom (price_modifier * 100)% of price range
                    # e.g., modifier=0.7 means include bottom 70% of prices
                    sorted_prices = sorted([p.SalePrice for p in available_products])
                    # Subtract 1 to convert count to 0-based index: for 10 items at 70%,
                    # int(10*0.7)=7 items means indices 0-6, so threshold is at index 6
                    threshold_idx = max(0, int(len(sorted_prices) * price_modifier) - 1)
                    price_threshold = sorted_prices[threshold_idx]
                    preferred_products = [
                        p for p in available_products if p.SalePrice <= price_threshold
                    ]
                elif price_modifier > 1.0:
                    # Quality/premium seekers: prefer higher-priced products
                    # Use explicit percentile mapping to match documented behavior
                    top_percentile_map = {
                        CustomerSegment.QUALITY_SEEKER: 0.3,   # Top 30% of prices
                        CustomerSegment.BRAND_LOYAL: 0.5,      # Top 50% of prices
                    }
                    top_percentile = top_percentile_map.get(segment, 0.3)
                    sorted_prices = sorted([p.SalePrice for p in available_products])
                    threshold_idx = min(
                        len(sorted_prices) - 1,
                        int(len(sorted_prices) * (1.0 - top_percentile))
                    )
                    price_threshold = sorted_prices[threshold_idx]
                    preferred_products = [
                        p for p in available_products if p.SalePrice >= price_threshold
                    ]
                else:
                    # Convenience-focused (modifier=1.0): no price filtering
                    preferred_products = available_products

                if not preferred_products:
                    # Fallback: if filtering left no products, use all available
                    preferred_products = available_products

                # Select products from category
                for _ in range(category_items):
                    if remaining_items <= 0:
                        break

                    product = self._rng.choice(preferred_products)

                    # Determine quantity (usually 1, sometimes more for consumables)
                    if category in ["food", "beverages", "household"]:
                        quantity = self._rng.choices(
                            [1, 2, 3], weights=[0.7, 0.2, 0.1]
                        )[0]
                    else:
                        quantity = 1

                    basket_items.append((product, quantity))
                    remaining_items -= 1

        # Fill remaining with random products if needed
        while remaining_items > 0:
            product = self._rng.choice(self.products)
            basket_items.append((product, 1))
            remaining_items -= 1

        return basket_items

    def get_preferred_stores_for_customer(
        self, customer_id: int, max_stores: int = 3
    ) -> list[Store]:
        """Get preferred stores for a customer based on geography."""
        customer = next((c for c in self.customers if c.ID == customer_id), None)
        if not customer:
            return self._rng.sample(self.stores, min(max_stores, len(self.stores)))

        # Find stores in same geography (simplified - in practice would use distance)
        same_geo_stores = [
            s for s in self.stores if s.GeographyID == customer.GeographyID
        ]

        if same_geo_stores:
            return self._rng.sample(
                same_geo_stores, min(max_stores, len(same_geo_stores))
            )
        else:
            # Fallback to random stores
            return self._rng.sample(self.stores, min(max_stores, len(self.stores)))

    def update_available_products(self, products: list[ProductMaster]) -> None:
        """Update the available products list and rebuild categories."""
        self.products = products
        self._product_categories = self._categorize_products()
        # Rebuild product category map
        self._product_category_map = {
            product.ID: self._get_product_category(product)
            for product in self.products
        }

    def apply_promotions_to_basket(
        self,
        basket: ShoppingBasket,
        customer_id: int,
        transaction_date: datetime,
    ) -> tuple[Decimal, list[dict]]:
        """
        Apply promotional discounts to a shopping basket.

        Args:
            basket: Shopping basket to apply promotions to
            customer_id: Customer ID for segment-based promotion selection
            transaction_date: Date of transaction for seasonal promotions

        Returns:
            Tuple of (total_discount_amount, basket_items_with_promotions)
            Each basket item is a dict with:
                - product: ProductMaster
                - qty: int
                - subtotal: Decimal
                - promo_code: str | None
                - discount: Decimal
        """
        # Get customer segment
        segment = self.get_customer_segment(customer_id)

        # Prepare basket items for promotion engine
        basket_items = []
        basket_categories = []
        basket_subtotal = Decimal("0.00")

        for product, qty in basket.items:
            item_subtotal = product.SalePrice * qty
            basket_subtotal += item_subtotal

            category = self._product_category_map.get(product.ID, "other")
            basket_categories.append(category)

            basket_items.append({
                "product": product,
                "qty": qty,
                "subtotal": item_subtotal,
            })

        # Check if promotion should be applied
        should_apply = self._promotion_engine.should_apply_promotion(
            customer_segment=segment.value,
            transaction_date=transaction_date,
            basket_categories=basket_categories,
        )

        if not should_apply:
            # No promotion, return items with no discount
            items_without_promo = [
                {**item, "promo_code": None, "discount": Decimal("0.00")}
                for item in basket_items
            ]
            return Decimal("0.00"), items_without_promo

        # Select appropriate promotion
        promo_config = self._promotion_engine.select_promotion(
            transaction_date=transaction_date,
            basket_subtotal=basket_subtotal,
            basket_categories=basket_categories,
        )

        if not promo_config:
            # No eligible promotion found
            items_without_promo = [
                {**item, "promo_code": None, "discount": Decimal("0.00")}
                for item in basket_items
            ]
            return Decimal("0.00"), items_without_promo

        # Apply promotion to basket
        total_discount, items_with_promos = self._promotion_engine.apply_promotion_to_basket(
            basket_items=basket_items,
            promo_config=promo_config,
            product_categories=self._product_category_map,
        )

        return total_discount, items_with_promos
