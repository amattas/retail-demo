"""
Retail behavior simulation patterns for realistic data generation.

This module provides simulators for various retail behaviors including
customer shopping patterns, inventory flows, marketing campaigns,
and cross-table business logic coordination.
"""

import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum

logger = logging.getLogger(__name__)

from retail_datagen.shared.models import (
    Customer,
    DeviceType,
    DisruptionSeverity,
    DisruptionType,
    DistributionCenter,
    InventoryReason,
    MarketingChannel,
    ProductMaster,
    Store,
    TruckStatus,
)

# Marketing campaign generation constants
CAMPAIGN_START_PROBABILITY = 0.90  # Probability of starting new campaigns when no campaign is active


class ShoppingBehaviorType(Enum):
    """Types of shopping behaviors."""

    QUICK_TRIP = "QUICK_TRIP"  # 1-3 items, focused shopping
    GROCERY_RUN = "GROCERY_RUN"  # 5-15 items, routine shopping
    FAMILY_SHOPPING = "FAMILY_SHOPPING"  # 10-30 items, planned shopping
    BULK_SHOPPING = "BULK_SHOPPING"  # 20-50 items, bulk purchases


class CustomerSegment(Enum):
    """Customer segments with different behaviors."""

    BUDGET_CONSCIOUS = "BUDGET_CONSCIOUS"
    CONVENIENCE_FOCUSED = "CONVENIENCE_FOCUSED"
    QUALITY_SEEKER = "QUALITY_SEEKER"
    BRAND_LOYAL = "BRAND_LOYAL"


# Marketing campaign constants
DEFAULT_MIN_DAILY_IMPRESSIONS = 100  # Minimum impressions per campaign per day


@dataclass
class ShoppingBasket:
    """Represents a shopping basket with products and quantities."""

    items: list[tuple[ProductMaster, int]]  # (product, quantity)
    behavior_type: ShoppingBehaviorType
    customer_segment: CustomerSegment
    total_items: int
    estimated_total: Decimal


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

        # Keywords for categorization
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

        for product in self.products:
            product_name_lower = product.ProductName.lower()
            categorized = False

            for category, keywords in category_keywords.items():
                if any(keyword in product_name_lower for keyword in keywords):
                    categories[category].append(product)
                    categorized = True
                    break

            if not categorized:
                categories["other"].append(product)

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
        self, customer_id: int, behavior_type: ShoppingBehaviorType | None = None
    ) -> ShoppingBasket:
        """
        Generate a realistic shopping basket for a customer.

        Args:
            customer_id: Customer ID
            behavior_type: Override shopping behavior (optional)

        Returns:
            ShoppingBasket with realistic product combinations
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
        target_items = self._rng.randint(min_items, max_items)

        # Select products based on behavior and segment
        basket_items = self._select_basket_products(
            segment, behavior_type, target_items
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

        # Segment price preferences
        price_modifiers = {
            CustomerSegment.BUDGET_CONSCIOUS: 0.7,  # Prefer lower prices
            CustomerSegment.CONVENIENCE_FOCUSED: 1.0,  # Price neutral
            CustomerSegment.QUALITY_SEEKER: 1.3,  # Prefer higher prices
            CustomerSegment.BRAND_LOYAL: 1.1,  # Slight price premium
        }

        price_modifiers[segment]
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

                # Filter products by price preference
                if segment == CustomerSegment.BUDGET_CONSCIOUS:
                    # Prefer products in bottom 60% of price range
                    price_threshold = sorted([p.SalePrice for p in available_products])[
                        int(len(available_products) * 0.6)
                    ]
                    preferred_products = [
                        p for p in available_products if p.SalePrice <= price_threshold
                    ]
                elif segment == CustomerSegment.QUALITY_SEEKER:
                    # Prefer products in top 40% of price range
                    price_threshold = sorted([p.SalePrice for p in available_products])[
                        int(len(available_products) * 0.6)
                    ]
                    preferred_products = [
                        p for p in available_products if p.SalePrice >= price_threshold
                    ]
                else:
                    preferred_products = available_products

                if not preferred_products:
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


class InventoryFlowSimulator:
    """
    Simulates realistic inventory flows through the supply chain.

    Models DC receiving, truck movements, store deliveries, and inventory
    adjustments with realistic timing and quantities.
    """

    def __init__(
        self,
        distribution_centers: list[DistributionCenter],
        stores: list[Store],
        products: list[ProductMaster],
        seed: int = 42,
    ):
        """
        Initialize inventory flow simulator.

        Args:
            distribution_centers: List of DC dimension records
            stores: List of store dimension records
            products: List of product master records
            seed: Random seed for reproducible simulations
        """
        self.dcs = distribution_centers
        self.stores = stores
        self.products = products
        self._rng = random.Random(seed)

        # Current inventory levels (simplified tracking)
        self._dc_inventory: dict[
            tuple[int, int], int
        ] = {}  # (dc_id, product_id) -> quantity
        self._store_inventory: dict[
            tuple[int, int], int
        ] = {}  # (store_id, product_id) -> quantity

        # Initialize with baseline inventory
        self._initialize_inventory()

        # Truck fleet simulation
        self._truck_capacity = 1000  # items per truck
        self._active_shipments: dict[str, dict] = {}  # shipment_id -> shipment_info

        # Reorder points and quantities
        self._reorder_points = self._calculate_reorder_points()

        # Supply chain disruptions tracking
        self._active_disruptions: dict[int, dict] = {}  # dc_id -> disruption_info
        self._disruption_counter = 1

    def _initialize_inventory(self):
        """Initialize baseline inventory levels."""
        for dc in self.dcs:
            for product in self.products:
                # DCs start with higher inventory levels
                initial_qty = self._rng.randint(100, 1000)
                self._dc_inventory[(dc.ID, product.ID)] = initial_qty

        for store in self.stores:
            for product in self.products:
                # Stores start with moderate inventory levels
                initial_qty = self._rng.randint(10, 100)
                self._store_inventory[(store.ID, product.ID)] = initial_qty

    def _calculate_reorder_points(self) -> dict[tuple[int, int], int]:
        """Calculate reorder points for store inventory."""
        reorder_points = {}

        for store in self.stores:
            for product in self.products:
                # Simple reorder point calculation (could be more sophisticated)
                base_reorder = self._rng.randint(5, 20)
                reorder_points[(store.ID, product.ID)] = base_reorder

        return reorder_points

    def simulate_dc_receiving(self, dc_id: int, date: datetime) -> list[dict]:
        """
        Simulate DC receiving shipments from suppliers.

        Args:
            dc_id: Distribution center ID
            date: Date of receiving

        Returns:
            List of inventory transaction records
        """
        transactions = []

        # Get capacity multiplier based on active disruptions
        capacity_multiplier = self.get_dc_capacity_multiplier(dc_id, date)

        # Simulate receiving 1-3 shipments per day (reduced by disruptions)
        base_shipments = self._rng.randint(1, 3)
        num_shipments = max(1, int(base_shipments * capacity_multiplier))

        for _ in range(num_shipments):
            # Select random products for shipment
            base_products = self._rng.randint(10, 50)
            num_products = max(5, int(base_products * capacity_multiplier))
            products_in_shipment = self._rng.sample(
                self.products, min(num_products, len(self.products))
            )

            for product in products_in_shipment:
                # Receiving quantities based on product type (reduced by disruptions)
                base_qty = self._rng.randint(50, 500)
                receive_qty = max(10, int(base_qty * capacity_multiplier))

                # Update inventory
                key = (dc_id, product.ID)
                self._dc_inventory[key] = self._dc_inventory.get(key, 0) + receive_qty

                transactions.append(
                    {
                        "DCID": dc_id,
                        "ProductID": product.ID,
                        "QtyDelta": receive_qty,
                        "Reason": InventoryReason.INBOUND_SHIPMENT,
                        "EventTS": date,
                    }
                )

        return transactions

    def simulate_store_demand(
        self, store_id: int, date: datetime, traffic_multiplier: float = 1.0
    ) -> list[dict]:
        """
        Simulate store demand and generate inventory deductions.

        Args:
            store_id: Store ID
            date: Date of demand
            traffic_multiplier: Multiplier for demand based on traffic patterns

        Returns:
            List of store inventory transactions
        """
        transactions = []

        # Base demand adjusted by traffic patterns
        base_products_sold = int(50 * traffic_multiplier)
        num_products = max(5, base_products_sold)

        # Select products that might be sold
        available_products = []
        for product in self.products:
            key = (store_id, product.ID)
            current_inventory = self._store_inventory.get(key, 0)
            if current_inventory > 0:
                available_products.append(product)

        if not available_products:
            return transactions

        products_to_sell = self._rng.sample(
            available_products, min(num_products, len(available_products))
        )

        for product in products_to_sell:
            key = (store_id, product.ID)
            current_inventory = self._store_inventory.get(key, 0)

            # Sales quantity (usually 1-5 units)
            max_sale_qty = min(current_inventory, self._rng.randint(1, 5))
            if max_sale_qty > 0:
                sale_qty = self._rng.randint(1, max_sale_qty)

                # Update inventory
                self._store_inventory[key] = current_inventory - sale_qty

                transactions.append(
                    {
                        "StoreID": store_id,
                        "ProductID": product.ID,
                        "QtyDelta": -sale_qty,
                        "Reason": InventoryReason.SALE,
                        "Source": "CUSTOMER_PURCHASE",
                        "EventTS": date,
                    }
                )

        return transactions

    def check_reorder_needs(self, store_id: int) -> list[tuple[int, int]]:
        """
        Check which products need reordering for a store.

        Args:
            store_id: Store ID to check

        Returns:
            List of (product_id, reorder_quantity) tuples
        """
        reorders = []

        for product in self.products:
            key = (store_id, product.ID)
            current_inventory = self._store_inventory.get(key, 0)
            reorder_point = self._reorder_points.get(key, 10)

            if current_inventory <= reorder_point:
                # Calculate reorder quantity
                reorder_qty = self._rng.randint(50, 200)
                reorders.append((product.ID, reorder_qty))

        return reorders

    def generate_truck_shipment(
        self,
        dc_id: int,
        store_id: int,
        reorder_list: list[tuple[int, int]],
        departure_time: datetime,
    ) -> dict:
        """
        Generate truck shipment from DC to store.

        Args:
            dc_id: Source distribution center ID
            store_id: Destination store ID
            reorder_list: List of (product_id, quantity) to ship
            departure_time: When truck departs

        Returns:
            Shipment information dictionary
        """
        # Generate unique shipment ID
        shipment_id = f"SHIP{departure_time.strftime('%Y%m%d')}{dc_id:02d}{store_id:03d}{self._rng.randint(100, 999)}"
        truck_id = f"TRK{self._rng.randint(1000, 9999)}"

        # Check for active disruptions at DC
        capacity_multiplier = self.get_dc_capacity_multiplier(dc_id, departure_time)

        # Add delays for disruptions (inverse of capacity - lower capacity = more delays)
        base_travel_hours = self._rng.randint(2, 12)  # 2-12 hours base travel time
        delay_multiplier = 2.0 - capacity_multiplier  # 1.0 to 2.0 range
        travel_hours = int(base_travel_hours * delay_multiplier)

        eta = departure_time + timedelta(hours=travel_hours)
        etd = eta + timedelta(hours=1)  # 1 hour unloading time

        shipment_info = {
            "shipment_id": shipment_id,
            "truck_id": truck_id,
            "dc_id": dc_id,
            "store_id": store_id,
            "departure_time": departure_time,
            "eta": eta,
            "etd": etd,
            "status": TruckStatus.SCHEDULED,
            "products": reorder_list,
            "total_items": sum(qty for _, qty in reorder_list),
        }

        # Track active shipment
        self._active_shipments[shipment_id] = shipment_info

        return shipment_info

    def update_shipment_status(
        self, shipment_id: str, current_time: datetime
    ) -> dict | None:
        """
        Update shipment status based on current time.

        Args:
            shipment_id: Shipment to update
            current_time: Current simulation time

        Returns:
            Updated shipment info or None if not found
        """
        if shipment_id not in self._active_shipments:
            return None

        shipment = self._active_shipments[shipment_id]

        # Update status based on time progression
        if current_time >= shipment["etd"]:
            shipment["status"] = TruckStatus.COMPLETED
            # Remove from active tracking
            del self._active_shipments[shipment_id]
        elif current_time >= shipment["eta"]:
            shipment["status"] = TruckStatus.UNLOADING
        elif current_time >= shipment["departure_time"] + timedelta(hours=1):
            shipment["status"] = TruckStatus.IN_TRANSIT
        elif current_time >= shipment["departure_time"]:
            shipment["status"] = TruckStatus.LOADING

        return shipment

    def complete_delivery(self, shipment_id: str) -> list[dict]:
        """
        Complete delivery and update store inventory.

        Args:
            shipment_id: Shipment to complete

        Returns:
            List of store inventory transactions
        """
        if shipment_id not in self._active_shipments:
            return []

        shipment = self._active_shipments[shipment_id]
        transactions = []

        store_id = shipment["store_id"]
        dc_id = shipment["dc_id"]

        # Process each product in shipment
        for product_id, quantity in shipment["products"]:
            # Update store inventory
            store_key = (store_id, product_id)
            self._store_inventory[store_key] = (
                self._store_inventory.get(store_key, 0) + quantity
            )

            # Update DC inventory (outbound)
            dc_key = (dc_id, product_id)
            self._dc_inventory[dc_key] = max(
                0, self._dc_inventory.get(dc_key, 0) - quantity
            )

            # Create transactions
            transactions.append(
                {
                    "StoreID": store_id,
                    "ProductID": product_id,
                    "QtyDelta": quantity,
                    "Reason": InventoryReason.INBOUND_SHIPMENT,
                    "Source": shipment["truck_id"],
                    "EventTS": shipment["etd"],
                }
            )

        return transactions

    def generate_truck_loading_events(
        self, shipment_info: dict, load_time: datetime
    ) -> list[dict]:
        """
        Generate truck loading events for inventory tracking.

        Args:
            shipment_info: Shipment information from generate_truck_shipment
            load_time: When loading occurs

        Returns:
            List of truck inventory loading records
        """
        truck_inventory_events = []

        for product_id, quantity in shipment_info["products"]:
            truck_inventory_events.append(
                {
                    "TruckId": shipment_info["truck_id"],
                    "ShipmentId": shipment_info["shipment_id"],
                    "ProductID": product_id,
                    "Quantity": quantity,
                    "Action": "LOAD",
                    "LocationID": shipment_info["dc_id"],
                    "LocationType": "DC",
                    "EventTS": load_time,
                }
            )

        return truck_inventory_events

    def generate_truck_unloading_events(
        self, shipment_info: dict, unload_time: datetime
    ) -> list[dict]:
        """
        Generate truck unloading events for inventory tracking.

        Args:
            shipment_info: Shipment information from generate_truck_shipment
            unload_time: When unloading occurs

        Returns:
            List of truck inventory unloading records
        """
        truck_inventory_events = []

        for product_id, quantity in shipment_info["products"]:
            truck_inventory_events.append(
                {
                    "TruckId": shipment_info["truck_id"],
                    "ShipmentId": shipment_info["shipment_id"],
                    "ProductID": product_id,
                    "Quantity": quantity,
                    "Action": "UNLOAD",
                    "LocationID": shipment_info["store_id"],
                    "LocationType": "STORE",
                    "EventTS": unload_time,
                }
            )

        return truck_inventory_events

    def track_truck_inventory_status(self, date: datetime) -> list[dict]:
        """
        Generate truck inventory tracking events for all active shipments.

        Args:
            date: Current simulation date

        Returns:
            List of truck inventory tracking records
        """
        all_truck_inventory_events = []

        for shipment_id, shipment_info in self._active_shipments.items():
            current_status = shipment_info.get("status", TruckStatus.SCHEDULED)

            # Generate loading events when truck status changes to LOADING
            if current_status == TruckStatus.LOADING:
                load_events = self.generate_truck_loading_events(shipment_info, date)
                all_truck_inventory_events.extend(load_events)

            # Generate unloading events when truck status changes to UNLOADING
            elif current_status == TruckStatus.UNLOADING:
                unload_events = self.generate_truck_unloading_events(
                    shipment_info, shipment_info["etd"]
                )
                all_truck_inventory_events.extend(unload_events)

        return all_truck_inventory_events

    def simulate_supply_chain_disruptions(self, date: datetime) -> list[dict]:
        """
        Generate and manage supply chain disruption events.

        Args:
            date: Current simulation date

        Returns:
            List of supply chain disruption records
        """

        disruption_events = []

        # Check for new disruption (2% chance per day per DC)
        for dc in self.dcs:
            if dc.ID not in self._active_disruptions and self._rng.random() < 0.02:
                disruption = self._create_disruption(dc.ID, date)
                self._active_disruptions[dc.ID] = disruption
                disruption_events.append(disruption)

        # Check for resolving existing disruptions
        resolved_dcs = []
        for dc_id, disruption in self._active_disruptions.items():
            # Check if disruption should end (based on duration)
            duration_hours = (date - disruption["start_time"]).total_seconds() / 3600
            expected_duration = disruption["expected_duration_hours"]

            # 70% chance to resolve after expected duration, increases over time
            resolve_probability = max(
                0.7, (duration_hours - expected_duration) / expected_duration * 0.5
            )

            if (
                duration_hours >= expected_duration
                and self._rng.random() < resolve_probability
            ):
                # Create resolution event
                resolution_event = {
                    "DCID": dc_id,
                    "DisruptionType": disruption["type"],
                    "Severity": disruption["severity"],
                    "Description": f"Resolved: {disruption['description']}",
                    "StartTime": disruption["start_time"],
                    "EndTime": date,
                    "ImpactPercentage": disruption["impact_percentage"],
                    "AffectedProducts": disruption["affected_products"],
                    "EventTS": date,
                }
                disruption_events.append(resolution_event)
                resolved_dcs.append(dc_id)

        # Remove resolved disruptions
        for dc_id in resolved_dcs:
            del self._active_disruptions[dc_id]

        return disruption_events

    def _create_disruption(self, dc_id: int, date: datetime) -> dict:
        """Create a new supply chain disruption event."""
        import json

        # Select disruption type based on weights
        disruption_types = [
            (DisruptionType.CAPACITY_CONSTRAINT, 0.3),
            (DisruptionType.EQUIPMENT_FAILURE, 0.25),
            (DisruptionType.WEATHER_DELAY, 0.2),
            (DisruptionType.LABOR_SHORTAGE, 0.15),
            (DisruptionType.SYSTEM_OUTAGE, 0.1),
        ]

        disruption_type = self._rng.choices(
            [dt[0] for dt in disruption_types],
            weights=[dt[1] for dt in disruption_types],
        )[0]

        # Select severity (minor more common)
        severity_weights = {
            DisruptionSeverity.MINOR: 0.6,
            DisruptionSeverity.MODERATE: 0.3,
            DisruptionSeverity.SEVERE: 0.1,
        }
        severity = self._rng.choices(
            list(severity_weights.keys()), weights=list(severity_weights.values())
        )[0]

        # Calculate impact percentage based on severity
        impact_ranges = {
            DisruptionSeverity.MINOR: (10, 30),
            DisruptionSeverity.MODERATE: (30, 60),
            DisruptionSeverity.SEVERE: (60, 90),
        }
        impact_percentage = self._rng.randint(*impact_ranges[severity])

        # Generate duration (hours)
        duration_ranges = {
            DisruptionSeverity.MINOR: (2, 12),
            DisruptionSeverity.MODERATE: (8, 48),
            DisruptionSeverity.SEVERE: (24, 168),  # Up to 1 week
        }
        expected_duration = self._rng.randint(*duration_ranges[severity])

        # Select affected products (random subset)
        num_affected = max(1, int(len(self.products) * (impact_percentage / 100) * 0.3))
        affected_product_ids = [
            p.ID for p in self._rng.sample(self.products, num_affected)
        ]

        # Generate description
        descriptions = {
            DisruptionType.CAPACITY_CONSTRAINT: "Reduced capacity due to high demand surge",
            DisruptionType.EQUIPMENT_FAILURE: "Equipment failure in sorting/loading systems",
            DisruptionType.WEATHER_DELAY: "Weather-related delays affecting inbound shipments",
            DisruptionType.LABOR_SHORTAGE: "Staff shortage impacting operations",
            DisruptionType.SYSTEM_OUTAGE: "IT system outage affecting inventory management",
        }

        return {
            "DCID": dc_id,
            "DisruptionType": disruption_type,
            "Severity": severity,
            "Description": descriptions[disruption_type],
            "StartTime": date,
            "EndTime": None,
            "ImpactPercentage": impact_percentage,
            "AffectedProducts": json.dumps(affected_product_ids),
            "EventTS": date,
            "type": disruption_type,
            "severity": severity,
            "description": descriptions[disruption_type],
            "start_time": date,
            "impact_percentage": impact_percentage,
            "affected_products": json.dumps(affected_product_ids),
            "expected_duration_hours": expected_duration,
        }

    def get_dc_capacity_multiplier(self, dc_id: int, date: datetime) -> float:
        """
        Get capacity multiplier for a DC considering active disruptions.

        Args:
            dc_id: Distribution center ID
            date: Current simulation date

        Returns:
            Capacity multiplier (1.0 = normal, 0.5 = 50% capacity, etc.)
        """
        if dc_id in self._active_disruptions:
            disruption = self._active_disruptions[dc_id]
            impact_percentage = disruption["impact_percentage"]
            return 1.0 - (impact_percentage / 100)
        return 1.0


class MarketingCampaignSimulator:
    """
    Simulates realistic marketing campaigns and customer interactions.

    Models different marketing channels, campaign targeting, and
    impression generation with realistic costs and conversion patterns.
    """

    def __init__(self, customers: list[Customer], seed: int = 42):
        """
        Initialize marketing campaign simulator.

        Args:
            customers: List of customer dimension records
            seed: Random seed for reproducible campaigns
        """
        self.customers = customers
        self._rng = random.Random(seed)
        self._impression_counter = 0  # Counter for unique impression IDs

        # Campaign types and their characteristics
        self._campaign_types = {
            "seasonal_sale": {
                "channels": [
                    MarketingChannel.FACEBOOK,
                    MarketingChannel.GOOGLE,
                    MarketingChannel.EMAIL,
                ],
                "duration_days": 7,
                "daily_impressions": 1000,
                "cost_per_impression": Decimal("0.15"),
                "target_segments": ["budget_conscious", "convenience_focused"],
            },
            "product_launch": {
                "channels": [
                    MarketingChannel.INSTAGRAM,
                    MarketingChannel.YOUTUBE,
                    MarketingChannel.DISPLAY,
                ],
                "duration_days": 14,
                "daily_impressions": 2000,
                "cost_per_impression": Decimal("0.25"),
                "target_segments": ["quality_seeker", "brand_loyal"],
            },
            "loyalty_program": {
                "channels": [MarketingChannel.EMAIL, MarketingChannel.SOCIAL],
                "duration_days": 30,
                "daily_impressions": 500,
                "cost_per_impression": Decimal("0.05"),
                "target_segments": ["brand_loyal", "quality_seeker"],
            },
            "flash_sale": {
                "channels": [MarketingChannel.SOCIAL, MarketingChannel.SEARCH],
                "duration_days": 1,
                "daily_impressions": 5000,
                "cost_per_impression": Decimal("0.30"),
                "target_segments": ["budget_conscious", "convenience_focused"],
            },
        }

        # Device distribution for impressions
        self._device_distribution = {
            DeviceType.MOBILE: 0.6,
            DeviceType.DESKTOP: 0.3,
            DeviceType.TABLET: 0.1,
        }

        # Active campaigns tracking
        self._active_campaigns: dict[str, dict] = {}
        self._campaign_counter = 1

    def start_campaign(self, campaign_type: str, start_date: datetime) -> str:
        """
        Start a new marketing campaign.

        Args:
            campaign_type: Type of campaign to start
            start_date: Campaign start date

        Returns:
            Campaign ID
        """
        if campaign_type not in self._campaign_types:
            raise ValueError(f"Unknown campaign type: {campaign_type}")

        campaign_id = f"CAMP{start_date.strftime('%Y%m%d')}{self._campaign_counter:04d}"
        self._campaign_counter += 1

        campaign_config = self._campaign_types[campaign_type].copy()
        campaign_info = {
            "campaign_id": campaign_id,
            "type": campaign_type,
            "start_date": start_date,
            "end_date": start_date
            + timedelta(days=campaign_config["duration_days"] - 1),
            "config": campaign_config,
            "total_impressions": 0,
            "total_cost": Decimal("0"),
        }

        self._active_campaigns[campaign_id] = campaign_info
        return campaign_id

    def generate_campaign_impressions(
        self, campaign_id: str, date: datetime, traffic_multiplier: float = 1.0
    ) -> list[dict]:
        """
        Generate marketing impressions for a campaign on a given date.

        Args:
            campaign_id: Campaign to generate impressions for
            date: Date to generate impressions
            traffic_multiplier: Multiplier for impression volume

        Returns:
            List of marketing impression records
        """
        # Use .get() to safely handle race conditions in parallel processing
        # Campaign may be removed by another thread between check and access
        campaign = self._active_campaigns.get(campaign_id)
        if campaign is None:
            return []

        # Check if campaign is active on this date
        # Convert datetime to date for comparison if needed
        date_to_check = date.date() if isinstance(date, datetime) else date
        campaign_start = (
            campaign["start_date"].date()
            if isinstance(campaign["start_date"], datetime)
            else campaign["start_date"]
        )
        campaign_end = (
            campaign["end_date"].date()
            if isinstance(campaign["end_date"], datetime)
            else campaign["end_date"]
        )

        if not (campaign_start <= date_to_check <= campaign_end):
            return []

        config = campaign["config"]
        impressions = []

        logger.info(f"      Generating impressions for campaign {campaign_id}")
        logger.info(
            f"        Config: daily_impressions={config.get('daily_impressions', 'MISSING')}, channels={len(config.get('channels', []))}"
        )
        logger.info(f"        Traffic multiplier: {traffic_multiplier}")

        # Calculate base impressions from traffic
        base_impressions_from_traffic = config["daily_impressions"] * traffic_multiplier
        min_daily_impressions = config.get(
            "min_daily_impressions", DEFAULT_MIN_DAILY_IMPRESSIONS
        )
        num_channels = len(config["channels"])

        # Store closure (traffic_multiplier=0) results in zero impressions
        # Otherwise enforce minimum threshold to prevent rounding to zero
        if traffic_multiplier == 0:
            base_impressions = 0
        else:
            # Ensure minimum TOTAL impressions across all channels (account for integer division)
            # Multiply minimum by channels to ensure we meet threshold after division
            min_impressions_total = min_daily_impressions * num_channels
            base_impressions = max(
                min_impressions_total, int(base_impressions_from_traffic)
            )

        logger.info(f"        Calculated base_impressions: {base_impressions}")
        logger.info(f"        Number of channels: {len(config['channels'])}")

        # Generate impressions across different channels
        logger.info(
            f"        Starting channel loop with {len(config['channels'])} channels"
        )
        for channel in config["channels"]:
            channel_impressions = base_impressions // len(config["channels"])
            logger.info(
                f"          Channel {channel}: {channel_impressions} impressions to generate"
            )

            for _ in range(channel_impressions):
                # Select target customer
                if not self.customers:
                    logger.error(
                        "          ERROR: No customers available for targeting!"
                    )
                    break

                target_customer = self._rng.choice(self.customers)

                # Select device type
                device_options = list(self._device_distribution.keys())
                device_weights = list(self._device_distribution.values())
                device = self._rng.choices(device_options, weights=device_weights)[0]

                # Generate creative ID
                creative_id = f"CREAT{campaign_id[-4:]}{channel.value[:3]}{self._rng.randint(1, 99):02d}"

                # Generate unique impression ID using counter
                self._impression_counter += 1
                impression_id = f"IMP{self._impression_counter:010d}"

                impression = {
                    "Channel": channel,
                    "CampaignId": campaign_id,
                    "CreativeId": creative_id,
                    "CustomerAdId": target_customer.AdId,
                    "ImpressionId": impression_id,
                    "Cost": config["cost_per_impression"],
                    "Device": device,
                    "EventTS": date,
                }

                impressions.append(impression)

                if len(impressions) % 100 == 0:
                    logger.debug(
                        f"          Generated {len(impressions)} impressions so far"
                    )

        # Update campaign totals
        campaign["total_impressions"] += len(impressions)
        campaign["total_cost"] += sum(imp["Cost"] for imp in impressions)

        logger.info(f"        Total impressions generated: {len(impressions)}")
        return impressions

    def should_start_campaign(
        self, date: datetime, traffic_multiplier: float = 1.0
    ) -> str | None:
        """
        Determine if a new campaign should start on a given date.

        Args:
            date: Date to check for new campaigns
            traffic_multiplier: Current traffic patterns

        Returns:
            Campaign type to start or None
        """
        # 90% of days should have active campaigns
        campaign_probability = CAMPAIGN_START_PROBABILITY

        if self._rng.random() < campaign_probability:
            # Select campaign type based on seasonality and current context
            available_types = list(self._campaign_types.keys())

            # Seasonal preferences
            if date.month in [11, 12]:  # Holiday season
                weights = [3, 1, 2, 4]  # Prefer seasonal sales and flash sales
            elif date.month in [8, 9]:  # Back to school
                weights = [2, 3, 1, 2]  # Prefer product launches
            else:
                weights = [1, 1, 1, 1]  # Equal weights

            return self._rng.choices(available_types, weights=weights)[0]

        return None


class BusinessRulesEngine:
    """
    Enforces business rules and constraints across fact table generation.

    Ensures data consistency, validates business logic, and maintains
    referential integrity between different fact tables.
    """

    def __init__(self):
        """Initialize business rules engine."""
        self._validation_errors: list[str] = []
        self._warnings: list[str] = []

    def validate_receipt_totals(
        self, receipt_lines: list[dict], receipt_total: Decimal
    ) -> bool:
        """
        Validate that receipt line totals match receipt header total.

        Args:
            receipt_lines: List of receipt line records
            receipt_total: Expected receipt total

        Returns:
            True if totals match within tolerance
        """
        calculated_total = sum(
            Decimal(str(line["UnitPrice"])) * line["Qty"] for line in receipt_lines
        )

        tolerance = Decimal("0.01")
        if abs(calculated_total - receipt_total) > tolerance:
            self._validation_errors.append(
                f"Receipt total mismatch: calculated={calculated_total}, expected={receipt_total}"
            )
            return False

        return True

    def validate_inventory_consistency(
        self,
        inventory_transactions: list[dict],
        starting_inventory: dict[tuple[int, int], int],
    ) -> bool:
        """
        Validate that inventory transactions don't result in negative inventory.

        Args:
            inventory_transactions: List of inventory transaction records
            starting_inventory: Starting inventory levels

        Returns:
            True if inventory remains non-negative
        """
        current_inventory = starting_inventory.copy()

        for transaction in inventory_transactions:
            if transaction.get("StoreID"):
                key = (transaction["StoreID"], transaction["ProductID"])
            else:
                key = (transaction["DCID"], transaction["ProductID"])

            current_level = current_inventory.get(key, 0)
            new_level = current_level + transaction["QtyDelta"]

            if new_level < 0 and transaction["Reason"] not in ["ADJUSTMENT", "LOST"]:
                self._validation_errors.append(
                    f"Negative inventory: {key} would have {new_level} units"
                )
                return False

            current_inventory[key] = max(0, new_level)

        return True

    def validate_truck_timing(self, truck_moves: list[dict]) -> bool:
        """
        Validate truck movement timing logic.

        Args:
            truck_moves: List of truck movement records

        Returns:
            True if timing is logical
        """
        for move in truck_moves:
            eta = move["ETA"]
            etd = move["ETD"]

            if etd < eta:
                self._validation_errors.append(
                    f"Truck {move['TruckId']}: ETD ({etd}) before ETA ({eta})"
                )
                return False

        return True

    def validate_geographic_consistency(
        self, records: list[dict], geography_mapping: dict[int, int]
    ) -> bool:
        """
        Validate geographic consistency in transactions.

        Args:
            records: Records to validate
            geography_mapping: Mapping of entity IDs to geography IDs

        Returns:
            True if geographically consistent
        """
        for record in records:
            if "StoreID" in record and "CustomerID" in record:
                store_geo = geography_mapping.get(record["StoreID"])
                customer_geo = geography_mapping.get(record["CustomerID"])

                if store_geo and customer_geo and store_geo != customer_geo:
                    # This is a warning, not an error (customers can travel)
                    self._warnings.append(
                        f"Customer {record['CustomerID']} shopping outside home geography"
                    )

        return True

    def get_validation_summary(self) -> dict[str, any]:
        """
        Get summary of validation results.

        Returns:
            Dictionary with validation results
        """
        return {
            "errors": self._validation_errors.copy(),
            "warnings": self._warnings.copy(),
            "error_count": len(self._validation_errors),
            "warning_count": len(self._warnings),
            "passed": len(self._validation_errors) == 0,
        }

    def clear_validation_results(self):
        """Clear accumulated validation results."""
        self._validation_errors.clear()
        self._warnings.clear()
