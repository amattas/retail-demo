"""
Customer geography and store affinity patterns for realistic shopping behavior.

This module implements customer home geographies and store selection patterns
that create realistic shopping behaviors where customers primarily shop at
1-2 nearby stores with occasional visits to distant locations.
"""

import logging
import math
import random
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np

from retail_datagen.shared.models import Customer, GeographyMaster, Store

logger = logging.getLogger(__name__)


@dataclass
class CustomerGeography:
    """
    Customer's home geography and shopping patterns.

    Attributes:
        customer_id: Customer ID
        home_zip: Customer's home ZIP code
        home_state: Customer's home state
        home_city: Customer's home city
        home_latitude: Approximate home latitude (for distance calculation)
        home_longitude: Approximate home longitude (for distance calculation)
        nearest_stores: List of (store_id, distance_miles) tuples, sorted by distance
        primary_store_id: Most frequent store (closest)
        secondary_store_id: Second most frequent store
        travel_propensity: Likelihood to shop far from home (0.0-1.0)
        customer_segment: Shopping behavior segment (tourist, commuter, etc.)
    """

    customer_id: int
    home_zip: str
    home_state: str
    home_city: str
    home_latitude: float
    home_longitude: float

    nearest_stores: List[Tuple[int, float]]  # [(store_id, distance_miles), ...]

    primary_store_id: int
    secondary_store_id: int
    travel_propensity: float  # 0.0 to 1.0
    customer_segment: str  # regular, tourist, commuter, snowbird, business_traveler

    def get_store_selection_weights(self, all_store_ids: List[int]) -> Dict[int, float]:
        """
        Calculate probability of shopping at each store based on distance and patterns.

        This implements a distance decay function where closer stores have higher
        probabilities, with special handling for primary/secondary stores.

        Args:
            all_store_ids: List of all available store IDs

        Returns:
            Dictionary mapping store_id to selection weight (unnormalized probabilities)
        """
        weights = {}

        # Create distance lookup from nearest_stores list
        distance_map = {store_id: dist for store_id, dist in self.nearest_stores}

        for store_id in all_store_ids:
            distance = distance_map.get(store_id, 999.0)  # Default far distance

            # Primary store gets 50% of trips
            if store_id == self.primary_store_id:
                weights[store_id] = 0.50
            # Secondary store gets 20% of trips
            elif store_id == self.secondary_store_id:
                weights[store_id] = 0.20
            # Nearby stores (< 5 miles) get higher weight
            elif distance < 5:
                weights[store_id] = 0.15 * self.travel_propensity
            # Regional stores (5-15 miles) get moderate weight
            elif distance < 15:
                weights[store_id] = 0.10 * self.travel_propensity
            # Distant stores get low weight (tourism, travel)
            else:
                weights[store_id] = 0.05 * self.travel_propensity

        # Normalize weights to sum to 1.0
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}

        return weights


class GeographyAssigner:
    """
    Assigns home geographies to customers and calculates store affinities.

    This class implements realistic customer geographic distributions where:
    - Customers are assigned home locations near stores
    - Store affinity is calculated based on distance
    - Special customer segments are identified (tourists, commuters, etc.)
    """

    def __init__(
        self,
        customers: List[Customer],
        stores: List[Store],
        geographies: List[GeographyMaster],
        seed: int = 42,
    ):
        """
        Initialize geography assigner.

        Args:
            customers: List of customer records (with existing GeographyID)
            stores: List of store records
            geographies: List of geography master records
            seed: Random seed for reproducibility
        """
        self.customers = customers
        self.stores = stores
        self.geographies = geographies
        self._rng = random.Random(seed)

        # Build geography lookup
        self._geo_lookup = {geo.ID: geo for geo in geographies}

        # Build store geography lookup
        self._store_geos = {}
        for store in stores:
            geo = self._geo_lookup.get(store.GeographyID)
            if geo:
                self._store_geos[store.ID] = geo

        # Generate synthetic coordinates for geographies (for distance calculation)
        self._generate_synthetic_coordinates()

        # Precompute store coordinate arrays for vectorized distance calculation
        try:
            self._store_ids_arr = np.array([s.ID for s in stores], dtype=np.int32)
            self._store_coords = np.array(
                [self._geo_coordinates.get(s.GeographyID, (np.nan, np.nan)) for s in stores],
                dtype=np.float64,
            )
        except Exception:
            # Fallbacks if numpy conversion fails; methods will use per-store path
            self._store_ids_arr = None
            self._store_coords = None

        logger.info(
            f"GeographyAssigner initialized: {len(customers)} customers, "
            f"{len(stores)} stores, {len(geographies)} geographies"
        )

    def _generate_synthetic_coordinates(self) -> None:
        """
        Generate synthetic latitude/longitude coordinates for each geography.

        Uses a deterministic hash-based approach to assign consistent coordinates
        to each geography based on its attributes. This ensures the same geography
        always gets the same coordinates across runs.
        """
        self._geo_coordinates = {}

        for geo in self.geographies:
            # Use geography attributes to generate deterministic coordinates
            # Hash based on state + city + zip for consistency
            hash_input = f"{geo.State}{geo.City}{geo.ZipCode}"
            hash_value = hash(hash_input)

            # Convert hash to pseudo-random but deterministic lat/lon
            # US latitude range: approximately 25°N to 49°N
            # US longitude range: approximately -125°W to -65°W
            lat = 25.0 + (abs(hash_value % 10000) / 10000.0) * 24.0
            lon = -125.0 + (abs(hash_value % 20000) / 20000.0) * 60.0

            self._geo_coordinates[geo.ID] = (lat, lon)

    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate distance between two lat/lon points using Haversine formula.

        Args:
            lat1, lon1: First point (degrees)
            lat2, lon2: Second point (degrees)

        Returns:
            Distance in miles
        """
        # Radius of Earth in miles
        R = 3959.0

        # Convert degrees to radians
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)

        # Haversine formula
        a = (
            math.sin(delta_lat / 2) ** 2
            + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = R * c

        return distance

    def _calculate_store_distances(
        self, customer_geo_id: int
    ) -> Dict[int, float]:
        """
        Calculate distances from customer's home geography to all stores.

        Args:
            customer_geo_id: Customer's home geography ID

        Returns:
            Dictionary mapping store_id to distance in miles
        """
        customer_coords = self._geo_coordinates.get(customer_geo_id)
        if not customer_coords:
            return {}

        # Vectorized path if precomputed arrays exist
        if getattr(self, "_store_coords", None) is not None and isinstance(
            self._store_coords, np.ndarray
        ):
            lat1, lon1 = float(customer_coords[0]), float(customer_coords[1])

            coords = self._store_coords  # shape (N, 2)
            lats = coords[:, 0]
            lons = coords[:, 1]

            valid = ~np.isnan(lats) & ~np.isnan(lons)
            if not valid.any():
                return {}

            lat1_rad = np.deg2rad(lat1)
            lat2_rad = np.deg2rad(lats[valid])
            dlat = np.deg2rad(lats[valid] - lat1)
            dlon = np.deg2rad(lons[valid] - lon1)

            a = (
                np.sin(dlat / 2.0) ** 2
                + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2.0) ** 2
            )
            c = 2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))
            dist = 3959.0 * c  # Earth radius in miles

            ids = self._store_ids_arr[valid].astype(int)
            return {int(ids[i]): float(dist[i]) for i in range(dist.shape[0])}

        # Fallback per-store calculation
        customer_lat, customer_lon = customer_coords
        distances: Dict[int, float] = {}
        for store in self.stores:
            store_geo_id = store.GeographyID
            store_coords = self._geo_coordinates.get(store_geo_id)
            if not store_coords:
                continue
            store_lat, store_lon = store_coords
            distance = self._calculate_distance(
                customer_lat, customer_lon, store_lat, store_lon
            )
            distances[store.ID] = distance
        return distances

    def _determine_customer_segment(self, customer: Customer) -> str:
        """
        Determine customer segment based on shopping patterns.

        Segments:
        - regular: 80% - Normal shoppers who primarily visit nearby stores
        - commuter: 10% - Have two primary stores (home + work)
        - business_traveler: 5% - Frequent distant store visits
        - tourist: 3% - Occasional visitors from outside the area
        - snowbird: 2% - Seasonal shoppers (two home locations)

        Args:
            customer: Customer record

        Returns:
            Segment name
        """
        # Use customer ID as seed for consistency
        customer_rng = random.Random(customer.ID)

        segment_weights = [
            ("regular", 0.80),
            ("commuter", 0.10),
            ("business_traveler", 0.05),
            ("tourist", 0.03),
            ("snowbird", 0.02),
        ]

        segments = [s[0] for s in segment_weights]
        weights = [s[1] for s in segment_weights]

        return customer_rng.choices(segments, weights=weights)[0]

    def _get_travel_propensity(self, segment: str) -> float:
        """
        Get travel propensity (likelihood to shop far from home) by segment.

        Args:
            segment: Customer segment

        Returns:
            Travel propensity (0.0 to 1.0)
        """
        propensities = {
            "regular": 0.10,  # Rarely travel far
            "commuter": 0.25,  # Moderate travel (work commute)
            "business_traveler": 0.50,  # High travel
            "tourist": 0.80,  # Very high travel
            "snowbird": 0.60,  # High travel (seasonal migration)
        }

        return propensities.get(segment, 0.10)

    def assign_geographies(self) -> Dict[int, CustomerGeography]:
        """
        Assign home geography and store affinity to each customer.

        Returns:
            Dictionary mapping customer_id to CustomerGeography
        """
        customer_geographies = {}

        logger.info("Assigning home geographies and store affinities to customers...")

        for i, customer in enumerate(self.customers):
            # Customer already has a GeographyID from master generation
            # Use it as their home geography
            home_geo = self._geo_lookup.get(customer.GeographyID)
            if not home_geo:
                logger.warning(
                    f"Customer {customer.ID} has invalid GeographyID {customer.GeographyID}, skipping"
                )
                continue

            # Get coordinates for this customer's home
            home_coords = self._geo_coordinates.get(home_geo.ID)
            if not home_coords:
                logger.warning(
                    f"No coordinates for geography {home_geo.ID}, skipping customer {customer.ID}"
                )
                continue

            home_lat, home_lon = home_coords

            # Calculate distances to all stores
            store_distances = self._calculate_store_distances(customer.GeographyID)
            if not store_distances:
                logger.warning(
                    f"Could not calculate store distances for customer {customer.ID}, skipping"
                )
                continue

            # Sort stores by distance
            sorted_stores = sorted(store_distances.items(), key=lambda x: x[1])
            nearest_stores = sorted_stores[:10]  # Keep top 10 nearest

            # Primary and secondary stores are the two closest
            primary_store_id = nearest_stores[0][0] if len(nearest_stores) > 0 else self.stores[0].ID
            secondary_store_id = nearest_stores[1][0] if len(nearest_stores) > 1 else primary_store_id

            # Determine customer segment
            segment = self._determine_customer_segment(customer)
            travel_propensity = self._get_travel_propensity(segment)

            # Create CustomerGeography record
            customer_geo = CustomerGeography(
                customer_id=customer.ID,
                home_zip=home_geo.ZipCode,
                home_state=home_geo.State,
                home_city=home_geo.City,
                home_latitude=home_lat,
                home_longitude=home_lon,
                nearest_stores=nearest_stores,
                primary_store_id=primary_store_id,
                secondary_store_id=secondary_store_id,
                travel_propensity=travel_propensity,
                customer_segment=segment,
            )

            customer_geographies[customer.ID] = customer_geo

            # Log progress every 10,000 customers
            if (i + 1) % 10000 == 0:
                logger.info(f"Assigned geographies to {i + 1:,} customers...")

        logger.info(f"Assigned geographies to {len(customer_geographies):,} customers")

        # Log summary statistics
        self._log_geography_summary(customer_geographies)

        return customer_geographies

    def _log_geography_summary(self, customer_geographies: Dict[int, CustomerGeography]) -> None:
        """Log summary statistics about customer geographies."""
        if not customer_geographies:
            return

        # Calculate segment distribution
        segments = [cg.customer_segment for cg in customer_geographies.values()]
        segment_counts = {}
        for segment in segments:
            segment_counts[segment] = segment_counts.get(segment, 0) + 1

        # Calculate average distance to primary store
        distances = [cg.nearest_stores[0][1] for cg in customer_geographies.values() if cg.nearest_stores]
        avg_distance = np.mean(distances) if distances else 0
        median_distance = np.median(distances) if distances else 0

        logger.info("=== Customer Geography Summary ===")
        logger.info(f"Total customers: {len(customer_geographies):,}")
        logger.info("Segment distribution:")
        for segment, count in sorted(segment_counts.items(), key=lambda x: -x[1]):
            pct = (count / len(customer_geographies)) * 100
            logger.info(f"  {segment}: {count:,} ({pct:.1f}%)")
        logger.info(f"Average distance to primary store: {avg_distance:.1f} miles")
        logger.info(f"Median distance to primary store: {median_distance:.1f} miles")


class StoreSelector:
    """
    Selects stores for customer shopping trips based on geography and affinity.

    This class uses customer geography data to make realistic store selections
    where customers primarily shop at nearby stores with occasional distant visits.
    """

    def __init__(
        self,
        customer_geographies: Dict[int, CustomerGeography],
        stores: List[Store],
        seed: int = 42,
    ):
        """
        Initialize store selector.

        Args:
            customer_geographies: Dictionary mapping customer_id to CustomerGeography
            stores: List of store records
            seed: Random seed for reproducibility
        """
        self.customer_geographies = customer_geographies
        self.stores = stores
        self._rng = random.Random(seed)

        # Build store ID list for selection
        self._store_ids = [store.ID for store in stores]

        # Build store lookup
        self._store_lookup = {store.ID: store for store in stores}

        logger.info(f"StoreSelector initialized with {len(customer_geographies):,} customer geographies")

    def select_store_for_customer(self, customer_id: int) -> Store | None:
        """
        Select a store for a customer based on their geography and affinity.

        Args:
            customer_id: Customer ID

        Returns:
            Selected Store object, or None if customer geography not found
        """
        customer_geo = self.customer_geographies.get(customer_id)
        if not customer_geo:
            # Fallback: random store if geography not assigned
            logger.debug(f"No geography for customer {customer_id}, selecting random store")
            return self._rng.choice(self.stores) if self.stores else None

        # Get store selection weights
        weights = customer_geo.get_store_selection_weights(self._store_ids)
        if not weights:
            # Fallback
            return self._rng.choice(self.stores) if self.stores else None

        # Select store based on weights
        store_ids = list(weights.keys())
        probabilities = list(weights.values())

        selected_store_id = self._rng.choices(store_ids, weights=probabilities)[0]
        return self._store_lookup.get(selected_store_id)

    def get_store_customer_distribution(self, store_id: int) -> Dict[str, float]:
        """
        Get geographic distribution of customers for a given store.

        Args:
            store_id: Store ID

        Returns:
            Dictionary with distribution metrics:
            - local_pct: Percentage of customers within 10 miles
            - regional_pct: Percentage 10-30 miles away
            - distant_pct: Percentage over 30 miles away
            - median_distance: Median customer distance
        """
        distances = []

        for customer_geo in self.customer_geographies.values():
            # Find distance to this store
            for sid, dist in customer_geo.nearest_stores:
                if sid == store_id:
                    distances.append(dist)
                    break

        if not distances:
            return {
                "local_pct": 0.0,
                "regional_pct": 0.0,
                "distant_pct": 0.0,
                "median_distance": 0.0,
            }

        local_count = sum(1 for d in distances if d < 10)
        regional_count = sum(1 for d in distances if 10 <= d < 30)
        distant_count = sum(1 for d in distances if d >= 30)

        total = len(distances)

        return {
            "local_pct": (local_count / total) * 100,
            "regional_pct": (regional_count / total) * 100,
            "distant_pct": (distant_count / total) * 100,
            "median_distance": float(np.median(distances)),
        }
