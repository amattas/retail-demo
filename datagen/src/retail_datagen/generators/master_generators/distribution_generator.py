"""
Distribution center and truck fleet generation.

Handles DC strategic placement and truck allocation strategies.
"""

import logging
from dataclasses import dataclass

from retail_datagen.config.models import RetailConfig
from retail_datagen.shared.models import (
    DistributionCenter,
    GeographyDict,
    GeographyMaster,
    Truck,
)

from ..utils import AddressGenerator, GeographicDistribution, IdentifierGenerator

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------------
# Dataclasses for Helper Method Return Types
# -------------------------------------------------------------------------


@dataclass
class TruckAllocationStrategy:
    """Result of truck allocation strategy calculation."""

    assignment_strategy: str  # "fixed" or "percentage"
    num_assigned_trucks: int  # Trucks assigned to specific DCs
    num_pool_trucks: int  # Trucks in shared pool (DCID=NULL)
    assigned_refrigerated: int  # Refrigerated trucks in assigned group
    assigned_non_refrigerated: int  # Non-refrigerated trucks in assigned group
    pool_refrigerated: int  # Refrigerated trucks in pool
    pool_non_refrigerated: int  # Non-refrigerated trucks in pool
    trucks_per_dc: int  # Fixed trucks per DC (0 if percentage-based)
    total_dc_trucks: int  # Total DC trucks


@dataclass
class AssignedTrucksResult:
    """Result of assigned trucks generation."""

    next_id: int  # Next available truck ID
    trucks_generated: int  # Count of trucks created
    dc_assignment_list: list[
        tuple[int, int, bool]
    ]  # (truck_id, dc_id, is_refrigerated)


@dataclass
class PoolTrucksResult:
    """Result of pool trucks generation."""

    next_id: int  # Next available truck ID
    trucks_generated: int  # Count of pool trucks created


class DistributionGeneratorMixin:
    """Mixin for distribution center and truck generation."""

    def generate_distribution_centers(
        self,
        dc_count: int,
        geography_master: list[GeographyMaster],
        selected_geography_data: list[GeographyDict],
        seed: int,
    ) -> list[DistributionCenter]:
        """
        Generate distribution centers with strategic placement.

        Args:
            dc_count: Number of DCs to generate
            geography_master: Geography dimension records
            selected_geography_data: Geography dict subset
            seed: Random seed

        Returns:
            List of DistributionCenter records
        """
        print("Generating distribution center data...")

        if not geography_master:
            raise ValueError("Geography master data must be generated first")

        # Initialize utilities
        geo_distribution = GeographicDistribution(selected_geography_data, seed + 1000)
        address_generator = AddressGenerator(selected_geography_data, seed + 1000)
        id_generator = IdentifierGenerator(seed + 1000)

        # DCs should be strategically placed in highest-weight regions
        strategic_geos = geo_distribution.get_strategic_locations(dc_count)

        distribution_centers = []

        # Build a fast key index
        geo_key_index = {
            (gm.City, gm.State, str(gm.ZipCode)): gm for gm in geography_master
        }

        for i, geo in enumerate(strategic_geos, 1):
            geo_master = geo_key_index.get((geo.City, geo.State, str(geo.Zip)))
            if geo_master is None:
                raise ValueError(
                    "Geography key not found for distribution center placement"
                )

            dc = DistributionCenter(
                ID=i,
                DCNumber=id_generator.generate_dc_number(i),
                Address=address_generator.generate_address(geo, "industrial"),
                GeographyID=geo_master.ID,
            )
            distribution_centers.append(dc)

        print(f"Generated {len(distribution_centers)} distribution center records")
        return distribution_centers

    # -------------------------------------------------------------------------
    # Truck Generation Helper Methods
    # -------------------------------------------------------------------------

    def _create_truck(
        self,
        truck_id: int,
        id_generator: IdentifierGenerator,
        is_refrigerated: bool,
        dc_id: int | None,
    ) -> Truck:
        """Create a single truck instance."""
        return Truck(
            ID=truck_id,
            LicensePlate=id_generator.generate_license_plate(truck_id),
            Refrigeration=is_refrigerated,
            DCID=dc_id,
        )

    def _calculate_truck_allocation_strategy(
        self, config: RetailConfig, dc_count: int
    ) -> TruckAllocationStrategy:
        """Calculate truck allocation strategy and refrigeration splits."""
        refrigerated_count = config.volume.refrigerated_trucks
        non_refrigerated_count = config.volume.non_refrigerated_trucks
        total_dc_trucks = refrigerated_count + non_refrigerated_count

        trucks_per_dc_config = config.volume.trucks_per_dc
        truck_dc_assignment_rate = config.volume.truck_dc_assignment_rate

        if trucks_per_dc_config is not None:
            # Fixed assignment: exactly N trucks per DC
            num_assigned_trucks = trucks_per_dc_config * dc_count
            num_pool_trucks = max(0, total_dc_trucks - num_assigned_trucks)
            assignment_strategy = "fixed"
            print(f"Using fixed assignment: {trucks_per_dc_config} trucks per DC")
            print(
                f"  - Assigned trucks: {num_assigned_trucks} ({trucks_per_dc_config} × {dc_count} DCs)"
            )
            print(f"  - Pool trucks (DCID=NULL): {num_pool_trucks}")
        else:
            # Percentage-based assignment
            num_assigned_trucks = int(total_dc_trucks * truck_dc_assignment_rate)
            num_pool_trucks = total_dc_trucks - num_assigned_trucks
            assignment_strategy = "percentage"
            trucks_per_dc_config = 0
            print(
                f"Using percentage-based assignment: {truck_dc_assignment_rate:.1%} of trucks assigned to DCs"
            )
            print(f"  - Assigned trucks: {num_assigned_trucks}")
            print(f"  - Pool trucks (DCID=NULL): {num_pool_trucks}")

        # Calculate refrigeration splits
        if num_assigned_trucks > 0 and total_dc_trucks > 0:
            assigned_refrigerated = round(
                num_assigned_trucks * (refrigerated_count / total_dc_trucks)
            )
            assigned_non_refrigerated = num_assigned_trucks - assigned_refrigerated
        else:
            assigned_refrigerated = 0
            assigned_non_refrigerated = 0

        pool_refrigerated = refrigerated_count - assigned_refrigerated
        pool_non_refrigerated = non_refrigerated_count - assigned_non_refrigerated

        return TruckAllocationStrategy(
            assignment_strategy=assignment_strategy,
            num_assigned_trucks=num_assigned_trucks,
            num_pool_trucks=num_pool_trucks,
            assigned_refrigerated=assigned_refrigerated,
            assigned_non_refrigerated=assigned_non_refrigerated,
            pool_refrigerated=pool_refrigerated,
            pool_non_refrigerated=pool_non_refrigerated,
            trucks_per_dc=trucks_per_dc_config if trucks_per_dc_config else 0,
            total_dc_trucks=total_dc_trucks,
        )

    def _generate_assigned_trucks(
        self,
        distribution_centers: list[DistributionCenter],
        id_generator: IdentifierGenerator,
        start_id: int,
        strategy: TruckAllocationStrategy,
    ) -> tuple[list[Truck], AssignedTrucksResult]:
        """Generate trucks assigned to specific DCs."""
        current_id = start_id
        trucks = []
        refrigerated_assigned = 0
        dc_assignment_list: list[tuple[int, int, bool]] = []
        dc_count = len(distribution_centers)

        if strategy.assignment_strategy == "fixed" and strategy.trucks_per_dc > 0:
            # Fixed assignment: exactly trucks_per_dc per DC
            for dc in distribution_centers:
                dc_refrigerated = max(
                    0,
                    min(
                        strategy.trucks_per_dc,
                        strategy.assigned_refrigerated - refrigerated_assigned,
                    ),
                )
                dc_non_refrigerated = max(0, strategy.trucks_per_dc - dc_refrigerated)

                # Create refrigerated trucks for this DC
                for _ in range(dc_refrigerated):
                    truck = self._create_truck(current_id, id_generator, True, dc.ID)
                    trucks.append(truck)
                    dc_assignment_list.append((current_id, dc.ID, True))
                    current_id += 1
                    refrigerated_assigned += 1

                # Create non-refrigerated trucks for this DC
                for _ in range(dc_non_refrigerated):
                    truck = self._create_truck(current_id, id_generator, False, dc.ID)
                    trucks.append(truck)
                    dc_assignment_list.append((current_id, dc.ID, False))
                    current_id += 1
        else:
            # Percentage-based: round-robin distribution
            dc_index = 0

            # Generate refrigerated assigned trucks
            for _ in range(strategy.assigned_refrigerated):
                dc = distribution_centers[dc_index % dc_count]
                truck = self._create_truck(current_id, id_generator, True, dc.ID)
                trucks.append(truck)
                dc_assignment_list.append((current_id, dc.ID, True))
                current_id += 1
                dc_index += 1

            # Generate non-refrigerated assigned trucks
            for _ in range(strategy.assigned_non_refrigerated):
                dc = distribution_centers[dc_index % dc_count]
                truck = self._create_truck(current_id, id_generator, False, dc.ID)
                trucks.append(truck)
                dc_assignment_list.append((current_id, dc.ID, False))
                current_id += 1
                dc_index += 1

        result = AssignedTrucksResult(
            next_id=current_id,
            trucks_generated=len(trucks),
            dc_assignment_list=dc_assignment_list,
        )
        return trucks, result

    def _generate_pool_trucks(
        self,
        id_generator: IdentifierGenerator,
        start_id: int,
        strategy: TruckAllocationStrategy,
    ) -> tuple[list[Truck], PoolTrucksResult]:
        """Generate pool trucks (DCID=None)."""
        current_id = start_id
        trucks = []

        # Pool refrigerated trucks
        for _ in range(strategy.pool_refrigerated):
            truck = self._create_truck(current_id, id_generator, True, None)
            trucks.append(truck)
            current_id += 1

        # Pool non-refrigerated trucks
        for _ in range(strategy.pool_non_refrigerated):
            truck = self._create_truck(current_id, id_generator, False, None)
            trucks.append(truck)
            current_id += 1

        result = PoolTrucksResult(next_id=current_id, trucks_generated=len(trucks))
        return trucks, result

    def _generate_supplier_trucks(
        self,
        config: RetailConfig,
        id_generator: IdentifierGenerator,
        start_id: int,
    ) -> list[Truck]:
        """Generate supplier-to-DC trucks (always pool trucks with DCID=None)."""
        current_id = start_id
        trucks = []
        supplier_refrigerated = config.volume.supplier_refrigerated_trucks
        supplier_non_refrigerated = config.volume.supplier_non_refrigerated_trucks
        supplier_total = supplier_refrigerated + supplier_non_refrigerated

        print(
            f"\nGenerating {supplier_total} supplier trucks "
            f"({supplier_refrigerated} refrigerated, {supplier_non_refrigerated} non-refrigerated)"
        )

        # Generate supplier refrigerated trucks
        for _ in range(supplier_refrigerated):
            truck = self._create_truck(current_id, id_generator, True, None)
            trucks.append(truck)
            current_id += 1

        # Generate supplier non-refrigerated trucks
        for _ in range(supplier_non_refrigerated):
            truck = self._create_truck(current_id, id_generator, False, None)
            trucks.append(truck)
            current_id += 1

        return trucks

    def _print_truck_generation_summary(
        self,
        config: RetailConfig,
        strategy: TruckAllocationStrategy,
        assigned_result: AssignedTrucksResult,
        pool_result: PoolTrucksResult,
        total_trucks: int,
    ) -> None:
        """Print detailed truck generation summary."""
        supplier_refrigerated = config.volume.supplier_refrigerated_trucks
        supplier_non_refrigerated = config.volume.supplier_non_refrigerated_trucks
        supplier_total = supplier_refrigerated + supplier_non_refrigerated

        print("\n=== Truck Generation Summary ===")
        print(f"Total trucks generated: {total_trucks}")
        print(f"\nDC-to-Store Fleet ({strategy.total_dc_trucks} trucks):")
        print(f"  - Assigned to DCs: {assigned_result.trucks_generated}")
        print(f"    • Refrigerated: {strategy.assigned_refrigerated}")
        print(f"    • Non-refrigerated: {strategy.assigned_non_refrigerated}")
        print(f"  - Pool trucks (DCID=NULL): {pool_result.trucks_generated}")
        print(f"    • Refrigerated: {strategy.pool_refrigerated}")
        print(f"    • Non-refrigerated: {strategy.pool_non_refrigerated}")
        print(f"\nSupplier-to-DC Fleet ({supplier_total} trucks):")
        print("  - All pool trucks (DCID=NULL)")
        print(f"    • Refrigerated: {supplier_refrigerated}")
        print(f"    • Non-refrigerated: {supplier_non_refrigerated}")

        # Show per-DC distribution for assigned trucks
        if assigned_result.dc_assignment_list:
            if strategy.assignment_strategy == "fixed":
                print(
                    f"\nPer-DC Assignment (fixed: {strategy.trucks_per_dc} trucks/DC):"
                )
            else:
                print("\nPer-DC Assignment (round-robin distribution):")

            dc_truck_counts: dict[int, dict[str, int]] = {}
            for _, dc_id, is_ref in assigned_result.dc_assignment_list:
                if dc_id not in dc_truck_counts:
                    dc_truck_counts[dc_id] = {"refrigerated": 0, "non_refrigerated": 0}
                if is_ref:
                    dc_truck_counts[dc_id]["refrigerated"] += 1
                else:
                    dc_truck_counts[dc_id]["non_refrigerated"] += 1

            for dc_id in sorted(dc_truck_counts.keys()):
                counts = dc_truck_counts[dc_id]
                total_dc = counts["refrigerated"] + counts["non_refrigerated"]
                print(
                    f"  DC {dc_id}: {total_dc} trucks "
                    f"({counts['refrigerated']} refrigerated, {counts['non_refrigerated']} non-refrigerated)"
                )

    def generate_trucks(
        self,
        config: RetailConfig,
        distribution_centers: list[DistributionCenter],
        seed: int,
    ) -> list[Truck]:
        """
        Generate trucks with refrigeration capabilities and DC assignment.

        Args:
            config: Retail configuration
            distribution_centers: DC records
            seed: Random seed

        Returns:
            List of Truck records
        """
        try:
            print("Generating truck data...")
            print(f"Distribution centers available: {len(distribution_centers)}")

            if not distribution_centers:
                raise ValueError("Distribution center data must be generated first")

            refrigerated_count = config.volume.refrigerated_trucks
            non_refrigerated_count = config.volume.non_refrigerated_trucks
            total_dc_trucks = refrigerated_count + non_refrigerated_count
            print(
                f"DC-to-Store trucks to generate: {total_dc_trucks} "
                f"({refrigerated_count} refrigerated, {non_refrigerated_count} non-refrigerated)"
            )

            # Calculate allocation strategy
            strategy = self._calculate_truck_allocation_strategy(
                config, len(distribution_centers)
            )

            # Initialize
            id_generator = IdentifierGenerator(seed + 3000)
            all_trucks = []
            current_id = 1

            # Generate assigned trucks
            assigned_trucks, assigned_result = self._generate_assigned_trucks(
                distribution_centers, id_generator, current_id, strategy
            )
            all_trucks.extend(assigned_trucks)
            current_id = assigned_result.next_id

            # Generate pool trucks
            pool_trucks, pool_result = self._generate_pool_trucks(
                id_generator, current_id, strategy
            )
            all_trucks.extend(pool_trucks)
            current_id = pool_result.next_id

            # Generate supplier trucks
            supplier_trucks = self._generate_supplier_trucks(
                config, id_generator, current_id
            )
            all_trucks.extend(supplier_trucks)

            # Print summary
            self._print_truck_generation_summary(
                config, strategy, assigned_result, pool_result, len(all_trucks)
            )

            return all_trucks
        except Exception as e:
            print(f"ERROR in generate_trucks: {e}")
            import traceback

            traceback.print_exc()
            raise
