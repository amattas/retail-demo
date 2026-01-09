"""
Supply chain disruption simulation.

This module handles creation, tracking, and resolution of supply chain
disruption events for realistic logistics modeling.
"""

import json
import logging
from datetime import datetime

from retail_datagen.shared.models import (
    DisruptionSeverity,
    DisruptionType,
)

logger = logging.getLogger(__name__)


class DisruptionMixin:
    """
    Mixin providing supply chain disruption functionality.

    Handles creation, tracking, and resolution of disruption events
    that affect DC operations and shipping capacity.

    Requires parent class to provide:
        - _rng: random.Random instance
        - dcs: list of DistributionCenter
        - products: list of ProductMaster
        - _active_disruptions: dict[int, dict]
        - _disruption_counter: int
    """

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
            duration_hours = (date - disruption["start_time"]).total_seconds() / 3600
            expected_duration = disruption["expected_duration_hours"]

            # 70% chance to resolve after expected duration
            resolve_probability = max(
                0.7, (duration_hours - expected_duration) / expected_duration * 0.5
            )

            if (
                duration_hours >= expected_duration
                and self._rng.random() < resolve_probability
            ):
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

        for dc_id in resolved_dcs:
            del self._active_disruptions[dc_id]

        return disruption_events

    def _create_disruption(self, dc_id: int, date: datetime) -> dict:
        """Create a new supply chain disruption event."""
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

        # Select severity
        severity_weights = {
            DisruptionSeverity.MINOR: 0.6,
            DisruptionSeverity.MODERATE: 0.3,
            DisruptionSeverity.SEVERE: 0.1,
        }
        severity = self._rng.choices(
            list(severity_weights.keys()), weights=list(severity_weights.values())
        )[0]

        # Calculate impact percentage
        impact_ranges = {
            DisruptionSeverity.MINOR: (10, 30),
            DisruptionSeverity.MODERATE: (30, 60),
            DisruptionSeverity.SEVERE: (60, 90),
        }
        impact_percentage = self._rng.randint(*impact_ranges[severity])

        # Generate duration
        duration_ranges = {
            DisruptionSeverity.MINOR: (2, 12),
            DisruptionSeverity.MODERATE: (8, 48),
            DisruptionSeverity.SEVERE: (24, 168),
        }
        expected_duration = self._rng.randint(*duration_ranges[severity])

        # Select affected products
        num_affected = max(1, int(len(self.products) * (impact_percentage / 100) * 0.3))
        affected_product_ids = [
            p.ID for p in self._rng.sample(self.products, num_affected)
        ]

        descriptions = {
            DisruptionType.CAPACITY_CONSTRAINT: (
                "Reduced capacity due to high demand surge"
            ),
            DisruptionType.EQUIPMENT_FAILURE: (
                "Equipment failure in sorting/loading systems"
            ),
            DisruptionType.WEATHER_DELAY: (
                "Weather-related delays affecting inbound shipments"
            ),
            DisruptionType.LABOR_SHORTAGE: "Staff shortage impacting operations",
            DisruptionType.SYSTEM_OUTAGE: (
                "IT system outage affecting inventory management"
            ),
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
            Capacity multiplier (1.0 = normal, 0.5 = 50% capacity)
        """
        disruptions = getattr(self, "_active_disruptions", {})
        if dc_id in disruptions:
            disruption = disruptions[dc_id]
            impact_percentage = disruption["impact_percentage"]
            return 1.0 - (impact_percentage / 100)
        return 1.0
