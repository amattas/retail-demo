"""
Online order generation with lifecycle tracking
"""
from __future__ import annotations

import logging
from datetime import datetime
import pandas as pd
from retail_datagen.generators.online_order_generator import generate_online_orders_with_lifecycle

logger = logging.getLogger(__name__)


class OnlineOrdersMixin:
    """Online order generation with lifecycle tracking"""

    def _generate_online_orders(
        self, date: datetime
    ) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
        """Generate online orders for the given date with complete lifecycle and corresponding inventory effects.

        Delegates to generate_online_orders_with_lifecycle for full implementation including:
        - Multi-line orders (1-5 items per order via basket generation)
        - Status progression (created -> picked -> shipped -> delivered)
        - Proper tax calculation based on fulfillment location
        - Realistic tender type distribution

        Returns:
            (orders, store_inventory_txn, dc_inventory_txn, order_lines)
        """
        # Basket adjuster applies the same holiday overlay used for POS
        def _adjuster(ts: datetime, basket):
            self._apply_holiday_overlay_to_basket(ts, basket)

        return generate_online_orders_with_lifecycle(
            date=date,
            config=self.config,
            customers=self.customers,
            geographies=self.geographies,
            stores=self.stores,
            distribution_centers=self.distribution_centers,
            customer_journey_sim=self.customer_journey_sim,
            inventory_flow_sim=self.inventory_flow_sim,
            temporal_patterns=self.temporal_patterns,
            rng=self._rng,
            generate_trace_id_func=self._generate_trace_id,
            basket_adjuster=_adjuster,
        )


