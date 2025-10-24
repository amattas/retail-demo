"""
Historical fact data generation engine for retail data generator.

This module implements the FactDataGenerator class that creates realistic
retail transaction data for all 8 fact tables with proper temporal patterns,
business logic coordination, and partitioned output.
"""

import inspect
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from multiprocessing import cpu_count
from pathlib import Path
from threading import Lock
from typing import Any, Callable

import pandas as pd
from retail_datagen.generators.seasonal_patterns import CompositeTemporalPatterns
from retail_datagen.generators.utils import DataFrameExporter, ProgressReporter
from retail_datagen.shared.cache import CacheManager
from retail_datagen.shared.models import (
    Customer,
    DistributionCenter,
    GeographyMaster,
    InventoryReason,
    ProductMaster,
    Store,
    TenderType,
)

from ..config.models import RetailConfig
from .retail_patterns import (
    BusinessRulesEngine,
    CustomerJourneySimulator,
    InventoryFlowSimulator,
    MarketingCampaignSimulator,
)

logger = logging.getLogger(__name__)


@dataclass
class FactGenerationSummary:
    """Summary of fact data generation results."""

    date_range: tuple[datetime, datetime]
    facts_generated: dict[str, int]
    total_records: int
    validation_results: dict[str, Any]
    generation_time_seconds: float
    partitions_created: int


@dataclass(frozen=True)
class MasterTableSpec:
    """Configuration describing how to load a master table."""

    attr_name: str
    filename: str
    model_cls: type[Any]
    dtype: dict[str, Any] | None = None
    row_adapter: Callable[[dict[str, Any]], dict[str, Any]] | None = None


class FactDataGenerator:
    """
    Main historical fact data generation engine.

    Generates all 8 fact tables with realistic retail behaviors, temporal patterns,
    and cross-fact coordination while maintaining business rule compliance.
    """

    # Define the core fact tables that are always generated
    FACT_TABLES = [
        "dc_inventory_txn",
        "truck_moves",
        "truck_inventory",
        "store_inventory_txn",
        "receipts",
        "receipt_lines",
        "foot_traffic",
        "ble_pings",
        # Omnichannel extension integrated into core facts
        "online_orders",
    ]

    def __init__(self, config: RetailConfig):
        """
        Initialize fact data generator.

        Args:
            config: Retail configuration containing generation parameters
        """
        self.config = config
        self._rng = random.Random(config.seed)

        # Initialize patterns and simulators
        self.temporal_patterns = CompositeTemporalPatterns(config.seed)
        self.business_rules = BusinessRulesEngine()

        # Master data will be loaded from existing files
        self.geographies: list[GeographyMaster] = []
        self.stores: list[Store] = []
        self.distribution_centers: list[DistributionCenter] = []
        self.customers: list[Customer] = []
        self.products: list[ProductMaster] = []

        # Simulators will be initialized after loading master data
        self.customer_journey_sim: CustomerJourneySimulator | None = None
        self.inventory_flow_sim: InventoryFlowSimulator | None = None
        self.marketing_campaign_sim: MarketingCampaignSimulator | None = None

        # Track active campaigns and shipments
        self._active_campaigns: dict[str, Any] = {}
        self._active_shipments: dict[str, Any] = {}

        # Generate unique trace IDs
        self._trace_counter = 1

        # Progress callback for API integration (day-based throttled status)
        self._progress_callback = None
        # Per-table (master-style) progress callback
        self._table_progress_callback: Callable[[str, float, str | None, dict | None], None] | None = None

        # Progress throttling for API updates (prevent flooding)
        self._last_progress_update_time = 0.0
        self._progress_lock = Lock()
        self._progress_history: list[tuple[float, float]] = []

        # Table state tracking for enhanced progress reporting
        self._table_states: dict[str, str] = {}
        self._reset_table_states()

        print(f"FactDataGenerator initialized with seed {config.seed}")

    @staticmethod
    def _normalize_geography_row(row: dict[str, Any]) -> dict[str, Any]:
        """Ensure geography rows preserve ZipCode formatting."""

        zip_code = row.get("ZipCode", "")
        return {**row, "ZipCode": str(zip_code)}

    @staticmethod
    def _to_decimal(value: Any) -> Decimal:
        """Convert a value into a Decimal instance."""

        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    @staticmethod
    def _to_bool(value: Any) -> bool:
        """Convert truthy strings and numerics to bool safely."""

        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes", "y", "t"}
        return bool(value)

    @classmethod
    def _normalize_product_row(cls, row: dict[str, Any]) -> dict[str, Any]:
        """Perform type conversions for product master rows."""

        launch_date = row.get("LaunchDate")
        if isinstance(launch_date, datetime):
            parsed_launch_date = launch_date
        else:
            parsed_launch_date = datetime.fromisoformat(str(launch_date))

        return {
            **row,
            "Cost": cls._to_decimal(row.get("Cost")),
            "MSRP": cls._to_decimal(row.get("MSRP")),
            "SalePrice": cls._to_decimal(row.get("SalePrice")),
            "RequiresRefrigeration": cls._to_bool(
                row.get("RequiresRefrigeration", False)
            ),
            "LaunchDate": parsed_launch_date,
        }

    def _master_table_specs(self) -> list[MasterTableSpec]:
        """Return specifications for all master tables that need loading."""

        return [
            MasterTableSpec(
                attr_name="geographies",
                filename="geographies_master.csv",
                model_cls=GeographyMaster,
                dtype={"ZipCode": str},
                row_adapter=self._normalize_geography_row,
            ),
            MasterTableSpec(
                attr_name="stores",
                filename="stores.csv",
                model_cls=Store,
            ),
            MasterTableSpec(
                attr_name="distribution_centers",
                filename="distribution_centers.csv",
                model_cls=DistributionCenter,
            ),
            MasterTableSpec(
                attr_name="customers",
                filename="customers.csv",
                model_cls=Customer,
            ),
            MasterTableSpec(
                attr_name="products",
                filename="products_master.csv",
                model_cls=ProductMaster,
                row_adapter=self._normalize_product_row,
            ),
        ]

    def _load_master_table(
        self, master_path: Path, spec: MasterTableSpec
    ) -> list[Any]:
        """Load a master table based on the supplied specification."""

        dataframe = pd.read_csv(master_path / spec.filename, dtype=spec.dtype)
        records: list[Any] = []
        for _, row in dataframe.iterrows():
            row_dict = row.to_dict()
            if spec.row_adapter:
                row_dict = spec.row_adapter(row_dict)
            records.append(spec.model_cls(**row_dict))
        return records

    def _reset_table_states(self) -> None:
        """Reset table states to 'not_started' for all tables."""
        self._table_states = {table: "not_started" for table in self.FACT_TABLES}
        self._progress_history = []

    def _calculate_eta(self, current_progress: float) -> float | None:
        """
        Calculate estimated seconds remaining based on progress rate.

        Args:
            current_progress: Current progress as a fraction (0.0 to 1.0)

        Returns:
            Estimated seconds remaining, or None if not enough data
        """
        if len(self._progress_history) < 2:
            return None

        # Calculate progress rate from history
        oldest = self._progress_history[0]
        newest = self._progress_history[-1]
        time_elapsed = newest[0] - oldest[0]
        progress_made = newest[1] - oldest[1]

        if progress_made <= 0 or time_elapsed <= 0:
            return None

        progress_rate = progress_made / time_elapsed  # progress per second
        remaining_progress = 1.0 - current_progress

        if progress_rate > 0:
            return remaining_progress / progress_rate

        return None

    # Per-table progress (master-style), similar to MasterDataGenerator
    def set_table_progress_callback(
        self,
        callback: Callable[[str, float, str | None, dict | None], None] | None,
    ) -> None:
        self._table_progress_callback = callback

    def _emit_table_progress(
        self,
        table_name: str,
        progress: float,
        message: str | None = None,
        table_counts: dict | None = None,
    ) -> None:
        if not self._table_progress_callback:
            return
        try:
            clamped = max(0.0, min(1.0, progress))
            self._table_progress_callback(table_name, clamped, message, table_counts)
        except Exception:
            pass

    def _update_table_states(self, table_progress: dict[str, float]) -> None:
        """
        Update table states based on current progress.

        Args:
            table_progress: Dictionary mapping table names to progress (0.0 to 1.0)
        """
        for table_name, progress in table_progress.items():
            if table_name not in self._table_states:
                continue

            current_state = self._table_states[table_name]

            if progress >= 1.0 and current_state != "completed":
                self._table_states[table_name] = "completed"
            elif progress > 0.0 and current_state == "not_started":
                self._table_states[table_name] = "in_progress"

    def load_master_data(self) -> None:
        """Load master data from existing CSV files."""
        print("Loading master data for fact generation...")

        master_path = Path(self.config.paths.master)

        loaded_counts: dict[str, int] = {}
        for spec in self._master_table_specs():
            records = self._load_master_table(master_path, spec)
            setattr(self, spec.attr_name, records)
            loaded_counts[spec.attr_name] = len(records)

        # Initialize simulators with loaded data
        self.customer_journey_sim = CustomerJourneySimulator(
            self.customers, self.products, self.stores, self.config.seed + 1000
        )

        self.inventory_flow_sim = InventoryFlowSimulator(
            self.distribution_centers,
            self.stores,
            self.products,
            self.config.seed + 2000,
        )

        self.marketing_campaign_sim = MarketingCampaignSimulator(
            self.customers, self.config.seed + 3000
        )

        print(
            "Loaded master data: "
            f"{loaded_counts.get('geographies', 0)} geographies, "
            f"{loaded_counts.get('stores', 0)} stores, "
            f"{loaded_counts.get('distribution_centers', 0)} DCs, "
            f"{loaded_counts.get('customers', 0)} customers, "
            f"{loaded_counts.get('products', 0)} products"
        )

    def generate_historical_data(
        self, start_date: datetime, end_date: datetime, parallel: bool = True
    ) -> FactGenerationSummary:
        """
        Generate historical fact data for the specified date range.

        Args:
            start_date: Start of historical data generation
            end_date: End of historical data generation
            parallel: Enable parallel day-by-day processing (default True)

        Returns:
            Summary of generation results
        """
        generation_start_time = datetime.now()
        print(
            f"Starting historical fact data generation from {start_date} to {end_date}"
        )

        # Reset table states for new generation run
        self._reset_table_states()

        # Ensure master data is loaded
        if not self.stores:
            self.load_master_data()

        # Initialize tracking
        facts_generated = {
            "dc_inventory_txn": 0,
            "truck_moves": 0,
            "truck_inventory": 0,
            "store_inventory_txn": 0,
            "receipts": 0,
            "receipt_lines": 0,
            "foot_traffic": 0,
            "ble_pings": 0,
            "marketing": 0,
            "supply_chain_disruption": 0,
            "online_orders": 0,
        }

        # NEW: Add table progress tracking
        table_progress = {table: 0.0 for table in facts_generated.keys()}

        total_days = (end_date - start_date).days + 1

        # Calculate expected records per table for accurate progress tracking
        customers_per_day = self.config.volume.customers_per_day
        expected_records = {
            "receipts": total_days * customers_per_day,
            "receipt_lines": total_days * customers_per_day * 3,  # ~3 items per receipt
            "foot_traffic": total_days * len(self.stores) * 100,  # ~100 visits per store per day
            "ble_pings": total_days * len(self.stores) * 500,  # ~500 pings per store per day
            "dc_inventory_txn": total_days * len(self.distribution_centers) * 50,  # ~50 txns per DC per day
            "truck_moves": total_days * 10,  # ~10 moves total per day
            "truck_inventory": total_days * 20,  # ~20 truck inventory events per day
            "store_inventory_txn": total_days * len(self.stores) * 20,  # ~20 txns per store per day
            "marketing": total_days * 10,  # ~10 campaigns per day
            "supply_chain_disruption": total_days * 2,  # ~2 disruptions per day
            "online_orders": total_days * max(0, int(self.config.volume.online_orders_per_day)),
        }

        progress_reporter = ProgressReporter(total_days, "Generating historical data")
        partitions_created = 0

        # Generate data day by day
        current_date = start_date
        day_counter = 0

        if parallel:
            # PARALLEL PROCESSING: Use ThreadPoolExecutor for day-by-day
            max_workers = min(cpu_count(), 8)  # Cap at 8 to avoid memory issues
            print(f"Using parallel processing with {max_workers} thread workers")

            # Generate list of all dates
            dates_to_process = []
            temp_date = start_date
            while temp_date <= end_date:
                dates_to_process.append(temp_date)
                temp_date += timedelta(days=1)

            # Process days in parallel
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all days for processing with progress reporting enabled
                future_to_date = {
                    executor.submit(
                        self._generate_and_export_day,
                        date,
                        day_idx + 1,
                        total_days,
                        report_progress=True  # Enable progress reporting from workers
                    ): date
                    for day_idx, date in enumerate(dates_to_process)
                }

                # Collect results as they complete
                for future in as_completed(future_to_date):
                    date = future_to_date[future]
                    try:
                        daily_counts, partition_count = future.result()

                        # Update counters
                        for fact_type, count in daily_counts.items():
                            facts_generated[fact_type] += count
                        partitions_created += partition_count

                        day_counter += 1
                        progress_reporter.update(1)

                        # Note: Progress updates are now sent from worker threads
                        # No need to send them again from main thread
                    except Exception as e:
                        print(f"Error processing {date}: {e}")
                        raise
        else:
            # SEQUENTIAL PROCESSING: Original loop (fallback)
            while current_date <= end_date:
                day_counter += 1

                daily_facts = self._generate_daily_facts(current_date)

                # Export daily facts to partitioned files
                partition_count = self._export_daily_facts(current_date, daily_facts)
                partitions_created += partition_count

                # Update counters
                for fact_type, records in daily_facts.items():
                    facts_generated[fact_type] += len(records)

                # Update per-table progress based on actual records generated
                for fact_type in facts_generated.keys():
                    current_count = facts_generated[fact_type]
                    expected = expected_records.get(fact_type, 1)
                    # Calculate actual progress (0.0 to 1.0), never exceed 1.0
                    table_progress[fact_type] = min(1.0, current_count / expected) if expected > 0 else 0.0

                # Emit per-table progress (master-style)
                for fact_type, prog in table_progress.items():
                    self._emit_table_progress(
                        fact_type,
                        prog,
                        f"Generating {fact_type.replace('_',' ')}",
                        None,
                    )

                # Update table states based on progress
                self._update_table_states(table_progress)

                # Calculate tables completed count
                tables_completed_count = sum(
                    1 for state in self._table_states.values()
                    if state == "completed"
                )

                # Enhanced message with table completion count
                enhanced_message = (
                    f"Generating data for {current_date.strftime('%Y-%m-%d')} "
                    f"(day {day_counter}/{total_days}) "
                    f"({tables_completed_count}/{len(self.FACT_TABLES)} tables complete)"
                )

                # Get table lists for detailed reporting
                tables_completed = [
                    table for table, state in self._table_states.items()
                    if state == "completed"
                ]
                tables_in_progress = [
                    table for table, state in self._table_states.items()
                    if state == "in_progress"
                ]
                tables_remaining = [
                    table for table, state in self._table_states.items()
                    if state == "not_started"
                ]

                # Update API progress with throttling
                self._send_throttled_progress_update(
                    day_counter,
                    enhanced_message,
                    total_days,
                    table_progress=table_progress,
                    tables_completed=tables_completed,
                    tables_in_progress=tables_in_progress,
                    tables_remaining=tables_remaining,
                )

                progress_reporter.update(1)
                current_date += timedelta(days=1)

        progress_reporter.complete()

        # Final validation
        validation_results = self.business_rules.get_validation_summary()

        generation_end_time = datetime.now()
        generation_time = (generation_end_time - generation_start_time).total_seconds()

        total_records = sum(facts_generated.values())

        summary = FactGenerationSummary(
            date_range=(start_date, end_date),
            facts_generated=facts_generated,
            total_records=total_records,
            validation_results=validation_results,
            generation_time_seconds=generation_time,
            partitions_created=partitions_created,
        )

        print(
            f"Historical data generation complete: {total_records} records "
            f"in {generation_time:.1f}s"
        )
        print(
            f"Generated {partitions_created} partitions "
            f"across {len(facts_generated)} fact tables"
        )

        # Cache the counts for dashboard performance
        self._cache_fact_counts(facts_generated)

        return summary

    def _generate_daily_facts(self, date: datetime) -> dict[str, list[dict]]:
        """
        Generate all fact data for a single day.

        Args:
            date: Date to generate facts for

        Returns:
            Dictionary of fact tables with their records
        """
        daily_facts = {
            "dc_inventory_txn": [],
            "truck_moves": [],
            "truck_inventory": [],
            "store_inventory_txn": [],
            "receipts": [],
            "receipt_lines": [],
            "foot_traffic": [],
            "ble_pings": [],
            "marketing": [],
            "supply_chain_disruption": [],
            "online_orders": [],
        }

        # Update available products for this date
        available_products = self._get_available_products_for_date(date)
        if self.customer_journey_sim:
            self.customer_journey_sim.update_available_products(available_products)

        # Generate base activity level for the day
        base_multiplier = self.temporal_patterns.get_overall_multiplier(date)

        # 1. Generate DC inventory transactions (supplier deliveries)
        dc_transactions = self._generate_dc_inventory_transactions(
            date, base_multiplier
        )
        daily_facts["dc_inventory_txn"].extend(dc_transactions)

        # 2. Generate marketing campaigns and impressions
        # Digital marketing runs 24/7 independently of store traffic/hours
        # Use constant multiplier of 1.0 for consistent digital ad delivery
        marketing_records = self._generate_marketing_activity(date, 1.0)
        if not marketing_records:
            logger.debug(
                f"No marketing records generated for {date.strftime('%Y-%m-%d')}"
            )
        else:
            logger.debug(
                f"Generated {len(marketing_records)} marketing records for {date.strftime('%Y-%m-%d')}"
            )
        daily_facts["marketing"].extend(marketing_records)

        # 3. Generate store operations throughout the day
        hourly_data = self._generate_hourly_store_activity(date, base_multiplier)

        # Aggregate hourly data into daily facts
        for hour_data in hourly_data:
            for fact_type, records in hour_data.items():
                daily_facts[fact_type].extend(records)

        # 4. Generate truck movements (based on inventory needs)
        truck_movements = self._generate_truck_movements(
            date, daily_facts["store_inventory_txn"]
        )
        daily_facts["truck_moves"].extend(truck_movements)

        # 4a. Generate truck inventory tracking events
        truck_inventory_events = self.inventory_flow_sim.track_truck_inventory_status(
            date
        )
        for event in truck_inventory_events:
            daily_facts["truck_inventory"].append(
                {
                    "TraceId": self._generate_trace_id(),
                    "EventTS": self._randomize_time_within_day(event["EventTS"]),
                    "TruckId": event["TruckId"],
                    "ShipmentId": event["ShipmentId"],
                    "ProductID": event["ProductID"],
                    "Quantity": event["Quantity"],
                    "Action": event["Action"],
                    "LocationID": event["LocationID"],
                    "LocationType": event["LocationType"],
                }
            )

        # 5. Update inventory based on truck deliveries
        delivery_transactions = self._process_truck_deliveries(
            date, daily_facts["truck_moves"]
        )
        daily_facts["store_inventory_txn"].extend(delivery_transactions)

        # 6. Generate online orders and integrate inventory effects
        online_orders, online_store_txn, online_dc_txn = self._generate_online_orders(
            date
        )
        daily_facts["online_orders"].extend(online_orders)
        daily_facts["store_inventory_txn"].extend(online_store_txn)
        daily_facts["dc_inventory_txn"].extend(online_dc_txn)

        # 7. Generate supply chain disruptions
        disruption_events = self.inventory_flow_sim.simulate_supply_chain_disruptions(
            date
        )
        for disruption in disruption_events:
            daily_facts["supply_chain_disruption"].append(
                {
                    "TraceId": self._generate_trace_id(),
                    "EventTS": self._randomize_time_within_day(disruption["EventTS"]),
                    "DCID": disruption["DCID"],
                    "Type": disruption["DisruptionType"].value,
                    "Severity": disruption["Severity"].value,
                    "Description": disruption["Description"],
                    "StartTime": disruption["StartTime"],
                    "EndTime": disruption["EndTime"],
                    "ImpactPercentage": disruption["ImpactPercentage"],
                    "AffectedProducts": disruption["AffectedProducts"],
                }
            )

        return daily_facts

    def _generate_online_orders(self, date: datetime) -> tuple[list[dict], list[dict], list[dict]]:
        """Generate online orders for the given date and corresponding inventory effects.

        Returns:
            (orders, store_inventory_txn, dc_inventory_txn)
        """
        orders: list[dict] = []
        store_txn: list[dict] = []
        dc_txn: list[dict] = []

        base_per_day = max(0, int(self.config.volume.online_orders_per_day))
        if base_per_day == 0 or not self.customers:
            return orders, store_txn, dc_txn

        # Seasonality/holiday multiplier, not bounded by store hours
        seasonal_mult = self.temporal_patterns.seasonal.get_seasonal_multiplier(date)
        # Smooth out extremes
        seasonal_mult = max(0.5, min(seasonal_mult, 2.5))
        total_orders = max(0, int(base_per_day * seasonal_mult))

        # Convenience
        rng = self._rng
        # InventoryReason and TenderType already imported from shared.models at module level

        for i in range(total_orders):
            # Random event time during the day
            hour = rng.randint(0, 23)
            minute = rng.randint(0, 59)
            second = rng.randint(0, 59)
            event_ts = datetime(
                date.year, date.month, date.day, hour, minute, second
            )

            customer = rng.choice(self.customers)

            # Generate a small basket using the same simulator
            basket = self.customer_journey_sim.generate_shopping_basket(customer.ID)

            # Choose fulfillment mode and node
            mode = rng.choices(
                ["SHIP_FROM_STORE", "SHIP_FROM_DC", "BOPIS"],
                weights=[0.55, 0.35, 0.10],
            )[0]

            if mode in ("SHIP_FROM_STORE", "BOPIS") and self.stores:
                node_type = "STORE"
                store = rng.choice(self.stores)
                node_id = store.ID
            else:
                node_type = "DC"
                dc = rng.choice(self.distribution_centers) if self.distribution_centers else None
                if not dc:
                    # Fallback to store if no DCs
                    node_type = "STORE"
                    store = rng.choice(self.stores)
                    node_id = store.ID
                else:
                    node_id = dc.ID

            # Tally totals similar to receipts
            subtotal = basket.estimated_total
            tax_rate = Decimal("0.08")
            tax = subtotal * tax_rate
            total = subtotal + tax

            order_id = (
                f"ONL{date.strftime('%Y%m%d')}{i:05d}{rng.randint(100,999)}"
            )
            trace_id = self._generate_trace_id()

            orders.append(
                {
                    "TraceId": trace_id,
                    "EventTS": event_ts,
                    "OrderId": order_id,
                    "CustomerID": customer.ID,
                    "FulfillmentMode": mode,
                    "FulfillmentNodeType": node_type,
                    "FulfillmentNodeID": node_id,
                    "Subtotal": str(subtotal),
                    "Tax": str(tax),
                    "Total": str(total),
                    "TenderType": TenderType.CREDIT_CARD.value,
                }
            )

            # Create inventory effects: decrement stock at node
            for product, qty in basket.items:
                if node_type == "STORE":
                    store_txn.append(
                        {
                            "TraceId": trace_id,
                            "EventTS": event_ts,
                            "StoreID": node_id,
                            "ProductID": product.ID,
                            "QtyDelta": -qty,
                            "Reason": InventoryReason.SALE.value,
                            "Source": "ONLINE",
                        }
                    )
                else:  # DC
                    dc_txn.append(
                        {
                            "TraceId": trace_id,
                            "EventTS": event_ts,
                            "DCID": node_id,
                            "ProductID": product.ID,
                            "QtyDelta": -qty,
                            "Reason": InventoryReason.SALE.value,
                            "Source": "ONLINE",
                        }
                    )

        return orders, store_txn, dc_txn

    def _generate_dc_inventory_transactions(
        self, date: datetime, multiplier: float
    ) -> list[dict]:
        """Generate DC inventory transactions for a day."""
        transactions = []

        # Each DC receives shipments
        for dc in self.distribution_centers:
            dc_transactions = self.inventory_flow_sim.simulate_dc_receiving(dc.ID, date)

            for transaction in dc_transactions:
                transactions.append(
                    {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": self._randomize_time_within_day(date),
                        "DCID": transaction["DCID"],
                        "ProductID": transaction["ProductID"],
                        "QtyDelta": transaction["QtyDelta"],
                        "Reason": transaction["Reason"].value,
                    }
                )

        return transactions

    def _generate_marketing_activity(
        self, date: datetime, multiplier: float
    ) -> list[dict]:
        """Generate marketing impressions and campaign activity."""

        # Defensive check: Verify simulator exists
        if self.marketing_campaign_sim is None:
            logger.error(
                "Marketing simulator not initialized - skipping marketing generation"
            )
            return []

        # DEBUG: Log entry into marketing generation
        logger.info(f"=== _generate_marketing_activity called for {date} ===")
        logger.info(f"  Traffic multiplier: {multiplier}")
        logger.info(f"  Current active campaigns: {len(self._active_campaigns)}")

        marketing_records = []

        # Check if new campaigns should start
        new_campaign_type = self.marketing_campaign_sim.should_start_campaign(
            date, multiplier
        )
        logger.info(f"  should_start_campaign returned: {new_campaign_type}")
        if new_campaign_type:
            campaign_id = self.marketing_campaign_sim.start_campaign(
                new_campaign_type, date
            )
            logger.info(f"  start_campaign returned: {campaign_id}")

            # Validation: Ensure campaign was actually created in simulator
            if campaign_id in self.marketing_campaign_sim._active_campaigns:
                # Store reference to campaign info, not just boolean
                campaign_info = self.marketing_campaign_sim._active_campaigns[
                    campaign_id
                ]
                self._active_campaigns[campaign_id] = campaign_info
                logger.debug(
                    f"Started new {new_campaign_type} campaign: {campaign_id} "
                    f"(end_date: {campaign_info['end_date']})"
                )
                logger.info(
                    f"  Campaign {campaign_id} added to tracking (total: {len(self._active_campaigns)})"
                )
            else:
                logger.error(f"Campaign {campaign_id} failed to create in simulator")
                logger.info(
                    f"  Simulator active campaigns: {list(self.marketing_campaign_sim._active_campaigns.keys())}"
                )
                # Critical failure - don't continue processing this day

        # Debug: Log state before sync
        logger.debug(
            f"Campaign tracking state: fact_gen has {len(self._active_campaigns)} "
            f"campaigns, simulator has {len(self.marketing_campaign_sim._active_campaigns)}"
        )

        # Sync: Remove orphaned campaigns that exist in tracking but not in simulator
        orphaned = set(self._active_campaigns.keys()) - set(
            self.marketing_campaign_sim._active_campaigns.keys()
        )
        if orphaned:
            logger.warning(f"Found {len(orphaned)} orphaned campaigns: {orphaned}")

        for campaign_id in orphaned:
            logger.debug(f"Removing orphaned campaign {campaign_id}")
            del self._active_campaigns[campaign_id]

        logger.debug(
            f"After sync: {len(self._active_campaigns)} campaigns in fact_gen tracking"
        )

        # Generate impressions for active campaigns
        for campaign_id in list(self._active_campaigns.keys()):
            logger.info(f"  Processing campaign {campaign_id}")

            # Check if campaign has reached its end date
            campaign = self.marketing_campaign_sim._active_campaigns.get(campaign_id)

            # CRITICAL: Detect state corruption
            if campaign is None:
                logger.error(
                    f"STATE CORRUPTION: Campaign {campaign_id} tracked in fact_gen "
                    f"but missing from simulator. Removing from fact_gen."
                )
                del self._active_campaigns[campaign_id]
                continue  # Skip this campaign entirely

            logger.info(
                f"    Campaign: {campaign.get('type', 'unknown')}, end_date: {campaign.get('end_date', 'unknown')}"
            )

            if date > campaign["end_date"]:
                # Campaign has completed its scheduled run
                del self._active_campaigns[campaign_id]
                logger.debug(f"Campaign {campaign_id} completed on {date}")
                logger.info(f"    Campaign {campaign_id} DELETED (expired)")
                continue

            logger.info(
                f"    Campaign {campaign_id} is active, generating impressions..."
            )

            impressions = self.marketing_campaign_sim.generate_campaign_impressions(
                campaign_id, date, multiplier
            )

            logger.info(
                f"    generate_campaign_impressions returned {len(impressions)} impressions"
            )

            if not impressions:
                logger.warning(
                    f"    No impressions generated for campaign {campaign_id}"
                )

            # Note: Zero impressions are acceptable - campaign continues if not expired
            for impression in impressions:
                logger.debug(
                    f"      Creating marketing record: {impression.get('channel', 'unknown')}"
                )
                marketing_records.append(
                    {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": self._randomize_time_within_day(date),
                        "Channel": impression["Channel"].value,
                        "CampaignId": impression["CampaignId"],
                        "CreativeId": impression["CreativeId"],
                        "CustomerAdId": impression["CustomerAdId"],
                        "ImpressionId": impression["ImpressionId"],
                        "Cost": str(impression["Cost"]),
                        "Device": impression["Device"].value,
                    }
                )

            logger.info(
                f"    Added {len(impressions)} marketing records from campaign {campaign_id}"
            )

        logger.info(
            f"=== _generate_marketing_activity complete: {len(marketing_records)} total records ==="
        )
        return marketing_records

    def _generate_hourly_store_activity(
        self, date: datetime, base_multiplier: float
    ) -> list[dict[str, list[dict]]]:
        """Generate store activity for each hour of the day."""
        hourly_activity = []

        for hour in range(24):
            hour_datetime = date.replace(hour=hour, minute=0, second=0, microsecond=0)
            hour_multiplier = self.temporal_patterns.get_overall_multiplier(
                hour_datetime
            )

            if hour_multiplier == 0:  # Store closed
                hourly_activity.append(
                    {
                        "receipts": [],
                        "receipt_lines": [],
                        "store_inventory_txn": [],
                        "foot_traffic": [],
                        "ble_pings": [],
                    }
                )
                continue

            hour_data = {
                "receipts": [],
                "receipt_lines": [],
                "store_inventory_txn": [],
                "foot_traffic": [],
                "ble_pings": [],
            }

            # Generate customer transactions for each store
            for store in self.stores:
                store_hour_data = self._generate_store_hour_activity(
                    store, hour_datetime, hour_multiplier
                )

                for fact_type, records in store_hour_data.items():
                    hour_data[fact_type].extend(records)

            hourly_activity.append(hour_data)

        return hourly_activity

    def _generate_store_hour_activity(
        self, store: Store, hour_datetime: datetime, multiplier: float
    ) -> dict[str, list[dict]]:
        """Generate activity for a single store during one hour."""
        hour_data = {
            "receipts": [],
            "receipt_lines": [],
            "store_inventory_txn": [],
            "foot_traffic": [],
            "ble_pings": [],
        }

        # Calculate expected customers for this hour
        base_customers_per_hour = self.config.volume.customers_per_day / 24
        expected_customers = int(base_customers_per_hour * multiplier)

        if expected_customers == 0:
            return hour_data

        # Generate foot traffic (slightly more than actual customers)
        foot_traffic_count = max(1, int(expected_customers * 1.2))
        foot_traffic_records = self._generate_foot_traffic(
            store, hour_datetime, foot_traffic_count
        )
        hour_data["foot_traffic"].extend(foot_traffic_records)

        # Generate customer transactions
        for _ in range(expected_customers):
            customer = self._rng.choice(self.customers)

            # Generate shopping basket
            basket = self.customer_journey_sim.generate_shopping_basket(customer.ID)

            # Create receipt
            receipt_data = self._create_receipt(store, customer, basket, hour_datetime)
            hour_data["receipts"].append(receipt_data["receipt"])
            hour_data["receipt_lines"].extend(receipt_data["lines"])
            hour_data["store_inventory_txn"].extend(
                receipt_data["inventory_transactions"]
            )

            # Generate BLE pings for this customer
            ble_records = self._generate_ble_pings(store, customer, hour_datetime)
            hour_data["ble_pings"].extend(ble_records)

        return hour_data

    def _create_receipt(
        self, store: Store, customer: Customer, basket: Any, transaction_time: datetime
    ) -> dict[str, list[dict]]:
        """Create receipt, receipt lines, and inventory transactions."""
        receipt_id = (
            f"RCP{transaction_time.strftime('%Y%m%d%H%M')}"
            f"{store.ID:03d}{self._rng.randint(1000, 9999)}"
        )
        trace_id = self._generate_trace_id()

        # Calculate receipt totals
        subtotal = basket.estimated_total
        tax_rate = Decimal("0.08")  # 8% tax rate
        tax = subtotal * tax_rate
        total = subtotal + tax

        # Select tender type based on customer segment
        tender_weights = {
            TenderType.CREDIT_CARD: 0.4,
            TenderType.DEBIT_CARD: 0.3,
            TenderType.CASH: 0.2,
            TenderType.MOBILE_PAY: 0.1,
        }

        tender_options = list(tender_weights.keys())
        weights = list(tender_weights.values())
        tender_type = self._rng.choices(tender_options, weights=weights)[0]

        # Create receipt header
        receipt = {
            "TraceId": trace_id,
            "EventTS": transaction_time,
            "StoreID": store.ID,
            "CustomerID": customer.ID,
            "ReceiptId": receipt_id,
            "Subtotal": str(subtotal),
            "Tax": str(tax),
            "Total": str(total),
            "TenderType": tender_type.value,
        }

        # Create receipt lines and inventory transactions
        lines = []
        inventory_transactions = []

        for line_num, (product, qty) in enumerate(basket.items, 1):
            # Apply any promotional pricing
            unit_price = product.SalePrice
            promo_code = None

            # Random promotional discounts (10% chance)
            if self._rng.random() < 0.1:
                discount = self._rng.uniform(0.05, 0.25)
                unit_price = unit_price * (1 - Decimal(str(discount)))
                promo_code = f"PROMO{self._rng.randint(100, 999)}"

            ext_price = unit_price * qty

            line = {
                "TraceId": trace_id,
                "EventTS": transaction_time,
                "ReceiptId": receipt_id,
                "Line": line_num,
                "ProductID": product.ID,
                "Qty": qty,
                "UnitPrice": str(unit_price),
                "ExtPrice": str(ext_price),
                "PromoCode": promo_code,
            }
            lines.append(line)

            # Create inventory transaction (sale)
            inventory_transaction = {
                "TraceId": trace_id,
                "EventTS": transaction_time,
                "StoreID": store.ID,
                "ProductID": product.ID,
                "QtyDelta": -qty,  # Negative for sale
                "Reason": InventoryReason.SALE.value,
                "Source": "CUSTOMER_PURCHASE",
            }
            inventory_transactions.append(inventory_transaction)

        return {
            "receipt": receipt,
            "lines": lines,
            "inventory_transactions": inventory_transactions,
        }

    def _generate_foot_traffic(
        self, store: Store, hour_datetime: datetime, traffic_count: int
    ) -> list[dict]:
        """Generate foot traffic sensor records."""
        traffic_records = []

        # Store zones where sensors are placed
        zones = ["ENTRANCE", "AISLES_A", "AISLES_B", "CHECKOUT", "EXIT"]

        for _ in range(traffic_count):
            # Simulate customer path through store
            zone = self._rng.choice(zones)
            sensor_id = f"SENSOR_{store.ID:03d}_{zone}"

            # Dwell time based on zone
            dwell_times = {
                "ENTRANCE": (30, 120),  # 30 seconds to 2 minutes
                "AISLES_A": (120, 600),  # 2 to 10 minutes
                "AISLES_B": (120, 600),  # 2 to 10 minutes
                "CHECKOUT": (60, 300),  # 1 to 5 minutes
                "EXIT": (15, 60),  # 15 seconds to 1 minute
            }

            min_dwell, max_dwell = dwell_times[zone]
            dwell_time = self._rng.randint(min_dwell, max_dwell)

            traffic_record = {
                "TraceId": self._generate_trace_id(),
                "EventTS": self._randomize_time_within_hour(hour_datetime),
                "StoreID": store.ID,
                "SensorId": sensor_id,
                "Zone": zone,
                "Dwell": dwell_time,
                "Count": 1,  # Single person detection
            }
            traffic_records.append(traffic_record)

        return traffic_records

    def _generate_ble_pings(
        self, store: Store, customer: Customer, transaction_time: datetime
    ) -> list[dict]:
        """Generate BLE beacon pings for a customer visit."""
        ble_records = []

        # Simulate customer journey through store with BLE pings
        zones = ["ENTRANCE", "ELECTRONICS", "GROCERY", "CLOTHING", "CHECKOUT"]
        beacons = [f"BEACON_{store.ID:03d}_{zone}" for zone in zones]

        # Customer visits 2-4 zones during their journey
        visited_zones = self._rng.sample(
            list(zip(zones, beacons)), self._rng.randint(2, 4)
        )

        for zone, beacon_id in visited_zones:
            # Multiple pings per zone (2-5 pings)
            ping_count = self._rng.randint(2, 5)

            for _ in range(ping_count):
                # RSSI varies by distance/proximity
                rssi = self._rng.randint(-80, -30)  # dBm

                ping_time = transaction_time + timedelta(
                    minutes=self._rng.randint(
                        -15, 15
                    )  # Within 15 minutes of transaction
                )

                ble_record = {
                    "TraceId": self._generate_trace_id(),
                    "EventTS": ping_time,
                    "StoreID": store.ID,
                    "BeaconId": beacon_id,
                    "CustomerBLEId": customer.BLEId,
                    "RSSI": rssi,
                    "Zone": zone,
                }
                ble_records.append(ble_record)

        return ble_records

    def _generate_truck_movements(
        self, date: datetime, store_transactions: list[dict]
    ) -> list[dict]:
        """Generate truck movements based on store inventory needs."""
        truck_movements = []

        # Analyze store inventory needs
        store_demands = {}
        for transaction in store_transactions:
            if transaction["QtyDelta"] < 0:  # Sales
                store_id = transaction["StoreID"]
                if store_id not in store_demands:
                    store_demands[store_id] = 0
                store_demands[store_id] += abs(transaction["QtyDelta"])

        # Generate truck shipments for stores with high demand
        for store_id, demand in store_demands.items():
            if demand > 100:  # Threshold for triggering shipment
                next(s for s in self.stores if s.ID == store_id)

                # Find nearest DC (simplified - use first DC)
                dc = self.distribution_centers[0]

                # Check reorder needs
                reorder_list = self.inventory_flow_sim.check_reorder_needs(store_id)

                if reorder_list:
                    # Generate truck shipment
                    departure_time = date.replace(hour=6, minute=0)  # 6 AM departure
                    shipment_info = self.inventory_flow_sim.generate_truck_shipment(
                        dc.ID, store_id, reorder_list, departure_time
                    )

                    truck_record = {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": departure_time,
                        "TruckId": shipment_info["truck_id"],
                        "DCID": shipment_info["dc_id"],
                        "StoreID": shipment_info["store_id"],
                        "ShipmentId": shipment_info["shipment_id"],
                        "Status": shipment_info["status"].value,
                        "ETA": shipment_info["eta"],
                        "ETD": shipment_info["etd"],
                    }
                    truck_movements.append(truck_record)

                    # Track shipment for future delivery processing
                    self._active_shipments[shipment_info["shipment_id"]] = shipment_info

        return truck_movements

    def _process_truck_deliveries(
        self, date: datetime, truck_moves: list[dict]
    ) -> list[dict]:
        """Process truck deliveries and generate inventory transactions."""
        delivery_transactions = []

        # Check for shipments completing delivery
        completed_shipments = []
        for shipment_id, shipment_info in self._active_shipments.items():
            if shipment_info["etd"].date() <= date.date():
                # Complete delivery
                transactions = self.inventory_flow_sim.complete_delivery(shipment_id)

                for transaction in transactions:
                    delivery_transactions.append(
                        {
                            "TraceId": self._generate_trace_id(),
                            "EventTS": transaction["EventTS"],
                            "StoreID": transaction["StoreID"],
                            "ProductID": transaction["ProductID"],
                            "QtyDelta": transaction["QtyDelta"],
                            "Reason": transaction["Reason"].value,
                            "Source": transaction["Source"],
                        }
                    )

                completed_shipments.append(shipment_id)

        # Remove completed shipments
        for shipment_id in completed_shipments:
            del self._active_shipments[shipment_id]

        return delivery_transactions

    def _get_available_products_for_date(self, date: datetime) -> list[ProductMaster]:
        """Get products that have been launched by the given date."""
        return [
            product
            for product in self.products
            if product.LaunchDate.date() <= date.date()
        ]

    def _export_daily_facts(
        self, date: datetime, daily_facts: dict[str, list[dict]]
    ) -> int:
        """Export daily facts to partitioned CSV files."""
        facts_path = Path(self.config.paths.facts)
        date_partition = f"dt={date.strftime('%Y-%m-%d')}"
        partitions_created = 0

        for fact_table, records in daily_facts.items():
            if not records:
                continue

            # Create partition directory
            partition_path = facts_path / fact_table / date_partition
            partition_path.mkdir(parents=True, exist_ok=True)

            # Export to CSV file
            file_path = partition_path / f"{fact_table}_{date.strftime('%Y%m%d')}.csv"
            DataFrameExporter.export_to_csv(records, file_path)

            partitions_created += 1

        return partitions_created

    def _generate_and_export_day(
        self, date: datetime, day_num: int, total_days: int, report_progress: bool = False
    ) -> tuple[dict[str, int], int]:
        """
        Generate and export data for a single day (used by parallel processing).

        This method is called by parallel threads. Thread-safe as each day
        writes to separate partition directories.

        Args:
            date: Date to generate data for
            day_num: Day number (for logging)
            total_days: Total number of days (for logging)
            report_progress: Whether to send progress updates (default False for backward compatibility)

        Returns:
            Tuple of (daily_counts dict, partition_count)
        """
        # Generate daily facts
        daily_facts = self._generate_daily_facts(date)

        # Export to CSV
        partition_count = self._export_daily_facts(date, daily_facts)

        # Count records
        daily_counts = {fact_type: len(records) for fact_type, records in daily_facts.items()}

        # Report progress if enabled
        if report_progress:
            self._send_progress_from_worker(
                day_num=day_num,
                total_days=total_days,
                date=date,
                daily_counts=daily_counts
            )

        return daily_counts, partition_count

    def _send_progress_from_worker(
        self,
        day_num: int,
        total_days: int,
        date: datetime,
        daily_counts: dict[str, int]
    ) -> None:
        """
        Thread-safe progress update from parallel worker.

        Called by worker threads to report progress. Uses locking to ensure
        thread safety when updating shared state.

        Args:
            day_num: Current day number
            total_days: Total number of days being generated
            date: Date being processed
            daily_counts: Record counts per fact table for this day
        """
        # Calculate table progress based on day completion
        progress = day_num / total_days if total_days > 0 else 0.0

        # Create table progress dict (all tables progress together in parallel mode)
        table_progress = {table: progress for table in self.FACT_TABLES}

        # Thread-safe: all state updates within lock, then release before calling progress update
        with self._progress_lock:
            # Update table states based on progress
            self._update_table_states(table_progress)

            # Get current table states for reporting (copy so we can release lock)
            tables_completed = [
                table for table, state in self._table_states.items()
                if state == "completed"
            ]
            tables_in_progress = [
                table for table, state in self._table_states.items()
                if state == "in_progress"
            ]
            tables_remaining = [
                table for table, state in self._table_states.items()
                if state == "not_started"
            ]

            tables_completed_count = len(tables_completed)

        # Emit per-table progress (master-style) outside the lock
        for table, prog in table_progress.items():
            self._emit_table_progress(
                table,
                prog,
                f"Generating {table.replace('_',' ')}",
                None,
            )

        # Create progress message OUTSIDE the lock
        message = (
            f"Generated data for {date.strftime('%Y-%m-%d')} "
            f"(day {day_num}/{total_days}, "
            f"{tables_completed_count}/{len(self.FACT_TABLES)} tables complete)"
        )

        # Send throttled update OUTSIDE the lock (it will acquire its own lock)
        self._send_throttled_progress_update(
            day_counter=day_num,
            message=message,
            total_days=total_days,
            table_progress=table_progress,
            tables_completed=tables_completed,
            tables_in_progress=tables_in_progress,
            tables_remaining=tables_remaining
        )

    def _send_throttled_progress_update(
        self,
        day_counter: int,
        message: str,
        total_days: int,
        table_progress: dict[str, float] | None = None,
        tables_completed: list[str] | None = None,
        tables_in_progress: list[str] | None = None,
        tables_remaining: list[str] | None = None,
        tables_failed: list[str] | None = None,
    ) -> None:
        """
        Send progress update to callback with throttling and ETA calculation.

        Updates are throttled to minimum 100ms intervals to ensure they're
        visible to users and don't overwhelm the API.

        Args:
            day_counter: Current day number
            message: Progress message
            total_days: Total number of days
            table_progress: Per-table progress percentages
            tables_completed: List of completed tables
            tables_in_progress: List of in-progress tables
            tables_remaining: List of not-started tables
            tables_failed: List of failed tables
        """
        if not self._progress_callback:
            logger.warning(f"Progress callback is None! Cannot send update: {message}")
            return

        logger.info(f"[PROGRESS] Callback exists, sending update: {message[:50]}")
        with self._progress_lock:
            current_time = time.time()
            progress = day_counter / total_days if total_days > 0 else 0.0

            # Throttle: Skip update if too soon (less than 50ms since last update)
            time_since_last = current_time - self._last_progress_update_time
            if time_since_last < 0.05:
                logger.debug(
                    f"Throttling progress update (too soon: {time_since_last*1000:.1f}ms < 50ms)"
                )
                return

            # Update progress history for ETA calculation
            self._progress_history.append((current_time, progress))
            if len(self._progress_history) > 10:
                self._progress_history.pop(0)

            # Calculate ETA
            eta = self._calculate_eta(progress)

            # Calculate progress rate (for informational purposes)
            progress_rate = None
            if eta is not None and (1.0 - progress) > 0:
                progress_rate = (1.0 - progress) / eta

            # Determine current table (first in_progress table, if any)
            current_table = None
            if tables_in_progress and len(tables_in_progress) > 0:
                current_table = tables_in_progress[0]

            callback_kwargs = {
                "table_progress": table_progress.copy() if table_progress else None,
                "current_table": current_table,
                "tables_completed": (tables_completed or []).copy(),
                "tables_failed": (tables_failed or []).copy(),
                "tables_in_progress": (tables_in_progress or []).copy() if tables_in_progress is not None else [],
                "tables_remaining": (tables_remaining or []).copy() if tables_remaining is not None else [],
                "estimated_seconds_remaining": eta,
                "progress_rate": progress_rate,
                "table_counts": None,
            }

            filtered_kwargs = self._filter_progress_kwargs(callback_kwargs)

            # Send the progress update
            try:
                self._progress_callback(day_counter, message, **filtered_kwargs)
                logger.debug(
                    f"Progress update sent: {progress:.2%} (day {day_counter}/{total_days}) "
                    f"ETA: {eta:.0f}s, tables_in_progress: {tables_in_progress}" if eta else f"at {current_time:.3f}"
                )
            except TypeError:
                # Fallback for old callbacks that only accept 2 parameters
                try:
                    self._progress_callback(day_counter, message)
                    logger.debug(
                        f"Progress update sent (legacy): {progress:.2%} at {current_time:.3f}"
                    )
                except TypeError:
                    logger.debug(
                        "Legacy progress callback invocation failed; "
                        "suppressing TypeError to preserve generation flow"
                    )

            # Update last update timestamp
            self._last_progress_update_time = current_time

    def _filter_progress_kwargs(self, candidate_kwargs: dict[str, Any]) -> dict[str, Any]:
        """Return only the keyword arguments supported by the progress callback."""
        callback = self._progress_callback
        if not callback:
            return {}

        # Drop fields that have no value so legacy callbacks don't see noisy kwargs
        cleaned_kwargs = {key: value for key, value in candidate_kwargs.items() if value is not None}
        if not cleaned_kwargs:
            return {}

        try:
            signature = inspect.signature(callback)
        except (TypeError, ValueError):
            # If the signature can't be inspected, assume callback can handle everything we pass now
            return cleaned_kwargs

        if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in signature.parameters.values()):
            return cleaned_kwargs

        accepted_names: set[str] = set()
        for name, param in signature.parameters.items():
            if param.kind in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ):
                accepted_names.add(name)

        # Remove the first positional parameters since we pass them positionally (day, message)
        positional_count = 0
        for name, param in signature.parameters.items():
            if param.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                if positional_count < 2:
                    accepted_names.discard(name)
                    positional_count += 1
                continue
            break

        if not accepted_names:
            return {}

        return {key: value for key, value in cleaned_kwargs.items() if key in accepted_names}

    def _generate_trace_id(self) -> str:
        """Generate unique trace ID."""
        trace_id = f"TRC{self._trace_counter:010d}"
        self._trace_counter += 1
        return trace_id

    def _randomize_time_within_day(self, date: datetime) -> datetime:
        """Generate random time within the given day."""
        hour = self._rng.randint(0, 23)
        minute = self._rng.randint(0, 59)
        second = self._rng.randint(0, 59)
        return date.replace(hour=hour, minute=minute, second=second)

    def _randomize_time_within_hour(self, hour_datetime: datetime) -> datetime:
        """Generate random time within the given hour."""
        minute = self._rng.randint(0, 59)
        second = self._rng.randint(0, 59)
        return hour_datetime.replace(minute=minute, second=second)

    def get_generation_summary(self, summary: FactGenerationSummary) -> dict[str, Any]:
        """Get detailed summary of fact generation."""
        return {
            "date_range": {
                "start": summary.date_range[0].isoformat(),
                "end": summary.date_range[1].isoformat(),
                "days": (summary.date_range[1] - summary.date_range[0]).days + 1,
            },
            "facts_generated": summary.facts_generated,
            "total_records": summary.total_records,
            "partitions_created": summary.partitions_created,
            "generation_time_seconds": summary.generation_time_seconds,
            "records_per_second": (
                summary.total_records / summary.generation_time_seconds
                if summary.generation_time_seconds > 0
                else 0
            ),
            "validation_results": summary.validation_results,
            "config": {
                "seed": self.config.seed,
                "stores": len(self.stores),
                "customers": len(self.customers),
                "products": len(self.products),
                "customers_per_day": self.config.volume.customers_per_day,
                "items_per_ticket_mean": self.config.volume.items_per_ticket_mean,
            },
        }

    def _cache_fact_counts(self, facts_generated: dict[str, int]) -> None:
        """
        Cache fact table counts for dashboard performance.

        Reads actual counts from disk to ensure accuracy.

        Args:
            facts_generated: Dictionary of table names that were just generated
        """
        try:
            from pathlib import Path

            cache_manager = CacheManager()
            facts_path = Path(self.config.paths.facts)

            # For each fact table, count actual records from all partitions
            for table_name in facts_generated.keys():
                table_path = facts_path / table_name
                if not table_path.exists():
                    continue

                total_count = 0
                # Count records across all date partitions
                for partition_dir in table_path.iterdir():
                    if partition_dir.is_dir() and partition_dir.name.startswith("dt="):
                        date_part = partition_dir.name[3:]  # Remove "dt=" prefix
                        date_suffix = date_part.replace("-", "")
                        csv_file = partition_dir / f"{table_name}_{date_suffix}.csv"

                        if csv_file.exists():
                            with open(csv_file, encoding="utf-8") as f:
                                # Subtract 1 for header row
                                count = sum(1 for _ in f) - 1
                                total_count += count

                # Cache the actual total count
                cache_manager.update_fact_table(
                    table_name, total_count, "Historical Data"
                )
                logger.info(f"Cached {table_name}: {total_count} total records")

            logger.info("Fact data counts cached successfully")
        except Exception as e:
            logger.error(f"Failed to cache counts: {e}")


# Convenience function for direct usage
def generate_historical_facts(
    config_path: str, start_date: datetime, end_date: datetime
) -> FactDataGenerator:
    """
    Convenience function to generate historical fact data from config file.

    Args:
        config_path: Path to configuration JSON file
        start_date: Start date for historical data
        end_date: End date for historical data

    Returns:
        FactDataGenerator instance with generated data
    """
    from retail_datagen.config.models import RetailConfig

    config = RetailConfig.from_file(config_path)
    generator = FactDataGenerator(config)
    summary = generator.generate_historical_data(start_date, end_date)

    print(
        f"Generated {summary.total_records} fact records across {summary.partitions_created} partitions"
    )
    return generator
