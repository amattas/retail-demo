"""
Inventory management for distribution centers and stores
"""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import numpy as np

from retail_datagen.generators.utils import ProgressReporter
from retail_datagen.shared.models import (
    InventoryReason,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from .models import FactGenerationSummary

# SessionMaker import for SQLite fallback path (deprecated, DuckDB-only runtime)
try:
    from retail_datagen.db.session import retail_session_maker

    SessionMaker = retail_session_maker()
except ImportError:
    SessionMaker = None  # type: ignore[assignment, misc]

logger = logging.getLogger(__name__)


class InventoryMixin:
    """Inventory management for distribution centers and stores"""

    def _generate_dc_inventory_transactions(
        self, date: datetime, multiplier: float
    ) -> list[dict]:
        """Generate DC inventory transactions for a day."""
        transactions = []

        # Each DC receives shipments
        for dc in self.distribution_centers:
            dc_transactions = self.inventory_flow_sim.simulate_dc_receiving(dc.ID, date)

            for transaction in dc_transactions:
                # Get current balance after this transaction
                balance = self.inventory_flow_sim.get_dc_balance(
                    transaction["DCID"], transaction["ProductID"]
                )

                transactions.append(
                    {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": self._randomize_time_within_day(date),
                        "DCID": transaction["DCID"],
                        "ProductID": transaction["ProductID"],
                        "QtyDelta": transaction["QtyDelta"],
                        "Reason": transaction["Reason"].value,
                        "Balance": balance,
                    }
                )

        return transactions

    def _build_store_customer_pools(self, customer_geographies: dict) -> None:
        """
        Build customer pools for each store for efficient weighted selection.

        For each store, create a list of (customer, weight) tuples where weight
        represents the probability of that customer shopping at that store.

        Args:
            customer_geographies: Dictionary mapping customer_id to CustomerGeography
        """
        # Initialize pools for all stores
        for store in self.stores:
            self._store_customer_pools[store.ID] = []

        # Build pools by calculating weights for each customer
        store_ids = [store.ID for store in self.stores]

        for customer in self.customers:
            customer_geo = customer_geographies.get(customer.ID)
            if not customer_geo:
                # If no geography, give equal weight to all stores
                equal_weight = 1.0 / len(self.stores)
                for store in self.stores:
                    self._store_customer_pools[store.ID].append(
                        (customer, equal_weight)
                    )
                continue

            # Get store selection weights for this customer
            store_weights = customer_geo.get_store_selection_weights(store_ids)

            # Add customer to each store's pool with appropriate weight
            for store_id, weight in store_weights.items():
                if weight > 0:  # Only add if there's a non-zero probability
                    self._store_customer_pools[store_id].append((customer, weight))

        # Build precomputed sampling arrays (customers list + weights) per store
        for store_id, pool in self._store_customer_pools.items():
            if not pool:
                # Fallback to global customers uniformly if pool is empty
                self._store_customer_sampling[store_id] = (
                    self.customers[:],
                    [1.0] * len(self.customers),
                )
                continue

            customers_list = [c for c, _ in pool]
            weights_list = [w for _, w in pool]
            total_w = sum(weights_list)
            if total_w <= 0:
                # Equal weights if all zero
                weights_list = [1.0] * len(customers_list)
            else:
                # Normalize once to avoid hot-loop normalization
                weights_list = [w / total_w for w in weights_list]

            self._store_customer_sampling[store_id] = (customers_list, weights_list)
        # Also cache NumPy-ready arrays for fast vector sampling
        self._store_customer_sampling_np: dict[int, tuple[np.ndarray, np.ndarray]] = {}
        for sid, (clist, wlist) in self._store_customer_sampling.items():
            try:
                idx = np.arange(len(clist), dtype=np.int32)
                p = np.asarray(wlist, dtype=np.float64)
                # Ensure probabilities sum to 1
                s = p.sum()
                if s > 0:
                    p = p / s
                else:
                    p.fill(1.0 / len(p))
                self._store_customer_sampling_np[sid] = (idx, p)
            except Exception as e:
                # Fallback will use Python choices
                logger.debug(
                    f"Failed to precompute numpy sampling for store {sid}: {e}"
                )

        # Log summary statistics
        pool_sizes = [len(pool) for pool in self._store_customer_pools.values()]
        avg_pool_size = sum(pool_sizes) / len(pool_sizes) if pool_sizes else 0
        min_pool_size = min(pool_sizes) if pool_sizes else 0
        max_pool_size = max(pool_sizes) if pool_sizes else 0

        logger.info(f"Built customer pools for {len(self.stores)} stores")
        logger.info(f"  Average pool size: {avg_pool_size:.0f} customers per store")
        logger.info(f"  Min pool size: {min_pool_size}")
        logger.info(f"  Max pool size: {max_pool_size}")

    async def generate_historical_data(
        self,
        start_date: datetime,
        end_date: datetime,
        *,
        publish_to_outbox: bool = False,
    ) -> FactGenerationSummary:
        """
        Generate historical fact data for the specified date range.

        Writes data directly to DuckDB.

        Args:
            start_date: Start of historical data generation
            end_date: End of historical data generation

        Returns:
            Summary of generation results

        Note:
            This method is async to support database operations. Call with:
            ```python
            summary = await generator.generate_historical_data(start, end)
            ```
        """
        generation_start_time = datetime.now(UTC)
        # Remember outbox preference for this run so helpers
        # (e.g., _insert_hourly_to_db) can mirror to streaming_outbox
        self._publish_to_outbox = bool(publish_to_outbox)
        print(
            f"Starting historical fact data generation from {start_date} to {end_date}"
        )

        # Reset table states for new generation run
        self._reset_table_states()

        # Reset hourly progress tracker for new generation run
        self.hourly_tracker.reset()

        # Ensure master data is loaded (DuckDB only)
        if not self.stores:
            self.load_master_data_from_duckdb()

        # Pre-check master readiness
        errors: list[str] = []
        if not self.stores:
            errors.append("No stores found in master database")
        if not self.customers:
            errors.append("No customers found in master database")
        if not self.products:
            errors.append("No products found in master database")
        if errors:
            raise ValueError("; ".join(errors))

        # Determine active tables
        active_tables = self._active_fact_tables()

        # Mark all tables as started
        for table in active_tables:
            self._progress_tracker.mark_table_started(table)

        # Initialize tracking for active tables only
        facts_generated = {t: 0 for t in active_tables}
        # Track records actually written to DB for live tile counts
        self._table_insert_counts: dict[str, int] = {t: 0 for t in active_tables}
        # Track DB totals to verify deltas (DuckDB path computes from summaries)
        self._fact_db_counts: dict[str, int] = {}

        # NEW: Add table progress tracking
        table_progress = {table: 0.0 for table in active_tables}

        total_days = (end_date - start_date).days + 1

        # Emit an early progress heartbeat so UIs show activity immediately
        try:
            self._send_throttled_progress_update(
                day_counter=0,
                message="Preparing historical data generation",
                total_days=total_days,
                table_progress=table_progress,
                tables_completed=[],
                tables_in_progress=active_tables,
                tables_remaining=[],
            )
        except Exception as e:
            logger.warning(f"Failed to send initial progress update: {e}")

        # Calculate expected records per table for accurate progress tracking
        # NOTE: customers_per_day is configured PER STORE, not total
        # Total daily customers = customers_per_day * number of stores
        customers_per_store_per_day = self.config.volume.customers_per_day
        total_customers_per_day = customers_per_store_per_day * len(self.stores)
        expected_records_all = {
            "receipts": total_days * total_customers_per_day,
            "receipt_lines": total_days * total_customers_per_day * 3,
            "foot_traffic": total_days * len(self.stores) * 100,
            "ble_pings": total_days * len(self.stores) * 500,
            "dc_inventory_txn": total_days * len(self.distribution_centers) * 50,
            "truck_moves": total_days * 10,
            "truck_inventory": total_days * 20,
            "store_inventory_txn": total_days * len(self.stores) * 20,
            "marketing": total_days * 10,
            "supply_chain_disruption": total_days * 2,
            "online_orders": total_days
            * max(0, int(self.config.volume.online_orders_per_day)),
        }
        expected_records = {
            k: v for k, v in expected_records_all.items() if k in active_tables
        }

        progress_reporter = ProgressReporter(total_days, "Generating historical data")

        # Generate data day by day
        current_date = start_date
        day_counter = 0

        # DuckDB-only runtime; no SQLAlchemy session

        async def _ensure_required_schema(session: AsyncSession) -> None:
            try:
                from sqlalchemy import text

                # Check if column exists
                res = await session.execute(text("PRAGMA table_info('fact_receipts')"))
                cols = [row[1] for row in res.fetchall()]
                # receipt_id_ext
                if "receipt_id_ext" not in cols:
                    await session.execute(
                        text("ALTER TABLE fact_receipts ADD COLUMN receipt_id_ext TEXT")
                    )
                    await session.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS ix_fact_receipts_ext ON fact_receipts (receipt_id_ext)"
                        )
                    )
                    logger.info(
                        "Migrated fact_receipts: added receipt_id_ext column and index"
                    )

                # receipt_type
                if "receipt_type" not in cols:
                    await session.execute(
                        text(
                            "ALTER TABLE fact_receipts ADD COLUMN receipt_type TEXT NOT NULL DEFAULT 'SALE'"
                        )
                    )
                    await session.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS ix_fact_receipts_type ON fact_receipts (receipt_type)"
                        )
                    )
                    logger.info(
                        "Migrated fact_receipts: added receipt_type column and index"
                    )

                # return_for_receipt_id
                if "return_for_receipt_id" not in cols:
                    await session.execute(
                        text(
                            "ALTER TABLE fact_receipts ADD COLUMN return_for_receipt_id INTEGER"
                        )
                    )
                    await session.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS ix_fact_receipts_return_for ON fact_receipts (return_for_receipt_id)"
                        )
                    )
                    logger.info(
                        "Migrated fact_receipts: added return_for_receipt_id column and index"
                    )

                # Ensure online order lines table exists
                await session.execute(
                    text(
                        "CREATE TABLE IF NOT EXISTS fact_online_order_lines (\n"
                        " line_id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                        " order_id INTEGER NOT NULL,\n"
                        " product_id INTEGER NOT NULL,\n"
                        " line_num INTEGER NOT NULL,\n"
                        " quantity INTEGER NOT NULL,\n"
                        " unit_price FLOAT NOT NULL,\n"
                        " ext_price FLOAT NOT NULL,\n"
                        " promo_code VARCHAR(50) NULL\n"
                        ")"
                    )
                )
                await session.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS ix_online_order_lines_order ON fact_online_order_lines (order_id)"
                    )
                )
                await session.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS ix_online_order_lines_order_product ON fact_online_order_lines (order_id, product_id)"
                    )
                )

                # Ensure dim_products has tags column
                res_prod = await session.execute(
                    text("PRAGMA table_info('dim_products')")
                )
                prod_cols = [row[1] for row in res_prod.fetchall()]
                if "tags" not in prod_cols:
                    await session.execute(
                        text("ALTER TABLE dim_products ADD COLUMN tags TEXT")
                    )
                    logger.info("Migrated dim_products: added tags column")

                await session.commit()
            except Exception as e:
                logger.warning(f"Schema ensure failed (non-fatal): {e}")

        async def _run_with_session():
            nonlocal day_counter, current_date
            # Ensure schema is compatible (adds new columns/tables if missing)
            if not self._use_duckdb:
                await _ensure_required_schema(self._session)
            # Drop nonessential indexes for faster bulk loads (SQLite only; skipped in DuckDB)
            dropped_indexes: list[tuple[str, str]] = []
            try:
                if not self._use_duckdb:
                    # Notify UI about pre-load DB optimization
                    self._send_throttled_progress_update(
                        0,
                        "Optimizing database for bulk load (dropping indexes)",
                        total_days,
                        table_progress=table_progress,
                    )
                    dropped_indexes = await self._capture_and_drop_indexes(
                        self._session, active_tables
                    )
                    self._send_throttled_progress_update(
                        0,
                        "Bulk load optimizations applied",
                        total_days,
                        table_progress=table_progress,
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to drop indexes for bulk load optimization: {e}"
                )
                dropped_indexes = []
            while current_date <= end_date:
                day_counter += 1

                # Generate daily facts (progress updates now happen during actual generation)
                daily_facts = await self._generate_daily_facts(
                    current_date, active_tables, day_counter, total_days
                )

                # Update counters
                for fact_type, records in daily_facts.items():
                    facts_generated[fact_type] += len(records)

                # Update per-table progress based on actual records generated
                for fact_type in facts_generated.keys():
                    current_count = facts_generated[fact_type]
                    expected = expected_records.get(fact_type, 1)
                    # Calculate actual progress (0.0 to 1.0), never exceed 1.0
                    table_progress[fact_type] = (
                        min(1.0, current_count / expected) if expected > 0 else 0.0
                    )

                # Emit per-table progress (master-style)
                for fact_type, prog in table_progress.items():
                    self._emit_table_progress(
                        fact_type,
                        prog,
                        f"Generating {fact_type.replace('_', ' ')}",
                        None,
                    )

                # Update progress tracker (progress only, not states)
                for table_name, progress in table_progress.items():
                    self._progress_tracker.update_progress(table_name, progress)

                # Get table lists from progress tracker
                tables_completed = self._progress_tracker.get_tables_by_state(
                    "completed"
                )
                tables_in_progress = self._progress_tracker.get_tables_by_state(
                    "in_progress"
                )
                tables_remaining = self._progress_tracker.get_tables_by_state(
                    "not_started"
                )

                # Calculate tables completed count
                tables_completed_count = len(tables_completed)

                # Enhanced message with table completion count
                enhanced_message = (
                    f"Generating data for {current_date.strftime('%Y-%m-%d')} "
                    f"(day {day_counter}/{total_days}) "
                    f"({tables_completed_count}/{len(active_tables)} tables complete)"
                )

                # Update API progress with throttling, include cumulative counts
                self._send_throttled_progress_update(
                    day_counter,
                    enhanced_message,
                    total_days,
                    table_progress=table_progress,
                    tables_completed=tables_completed,
                    tables_in_progress=tables_in_progress,
                    tables_remaining=tables_remaining,
                    # For UI tiles prefer DB-written counts if available, otherwise generation counts
                    table_counts=(
                        self._table_insert_counts.copy()
                        if getattr(self, "_table_insert_counts", None)
                        else facts_generated.copy()
                    ),
                )

                progress_reporter.update(1)
                current_date += timedelta(days=1)

            # Recreate any dropped indexes after generation completes for this run
            try:
                if (not self._use_duckdb) and dropped_indexes:
                    await self._recreate_indexes(self._session, dropped_indexes)
            except Exception as e:
                logger.warning(f"Failed to recreate indexes after generation: {e}")

        if self._use_duckdb:
            # No async DB session needed for DuckDB path
            await _run_with_session()
        elif self._session is None:
            async with SessionMaker() as session:
                self._session = session
                await _run_with_session()
                self._session = None
        else:
            await _run_with_session()

        progress_reporter.complete()

        # Mark generation complete (transitions all tables to 'completed')
        self._progress_tracker.mark_generation_complete()

        # Final validation
        validation_results = self.business_rules.get_validation_summary()

        generation_end_time = datetime.now(UTC)
        generation_time = (generation_end_time - generation_start_time).total_seconds()

        total_records = sum(facts_generated.values())

        summary = FactGenerationSummary(
            date_range=(start_date, end_date),
            facts_generated=facts_generated,
            total_records=total_records,
            validation_results=validation_results,
            generation_time_seconds=generation_time,
            partitions_created=0,  # No longer applicable in database-only mode
        )

        print(
            f"Historical data generation complete: {total_records} records "
            f"in {generation_time:.1f}s"
        )
        print(f"Generated {len(facts_generated)} fact tables")

        # Cache the counts for dashboard performance
        self._cache_fact_counts(facts_generated)

        # Update watermarks if database session provided (SQLite-only path)
        if self._session and not self._use_duckdb:
            await self._update_watermarks_after_generation(
                start_date, end_date, active_tables
            )
            logger.info("Updated watermarks for all generated fact tables")

        return summary

    async def _generate_daily_facts(
        self, date: datetime, active_tables: list[str], day_index: int, total_days: int
    ) -> dict[str, list[dict]]:
        """
        Generate all fact data for a single day.

        Hourly progress updates are sent after each hour's data is exported, with
        thread-safe locking to prevent race conditions.

        Args:
            date: Date to generate facts for
            active_tables: List of fact tables to generate
            day_index: Current day number (1-indexed)
            total_days: Total number of days being generated

        Returns:
            Dictionary of fact tables with their records

        Note:
            Now async to support database operations during hourly exports.
        """
        daily_facts = {t: [] for t in active_tables}

        # Update available products for this date
        available_products = self._get_available_products_for_date(date)
        if self.customer_journey_sim:
            self.customer_journey_sim.update_available_products(available_products)

        # Generate base activity level for the day
        base_multiplier = self.temporal_patterns.get_overall_multiplier(date)

        # 1. Generate DC inventory transactions (supplier deliveries)
        if "dc_inventory_txn" in active_tables:
            dc_transactions = (
                self._generate_dc_inventory_txn(date, base_multiplier)
                if hasattr(self, "_generate_dc_inventory_txn")
                else self._generate_dc_inventory_transactions(date, base_multiplier)
            )
            daily_facts["dc_inventory_txn"].extend(dc_transactions)
            # Insert daily DC transactions immediately (not hourly)
            if dc_transactions:
                try:
                    await self._insert_hourly_to_db(
                        self._session,
                        "dc_inventory_txn",
                        dc_transactions,
                        hour=0,
                        commit_every_batches=0,
                    )
                    # Update progress for this daily-generated table (track all 24 hours as complete)
                    for hour in range(24):
                        self.hourly_tracker.update_hourly_progress(
                            "dc_inventory_txn", day_index, hour, total_days
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to insert dc_inventory_txn for {date.strftime('%Y-%m-%d')}: {e}"
                    )

        # 2. Generate marketing campaigns and impressions
        # Digital marketing runs 24/7 independently of store traffic/hours
        # Use constant multiplier of 1.0 for consistent digital ad delivery
        if "marketing" in active_tables:
            marketing_boost = 1.0
            try:
                marketing_boost = self._compute_marketing_multiplier(date)
            except Exception as e:
                logger.warning(
                    f"Failed to compute marketing multiplier for {date}, using default 1.0: {e}"
                )
            marketing_records = self._generate_marketing_activity(date, marketing_boost)
            if marketing_records:
                logger.debug(
                    f"Generated {len(marketing_records)} marketing records for {date.strftime('%Y-%m-%d')}"
                )
            daily_facts["marketing"].extend(marketing_records)

            # NEW: Update marketing progress (treated as completing all 24 hours at once)
            for hour in range(24):
                self.hourly_tracker.update_hourly_progress(
                    table="marketing", day=day_index, hour=hour, total_days=total_days
                )

            # NEW: Insert marketing records for the day directly (not hourly)
            if marketing_records:
                try:
                    await self._insert_hourly_to_db(
                        self._session,
                        "marketing",
                        marketing_records,
                        hour=0,
                        commit_every_batches=0,
                    )
                    # Update progress for this daily-generated table (track all 24 hours as complete)
                    for hour in range(24):
                        self.hourly_tracker.update_hourly_progress(
                            "marketing", day_index, hour, total_days
                        )
                    # Verify daily insert size once for marketing
                    try:
                        from sqlalchemy import func, select

                        model = self._get_model_for_table("marketing")
                        total_db = (
                            await self._session.execute(
                                select(func.count()).select_from(model)
                            )
                        ).scalar() or 0
                        logger.info(
                            f"marketing verification (daily): inserted={len(marketing_records)}, db_total={int(total_db)}"
                        )
                    except Exception as ve:
                        logger.debug(f"Marketing verification skipped: {ve}")
                except Exception as e:
                    logger.error(
                        f"Failed to insert marketing for {date.strftime('%Y-%m-%d')}: {e}"
                    )

        # 3. Generate and write store operations hour-by-hour to minimize memory usage
        # Define which tables are generated hourly (others are generated daily)
        hourly_generated_tables = [
            "receipts",
            "receipt_lines",
            "store_inventory_txn",
            "foot_traffic",
            "ble_pings",
            "fact_payments",
        ]

        # Log hourly data processing for debugging
        logger.debug(
            f"Day {day_index}/{total_days} ({date.strftime('%Y-%m-%d')}): Processing 24 hours"
        )

        # Generate and export each hour immediately to avoid accumulating all 24 hours in memory
        for hour_idx in range(24):
            # Generate data for this specific hour only
            hour_datetime = date.replace(
                hour=hour_idx, minute=0, second=0, microsecond=0
            )
            hour_multiplier = self.temporal_patterns.get_overall_multiplier(
                hour_datetime
            )

            # Early heartbeat for this hour before heavy generation
            try:
                if self._progress_callback:
                    progress_state = self.hourly_tracker.get_current_progress()
                    self._send_throttled_progress_update(
                        day_counter=day_index,
                        message=(
                            f"Preparing hour {hour_idx + 1}/24 for {date.strftime('%Y-%m-%d')}"
                        ),
                        total_days=total_days,
                        table_progress=progress_state.get("per_table_progress", {}),
                        tables_in_progress=progress_state.get("tables_in_progress", []),
                    )
            except Exception as e:
                logger.debug(
                    f"Failed to send progress update during hourly generation: {e}"
                )

            if hour_multiplier == 0:  # Store closed
                hour_data = {
                    "receipts": [],
                    "receipt_lines": [],
                    "store_inventory_txn": [],
                    "foot_traffic": [],
                    "ble_pings": [],
                    "fact_payments": [],
                }
            else:
                hour_data = {
                    "receipts": [],
                    "receipt_lines": [],
                    "store_inventory_txn": [],
                    "foot_traffic": [],
                    "ble_pings": [],
                    "fact_payments": [],
                }

                # Generate customer transactions for each store for this hour
                for store in self.stores:
                    store_hour_data = self._generate_store_hour_activity(
                        store, hour_datetime, hour_multiplier
                    )
                    for fact_type, records in store_hour_data.items():
                        hour_data[fact_type].extend(records)
            hourly_subset = {
                t: (hour_data.get(t, []) if t in active_tables else [])
                for t in active_tables
            }
            try:
                await self._export_hourly_facts(date, hour_idx, hourly_subset)

                # NEW: Update hourly progress tracker after successful export
                # Only update progress for tables that are actually generated hourly
                for table in hourly_generated_tables:
                    if table in active_tables:
                        self.hourly_tracker.update_hourly_progress(
                            table=table,
                            day=day_index,
                            hour=hour_idx,
                            total_days=total_days,
                        )
                        # Log receipts progress at debug level
                        if table == "receipts":
                            logger.debug(
                                f"Receipts progress updated: day={day_index}, hour={hour_idx}, total_days={total_days}"
                            )

                # NEW: Send progress update after hourly exports complete (throttled)
                if self._progress_callback:
                    progress_data = self.hourly_tracker.get_current_progress()
                    # Convert to table progress dict format expected by throttled update
                    table_progress = progress_data.get("per_table_progress", {})

                    # Log thread info for debugging
                    thread_name = threading.current_thread().name
                    logger.debug(
                        f"[{thread_name}] Sending hourly progress: day {day_index}/{total_days}, "
                        f"hour {hour_idx + 1}/24"
                    )

                    # Send throttled progress update with hourly detail
                    self._send_throttled_progress_update(
                        day_counter=day_index,
                        message=f"Generating {date.strftime('%Y-%m-%d')} (day {day_index}/{total_days}, hour {hour_idx + 1}/24)",
                        total_days=total_days,
                        table_progress=table_progress,
                        tables_in_progress=progress_data.get("tables_in_progress", []),
                    )
            except Exception as e:
                logger.error(f"Hourly export failed for {date} hour {hour_idx}: {e}")
            for fact_type, records in hour_data.items():
                if fact_type in active_tables:
                    daily_facts[fact_type].extend(records)

        # 4. Generate truck movements (based on inventory needs)
        # This creates initial shipments in SCHEDULED status
        if "truck_moves" in active_tables:
            base_store_txn = daily_facts.get("store_inventory_txn", [])
            truck_movements = self._generate_truck_movements(date, base_store_txn)
            daily_facts["truck_moves"].extend(truck_movements)

            # Process all active shipments and generate status progression throughout the day
            truck_lifecycle_records, dc_outbound_txn, store_inbound_txn = (
                self._process_truck_lifecycle(date)
            )

            # Add truck status progression records
            daily_facts["truck_moves"].extend(truck_lifecycle_records)

            # Add DC outbound transactions (when trucks are loaded)
            if "dc_inventory_txn" in active_tables and dc_outbound_txn:
                daily_facts["dc_inventory_txn"].extend(dc_outbound_txn)
                # Insert these lifecycle DC transactions immediately (daily batch)
                try:
                    await self._insert_hourly_to_db(
                        self._session,
                        "dc_inventory_txn",
                        dc_outbound_txn,
                        hour=0,
                        commit_every_batches=0,
                    )
                    # Mark hours complete for this table (lifecycle-generated)
                    for hour in range(24):
                        self.hourly_tracker.update_hourly_progress(
                            "dc_inventory_txn", day_index, hour, total_days
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to insert lifecycle dc_inventory_txn for {date.strftime('%Y-%m-%d')}: {e}"
                    )

            # Add store inbound transactions (when trucks are unloaded)
            if "store_inventory_txn" in active_tables and store_inbound_txn:
                daily_facts["store_inventory_txn"].extend(store_inbound_txn)
                # Insert these lifecycle store transactions immediately (daily batch)
                try:
                    await self._insert_hourly_to_db(
                        self._session,
                        "store_inventory_txn",
                        store_inbound_txn,
                        hour=0,
                        commit_every_batches=0,
                    )
                    # Mark hours complete for this table (lifecycle-generated)
                    for hour in range(24):
                        self.hourly_tracker.update_hourly_progress(
                            "store_inventory_txn", day_index, hour, total_days
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to insert lifecycle store_inventory_txn for {date.strftime('%Y-%m-%d')}: {e}"
                    )

            # Write all truck_moves records (including lifecycle progression)
            all_truck_moves = daily_facts.get("truck_moves", [])
            if all_truck_moves:
                try:
                    await self._insert_hourly_to_db(
                        self._session,
                        "truck_moves",
                        all_truck_moves,
                        hour=0,
                        commit_every_batches=0,
                    )
                    # Update progress for this daily-generated table (track all 24 hours as complete)
                    for hour in range(24):
                        self.hourly_tracker.update_hourly_progress(
                            "truck_moves", day_index, hour, total_days
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to insert truck_moves for {date.strftime('%Y-%m-%d')}: {e}"
                    )

        # 4a. Generate truck inventory tracking events
        if "truck_inventory" in active_tables:
            truck_inventory_events = (
                self.inventory_flow_sim.track_truck_inventory_status(date)
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
            # Update progress for this daily-generated table (treated as complete across hours)
            for hour in range(24):
                self.hourly_tracker.update_hourly_progress(
                    "truck_inventory", day_index, hour, total_days
                )

        # 5. Legacy delivery processing - now handled by _process_truck_lifecycle
        # Kept for backward compatibility but will be empty since lifecycle handles it
        if "store_inventory_txn" in active_tables:
            base_truck_moves = daily_facts.get("truck_moves", [])
            delivery_transactions = self._process_truck_deliveries(
                date, base_truck_moves
            )
            if delivery_transactions:
                # Only add if not already added by lifecycle processing
                # This prevents double-counting
                logger.debug(
                    f"Legacy delivery processing added {len(delivery_transactions)} transactions"
                )
                # Skip adding these since _process_truck_lifecycle handles it
                pass

        # 6. Generate online orders and integrate inventory effects
        if "online_orders" in active_tables:
            online_orders, online_store_txn, online_dc_txn, online_order_lines = (
                self._generate_online_orders(date)
            )
            daily_facts["online_orders"].extend(online_orders)

            # Generate payments for online orders (separate from in-store payments)
            # Note: In this synthetic data, declined payments don't affect order fulfillment.
            # Orders are always created - this simulates systems where payment processing
            # happens asynchronously or where declined payments trigger retry flows.
            online_order_payments: list[dict] = []
            if "fact_payments" in active_tables and online_orders:
                for order in online_orders:
                    payment = self._generate_payment_for_online_order(
                        order, order.get("EventTS", date)
                    )
                    online_order_payments.append(payment)
            # First write online order headers (so lines can resolve order_id)
            if online_orders:
                try:
                    await self._insert_hourly_to_db(
                        self._session,
                        "online_orders",
                        online_orders,
                        hour=0,
                        commit_every_batches=0,
                    )
                    # Update progress for this daily-generated table (track all 24 hours as complete)
                    for hour in range(24):
                        self.hourly_tracker.update_hourly_progress(
                            "online_orders", day_index, hour, total_days
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to insert online_orders for {date.strftime('%Y-%m-%d')}: {e}"
                    )
            # Then write online order lines (daily batch)
            if online_order_lines:
                try:
                    await self._insert_hourly_to_db(
                        self._session,
                        "online_order_lines",
                        online_order_lines,
                        hour=0,
                        commit_every_batches=0,
                    )
                    # Update progress for line items (treated as complete across hours)
                    for hour in range(24):
                        self.hourly_tracker.update_hourly_progress(
                            "online_order_lines", day_index, hour, total_days
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to insert online_order_lines for {date.strftime('%Y-%m-%d')}: {e}"
                    )
            # Insert online order payments (separate from hourly in-store payments)
            if online_order_payments:
                try:
                    await self._insert_hourly_to_db(
                        self._session,
                        "fact_payments",
                        online_order_payments,
                        hour=0,
                        commit_every_batches=0,
                    )
                    # Track as daily-generated (all hours complete)
                    # Note: In-store payments are tracked hourly; this is for online orders only
                    for hour in range(24):
                        self.hourly_tracker.update_hourly_progress(
                            "fact_payments", day_index, hour, total_days
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to insert online order payments for {date.strftime('%Y-%m-%d')}: {e}"
                    )
            # Cascade inventory effects
            if "store_inventory_txn" in active_tables and online_store_txn:
                daily_facts["store_inventory_txn"].extend(online_store_txn)
            if "dc_inventory_txn" in active_tables and online_dc_txn:
                daily_facts["dc_inventory_txn"].extend(online_dc_txn)

        # 7. Generate supply chain disruptions
        if "supply_chain_disruption" in active_tables:
            disruption_events = (
                self.inventory_flow_sim.simulate_supply_chain_disruptions(date)
            )
            for disruption in disruption_events:
                daily_facts["supply_chain_disruption"].append(
                    {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": self._randomize_time_within_day(
                            disruption["EventTS"]
                        ),
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

        # 8. Small store inventory adjustments for audit realism
        try:
            if "store_inventory_txn" in active_tables:
                adjustments: list[dict] = []
                # Create a few random adjustments per day across the network
                # ~0.02% of store-product combinations touched (bounded)
                num_stores = len(self.stores)
                samples = max(1, int(num_stores * 0.10))  # ~10% of stores per day
                sampled_stores = (
                    self._rng.sample(self.stores, min(samples, num_stores))
                    if self.stores
                    else []
                )
                for st in sampled_stores:
                    # Pick 1-3 random products to adjust
                    k = self._rng.randint(1, 3)
                    prods = self._rng.sample(self.products, k) if self.products else []
                    for p in prods:
                        # ±1 to ±5 units
                        delta = self._rng.randint(-5, 5)
                        if delta == 0:
                            continue
                        # Update in-memory balance
                        key = (st.ID, p.ID)
                        cur = self.inventory_flow_sim._store_inventory.get(key, 0)
                        new_bal = max(0, cur + delta)
                        self.inventory_flow_sim._store_inventory[key] = new_bal
                        adjustments.append(
                            {
                                "TraceId": self._generate_trace_id(),
                                "EventTS": date.replace(hour=22, minute=0, second=0),
                                "StoreID": st.ID,
                                "ProductID": p.ID,
                                "QtyDelta": delta,
                                "Reason": InventoryReason.ADJUSTMENT.value,
                                "Source": "CYCLE_COUNT",
                                "Balance": new_bal,
                            }
                        )
                if adjustments:
                    await self._insert_hourly_to_db(
                        self._session,
                        "store_inventory_txn",
                        adjustments,
                        hour=22,
                        commit_every_batches=0,
                    )
                    for hour in range(22, 24):
                        self.hourly_tracker.update_hourly_progress(
                            "store_inventory_txn", day_index, hour, total_days
                        )
        except Exception as e:
            logger.warning(
                f"Adjustment generation failed for {date.strftime('%Y-%m-%d')}: {e}"
            )

        # 9. Generate return receipts and inventory effects (baseline + holiday spikes)
        try:
            if getattr(self, "_use_duckdb", False):
                await self._generate_and_insert_returns_duckdb(date, active_tables)
            else:
                await self._generate_and_insert_returns(date, active_tables)
        except Exception as e:
            logger.warning(
                f"Return generation failed for {date.strftime('%Y-%m-%d')}: {e}"
            )

        return daily_facts
