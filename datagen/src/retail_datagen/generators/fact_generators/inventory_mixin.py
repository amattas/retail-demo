"""
Inventory management for distribution centers and stores.

This module provides the main entry point for historical fact data generation.
The daily fact generation logic has been modularized into daily_facts_mixin.py.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import numpy as np

from retail_datagen.generators.utils import ProgressReporter

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from .daily_facts_mixin import DailyFactsMixin
from .models import FactGenerationSummary

# SessionMaker import for SQLite fallback path (deprecated, DuckDB-only runtime)
try:
    from retail_datagen.db.session import retail_session_maker

    SessionMaker = retail_session_maker()
except ImportError:
    SessionMaker = None  # type: ignore[assignment, misc]

logger = logging.getLogger(__name__)


class InventoryMixin(DailyFactsMixin):
    """Inventory management for distribution centers and stores.

    Inherits from:
        DailyFactsMixin: Daily fact generation orchestration
    """

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
            publish_to_outbox: Whether to also write to streaming_outbox table

        Returns:
            Summary of generation results

        Note:
            This method is async to support database operations. Call with:
            ```python
            summary = await generator.generate_historical_data(start, end)
            ```
        """
        generation_start_time = datetime.now(UTC)
        # Remember outbox preference for this run
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

        # Add table progress tracking
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
            "reorders": total_days * len(self.stores) * 2,
            "online_orders": (
                total_days * max(0, int(self.config.volume.online_orders_per_day))
            ),
        }
        expected_records = {
            k: v for k, v in expected_records_all.items() if k in active_tables
        }

        progress_reporter = ProgressReporter(total_days, "Generating historical data")

        # Generate data day by day
        current_date = start_date
        day_counter = 0

        async def _ensure_required_schema(session: AsyncSession) -> None:
            try:
                from sqlalchemy import text

                # Check if column exists
                result = await session.execute(
                    text("PRAGMA table_info('fact_receipts')")
                )
                cols = [row[1] for row in result.fetchall()]
                # receipt_id_ext
                if "receipt_id_ext" not in cols:
                    await session.execute(
                        text("ALTER TABLE fact_receipts ADD COLUMN receipt_id_ext TEXT")
                    )
                    await session.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS ix_fact_receipts_ext "
                            "ON fact_receipts (receipt_id_ext)"
                        )
                    )
                    logger.info(
                        "Migrated fact_receipts: added receipt_id_ext column and index"
                    )

                # receipt_type
                if "receipt_type" not in cols:
                    await session.execute(
                        text(
                            "ALTER TABLE fact_receipts "
                            "ADD COLUMN receipt_type TEXT NOT NULL DEFAULT 'SALE'"
                        )
                    )
                    await session.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS ix_fact_receipts_type "
                            "ON fact_receipts (receipt_type)"
                        )
                    )
                    logger.info(
                        "Migrated fact_receipts: added receipt_type column and index"
                    )

                # return_for_receipt_id
                if "return_for_receipt_id" not in cols:
                    await session.execute(
                        text(
                            "ALTER TABLE fact_receipts "
                            "ADD COLUMN return_for_receipt_id INTEGER"
                        )
                    )
                    await session.execute(
                        text(
                            "CREATE INDEX IF NOT EXISTS ix_fact_receipts_return_for "
                            "ON fact_receipts (return_for_receipt_id)"
                        )
                    )
                    logger.info(
                        "Migrated fact_receipts: "
                        "added return_for_receipt_id column and index"
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
                        "CREATE INDEX IF NOT EXISTS ix_online_order_lines_order "
                        "ON fact_online_order_lines (order_id)"
                    )
                )
                await session.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS "
                        "ix_online_order_lines_order_product "
                        "ON fact_online_order_lines (order_id, product_id)"
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

            # Drop nonessential indexes for faster bulk loads (SQLite only)
            dropped_indexes: list[tuple[str, str]] = []
            try:
                if not self._use_duckdb:
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

                # Generate daily facts (progress updates happen during generation)
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

                tables_completed_count = len(tables_completed)

                enhanced_message = (
                    f"Generating data for {current_date.strftime('%Y-%m-%d')} "
                    f"(day {day_counter}/{total_days}) "
                    f"({tables_completed_count}/{len(active_tables)} tables complete)"
                )

                # Update API progress with throttling
                self._send_throttled_progress_update(
                    day_counter,
                    enhanced_message,
                    total_days,
                    table_progress=table_progress,
                    tables_completed=tables_completed,
                    tables_in_progress=tables_in_progress,
                    tables_remaining=tables_remaining,
                    table_counts=(
                        self._table_insert_counts.copy()
                        if getattr(self, "_table_insert_counts", None)
                        else facts_generated.copy()
                    ),
                )

                progress_reporter.update(1)
                current_date += timedelta(days=1)

            # Recreate any dropped indexes after generation completes
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
