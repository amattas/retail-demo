"""
Utility methods for fact data generation.
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal

from retail_datagen.shared.models import (
    InventoryReason,
    ProductMaster,
    ProductTaxability,
    TenderType,
)

logger = logging.getLogger(__name__)


class UtilsMixin:
    """Utility helper methods for fact generation."""

    def _get_available_products_for_date(self, date: datetime) -> list[ProductMaster]:
        """Get products that have been launched by the given date."""
        return [
            product
            for product in self.products
            if product.LaunchDate.date() <= date.date()
        ]

    async def _export_hourly_facts(
        self, date: datetime, hour: int, hourly_facts: dict[str, list[dict]]
    ) -> None:
        """
        Export hourly facts to database.

        Args:
            date: Date being generated
            hour: Hour of day (0-23)
            hourly_facts: Dictionary mapping table names to record lists
        """
        # Deterministic insertion order to respect FKs
        preferred_order = [
            "receipts",
            "receipt_lines",
            "store_inventory_txn",
            "dc_inventory_txn",
            "truck_moves",
            "foot_traffic",
            "ble_pings",
            "online_orders",
        ]
        batch_hours = 1
        try:
            batch_hours = max(
                1, int(getattr(self.config.performance, "batch_hours", 1))
            )
        except (AttributeError, ValueError, TypeError) as e:
            logger.debug(f"Failed to get batch_hours from config, using default 1: {e}")
            batch_hours = 1

        flush_now = ((hour + 1) % batch_hours == 0) or (hour == 23)

        for fact_table in preferred_order:
            records = hourly_facts.get(fact_table) or []
            if records:
                # Accumulate into batch buffer
                self._batch_buffers.setdefault(fact_table, []).extend(records)

            if not flush_now:
                continue

            # Flush this table's buffer if any
            buf = self._batch_buffers.get(fact_table) or []
            if not buf:
                continue

            try:
                await self._insert_hourly_to_db(self._session, fact_table, buf, hour)
                # Clear buffer after successful insert
                self._batch_buffers[fact_table] = []
            except Exception as e:
                logger.error(
                    f"Failed to insert {fact_table} batched up to hour {hour}: {e}"
                )
                raise

    def _is_food_product(self, product: ProductMaster) -> bool:
        dept = (product.Department or "").lower()
        cat = (product.Category or "").lower()
        food_keywords = [
            "grocery",
            "food",
            "produce",
            "meat",
            "seafood",
            "dairy",
            "bakery",
            "beverage",
            "snack",
            "pantry",
            "frozen",
        ]
        return any(k in dept or k in cat for k in food_keywords)

    async def _generate_and_insert_returns_duckdb(
        self, date: datetime, active_tables: list[str]
    ) -> None:
        """DuckDB-native returns generator using external receipt IDs for linkage.

        Samples same-day SALE receipts, creates RETURN headers/lines, and
        emits corresponding store inventory transactions including
        dispositions (damaged/RTV/restock).
        """
        if "receipts" not in active_tables or "receipt_lines" not in active_tables:
            return

        if not getattr(self, "_duckdb_conn", None):
            return

        # Spike factor for Dec 26 (day after Christmas)
        spike = 6.0 if (date.month == 12 and date.day == 26) else 1.0
        target_pct = 0.01 * spike

        date_str = date.strftime("%Y-%m-%d")
        q = (
            "SELECT receipt_id_ext, store_id FROM fact_receipts "
            "WHERE date(event_ts)=? AND (receipt_type IS NULL OR receipt_type='SALE')"
        )
        rows = self._duckdb_conn.execute(q, [date_str]).fetchall()
        if not rows:
            return

        import random as _r

        max_returns = max(1, int(len(rows) * min(0.10, target_pct)))
        sampled = _r.sample(rows, min(max_returns, len(rows)))

        return_receipts: list[dict] = []
        return_lines: list[dict] = []
        return_store_txn: list[dict] = []

        store_rates = {
            s.ID: (s.tax_rate if s.tax_rate is not None else Decimal("0.07407"))
            for s in self.stores
        }
        products_by_id = {p.ID: p for p in self.products}

        for orig_receipt_ext, store_id in sampled:
            lq = (
                "SELECT product_id, quantity, unit_price, ext_price, line_num "
                "FROM fact_receipt_lines WHERE receipt_id_ext=?"
            )
            line_rows = self._duckdb_conn.execute(lq, [orig_receipt_ext]).fetchall()
            if not line_rows:
                continue

            return_id_ext = (
                f"RET{date.strftime('%Y%m%d')}"
                f"{int(store_id):03d}{self._rng.randint(1000, 9999)}"
            )
            trace_id = self._generate_trace_id()
            store_tax_rate = store_rates.get(int(store_id), Decimal("0.07407"))
            subtotal = Decimal("0.00")
            total_tax = Decimal("0.00")

            for product_id, qty, unit_price, ext_price, line_num in line_rows:
                product = products_by_id.get(int(product_id))
                if not product:
                    continue
                nqty = int(qty) * -1
                unit_price_dec = self._to_decimal(unit_price)
                neg_ext = (unit_price_dec * Decimal(nqty)).quantize(Decimal("0.01"))

                taxability = getattr(product, "taxability", ProductTaxability.TAXABLE)
                tax_mult = (
                    Decimal("1.0")
                    if taxability == ProductTaxability.TAXABLE
                    else (
                        Decimal("0.5")
                        if taxability == ProductTaxability.REDUCED_RATE
                        else Decimal("0.0")
                    )
                )
                line_tax = (neg_ext * store_tax_rate * tax_mult).quantize(
                    Decimal("0.01")
                )

                subtotal += neg_ext
                total_tax += line_tax

                return_lines.append(
                    {
                        "TraceId": trace_id,
                        "EventTS": date.replace(hour=12, minute=0, second=0),
                        "ReceiptId": return_id_ext,
                        "Line": int(line_num),
                        "ProductID": int(product_id),
                        "Qty": nqty,
                        "UnitPrice": str(unit_price_dec.quantize(Decimal("0.01"))),
                        "ExtPrice": str(neg_ext),
                        "PromoCode": "RETURN",
                    }
                )

                # Store inventory add for return
                key = (int(store_id), int(product_id))
                cur = self.inventory_flow_sim._store_inventory.get(key, 0)
                self.inventory_flow_sim._store_inventory[key] = cur + (-nqty)
                balance = self.inventory_flow_sim.get_store_balance(
                    int(store_id), int(product_id)
                )
                return_store_txn.append(
                    {
                        "TraceId": trace_id,
                        "EventTS": date.replace(hour=12, minute=30, second=0),
                        "StoreID": int(store_id),
                        "ProductID": int(product_id),
                        "QtyDelta": -nqty,
                        "Reason": InventoryReason.RETURN.value,
                        "Source": return_id_ext,
                        "Balance": balance,
                    }
                )

                # Dispositions
                if self._is_food_product(product):
                    self.inventory_flow_sim._store_inventory[key] = max(
                        0, self.inventory_flow_sim._store_inventory[key] - (-nqty)
                    )
                    balance2 = self.inventory_flow_sim.get_store_balance(
                        int(store_id), int(product_id)
                    )
                    return_store_txn.append(
                        {
                            "TraceId": trace_id,
                            "EventTS": date.replace(hour=12, minute=45, second=0),
                            "StoreID": int(store_id),
                            "ProductID": int(product_id),
                            "QtyDelta": nqty,
                            "Reason": InventoryReason.DAMAGED.value,
                            "Source": "RETURN_DESTROY",
                            "Balance": balance2,
                        }
                    )
                else:
                    r = self._rng.random()
                    if r < 0.40:
                        pass
                    elif r < 0.60:
                        pass
                    elif r < 0.90:
                        self.inventory_flow_sim._store_inventory[key] = max(
                            0, self.inventory_flow_sim._store_inventory[key] - (-nqty)
                        )
                        balance2 = self.inventory_flow_sim.get_store_balance(
                            int(store_id), int(product_id)
                        )
                        return_store_txn.append(
                            {
                                "TraceId": trace_id,
                                "EventTS": date.replace(hour=13, minute=0, second=0),
                                "StoreID": int(store_id),
                                "ProductID": int(product_id),
                                "QtyDelta": nqty,
                                "Reason": InventoryReason.OUTBOUND_SHIPMENT.value,
                                "Source": "RTV",
                                "Balance": balance2,
                            }
                        )
                    else:
                        self.inventory_flow_sim._store_inventory[key] = max(
                            0, self.inventory_flow_sim._store_inventory[key] - (-nqty)
                        )
                        balance2 = self.inventory_flow_sim.get_store_balance(
                            int(store_id), int(product_id)
                        )
                        return_store_txn.append(
                            {
                                "TraceId": trace_id,
                                "EventTS": date.replace(hour=13, minute=15, second=0),
                                "StoreID": int(store_id),
                                "ProductID": int(product_id),
                                "QtyDelta": nqty,
                                "Reason": InventoryReason.DAMAGED.value,
                                "Source": "RETURN_DESTROY",
                                "Balance": balance2,
                            }
                        )

            total = (subtotal + total_tax).quantize(Decimal("0.01"))
            return_receipts.append(
                {
                    "TraceId": trace_id,
                    "EventTS": date.replace(hour=12, minute=0, second=0),
                    "StoreID": int(store_id),
                    "CustomerID": None,
                    "ReceiptId": return_id_ext,
                    "ReceiptType": "RETURN",
                    "ReturnForReceiptIdExt": str(orig_receipt_ext),
                    "Subtotal": str(subtotal),
                    "DiscountAmount": str(Decimal("0.00")),
                    "Tax": str(total_tax),
                    "Total": str(total),
                    "TenderType": TenderType.CREDIT_CARD.value,
                }
            )

        if return_receipts:
            await self._insert_hourly_to_db(
                self._session,
                "receipts",
                return_receipts,
                hour=0,
                commit_every_batches=0,
            )
        if return_lines:
            await self._insert_hourly_to_db(
                self._session,
                "receipt_lines",
                return_lines,
                hour=0,
                commit_every_batches=0,
            )
        if return_store_txn and "store_inventory_txn" in active_tables:
            await self._insert_hourly_to_db(
                self._session,
                "store_inventory_txn",
                return_store_txn,
                hour=0,
                commit_every_batches=0,
            )

    async def _generate_and_insert_returns(
        self, date: datetime, active_tables: list[str]
    ) -> None:
        """Generate return receipts for this date (baseline + Dec 26 spike).

        Insert into DB.

        Strategy: sample a small subset of recent receipts, build negative receipts with
        corresponding inventory transactions and dispositions.
        """
        from sqlalchemy import text

        if "receipts" not in active_tables or "receipt_lines" not in active_tables:
            return

        # Determine spike factor for Dec 26 (day after Christmas)
        spike = 1.0
        if date.month == 12 and date.day == 26:
            spike = 6.0  # mid point of 5–10x

        # Baseline target returns per day as percentage of receipts (approx)
        # We'll cap to avoid heavy runtime on large datasets
        target_pct = 0.01 * spike  # 1% baseline, spiked on Dec 26

        # Fetch today's receipts (ids and store_id)
        rows = (
            await self._session.execute(
                text(
                    "SELECT receipt_id, store_id, event_ts FROM fact_receipts "
                    "WHERE date(event_ts)=:d "
                    "AND (receipt_type IS NULL OR receipt_type='SALE')"
                ),
                {"d": date.strftime("%Y-%m-%d")},
            )
        ).fetchall()
        if not rows:
            return

        import random as _r

        max_returns = max(
            1, int(len(rows) * min(0.1, target_pct))
        )  # cap at 10% for safety
        sampled = _r.sample(rows, min(max_returns, len(rows)))

        # Build return receipts
        return_receipts = []
        return_lines = []
        return_store_txn = []

        store_rates = {
            s.ID: (s.tax_rate if s.tax_rate is not None else Decimal("0.07407"))
            for s in self.stores
        }
        products_by_id = {p.ID: p for p in self.products}

        for orig_receipt_pk, store_id, event_ts in sampled:
            # Fetch lines for original receipt
            line_rows = (
                await self._session.execute(
                    text(
                        "SELECT product_id, quantity, unit_price, ext_price, "
                        "line_num FROM fact_receipt_lines WHERE receipt_id=:rid"
                    ),
                    {"rid": orig_receipt_pk},
                )
            ).fetchall()
            if not line_rows:
                continue

            # Build return header
            return_id_ext = (
                f"RET{date.strftime('%Y%m%d')}"
                f"{store_id:03d}{self._rng.randint(1000, 9999)}"
            )
            trace_id = self._generate_trace_id()
            store_tax_rate = store_rates.get(store_id, Decimal("0.07407"))
            subtotal = Decimal("0.00")
            total_tax = Decimal("0.00")

            # Lines
            for product_id, qty, unit_price, ext_price, line_num in line_rows:
                product = products_by_id.get(int(product_id))
                if not product:
                    continue
                # Negative quantities and ext price
                nqty = int(qty) * -1
                unit_price_dec = self._to_decimal(unit_price)
                (unit_price_dec * nqty).quantize(Decimal("0.01"))
                # ext_price could be used, but recompute to ensure consistency
                # with negative qty
                neg_ext = (unit_price_dec * Decimal(nqty)).quantize(Decimal("0.01"))

                # Taxability
                taxability = getattr(product, "taxability", ProductTaxability.TAXABLE)
                if taxability == ProductTaxability.TAXABLE:
                    tax_mult = Decimal("1.0")
                elif taxability == ProductTaxability.REDUCED_RATE:
                    tax_mult = Decimal("0.5")
                else:
                    tax_mult = Decimal("0.0")

                line_tax = (neg_ext * store_tax_rate * tax_mult).quantize(
                    Decimal("0.01")
                )

                subtotal += neg_ext
                total_tax += line_tax

                return_lines.append(
                    {
                        "TraceId": trace_id,
                        "EventTS": date.replace(hour=12, minute=0, second=0),
                        "ReceiptId": return_id_ext,
                        "Line": int(line_num),
                        "ProductID": int(product_id),
                        "Qty": nqty,
                        "UnitPrice": str(unit_price_dec.quantize(Decimal("0.01"))),
                        "ExtPrice": str(neg_ext),
                        "PromoCode": "RETURN",
                    }
                )

                # Inventory add for return
                key = (int(store_id), int(product_id))
                current_balance = self.inventory_flow_sim._store_inventory.get(key, 0)
                self.inventory_flow_sim._store_inventory[key] = current_balance + (
                    -nqty
                )
                balance = self.inventory_flow_sim.get_store_balance(
                    int(store_id), int(product_id)
                )

                return_store_txn.append(
                    {
                        "TraceId": trace_id,
                        "EventTS": date.replace(hour=12, minute=30, second=0),
                        "StoreID": int(store_id),
                        "ProductID": int(product_id),
                        "QtyDelta": -nqty,  # positive added back
                        "Reason": InventoryReason.RETURN.value,
                        "Source": return_id_ext,
                        "Balance": balance,
                    }
                )

                # Disposition
                if self._is_food_product(product):
                    # Destroy all food returns
                    self.inventory_flow_sim._store_inventory[key] = max(
                        0, self.inventory_flow_sim._store_inventory[key] - (-nqty)
                    )
                    balance2 = self.inventory_flow_sim.get_store_balance(
                        int(store_id), int(product_id)
                    )
                    return_store_txn.append(
                        {
                            "TraceId": trace_id,
                            "EventTS": date.replace(hour=12, minute=45, second=0),
                            "StoreID": int(store_id),
                            "ProductID": int(product_id),
                            "QtyDelta": nqty,  # remove the quantity (negative of added)
                            "Reason": InventoryReason.DAMAGED.value,
                            "Source": "RETURN_DESTROY",
                            "Balance": balance2,
                        }
                    )
                else:
                    # Non-food: 40% restock, 20% open-box, 30% RTV, 10% destroy
                    r = self._rng.random()
                    if r < 0.40:
                        # Restock: nothing additional
                        pass
                    elif r < 0.60:
                        # Open box: keep in stock for future sale (no immediate txn)
                        pass
                    elif r < 0.90:
                        # Return to vendor: outbound shipment
                        self.inventory_flow_sim._store_inventory[key] = max(
                            0, self.inventory_flow_sim._store_inventory[key] - (-nqty)
                        )
                        balance2 = self.inventory_flow_sim.get_store_balance(
                            int(store_id), int(product_id)
                        )
                        return_store_txn.append(
                            {
                                "TraceId": trace_id,
                                "EventTS": date.replace(hour=13, minute=0, second=0),
                                "StoreID": int(store_id),
                                "ProductID": int(product_id),
                                "QtyDelta": nqty,
                                "Reason": InventoryReason.OUTBOUND_SHIPMENT.value,
                                "Source": "RTV",
                                "Balance": balance2,
                            }
                        )
                    else:
                        # Destroy/damaged
                        self.inventory_flow_sim._store_inventory[key] = max(
                            0, self.inventory_flow_sim._store_inventory[key] - (-nqty)
                        )
                        balance2 = self.inventory_flow_sim.get_store_balance(
                            int(store_id), int(product_id)
                        )
                        return_store_txn.append(
                            {
                                "TraceId": trace_id,
                                "EventTS": date.replace(hour=13, minute=15, second=0),
                                "StoreID": int(store_id),
                                "ProductID": int(product_id),
                                "QtyDelta": nqty,
                                "Reason": InventoryReason.DAMAGED.value,
                                "Source": "RETURN_DESTROY",
                                "Balance": balance2,
                            }
                        )

            total = (subtotal + total_tax).quantize(Decimal("0.01"))
            return_receipts.append(
                {
                    "TraceId": trace_id,
                    "EventTS": date.replace(hour=12, minute=0, second=0),
                    "StoreID": int(store_id),
                    "CustomerID": None,
                    "ReceiptId": return_id_ext,
                    "ReceiptType": "RETURN",
                    "ReturnForReceiptId": int(orig_receipt_pk),
                    "Subtotal": str(subtotal),
                    "DiscountAmount": str(Decimal("0.00")),
                    "Tax": str(total_tax),
                    "Total": str(total),
                    "TenderType": TenderType.CREDIT_CARD.value,
                }
            )

        # Insert returns in a single commit each (daily batch)
        if return_receipts:
            await self._insert_hourly_to_db(
                self._session,
                "receipts",
                return_receipts,
                hour=0,
                commit_every_batches=0,
            )
        if return_lines:
            await self._insert_hourly_to_db(
                self._session,
                "receipt_lines",
                return_lines,
                hour=0,
                commit_every_batches=0,
            )
        if return_store_txn and "store_inventory_txn" in active_tables:
            await self._insert_hourly_to_db(
                self._session,
                "store_inventory_txn",
                return_store_txn,
                hour=0,
                commit_every_batches=0,
            )

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
