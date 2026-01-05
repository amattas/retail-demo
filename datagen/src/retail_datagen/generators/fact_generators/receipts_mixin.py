"""
Receipt generation and in-store customer activity
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from datetime import time as dt_time
from decimal import Decimal
import pandas as pd
from retail_datagen.shared.models import Customer, ProductMaster, Store, TenderType

logger = logging.getLogger(__name__)


class ReceiptsMixin:
    """Receipt generation and in-store customer activity"""

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

        # Holiday closure: Christmas Day closed (no activity)
        if hour_datetime.month == 12 and hour_datetime.day == 25:
            return hour_data

        # Calculate expected customers for this hour
        # NOTE: customers_per_day is configured PER STORE, not total across all stores
        base_customers_per_hour = self.config.volume.customers_per_day / 24

        # Apply store profile multiplier for realistic variability
        store_multiplier = float(getattr(store, 'daily_traffic_multiplier', Decimal("1.0")))
        expected_customers = int(base_customers_per_hour * multiplier * store_multiplier)

        # Generate foot traffic (will be calibrated to receipts with conversion rates)
        # Pass expected_customers (receipt count) to calculate realistic foot traffic
        foot_traffic_records = self._generate_foot_traffic(
            store, hour_datetime, expected_customers
        )
        hour_data["foot_traffic"].extend(foot_traffic_records)

        if expected_customers == 0:
            return hour_data

        # Generate customer transactions
        # Use precomputed per-store sampling to select customers for this hour in bulk
        if expected_customers > 0:
            if store.ID in self._store_customer_sampling_np:
                # Vectorized sampling via NumPy over index array, then map back to customers
                idx_arr, p = self._store_customer_sampling_np[store.ID]
                if len(idx_arr) > 0:
                    chosen_idx = self._np_rng.choice(
                        idx_arr, size=expected_customers, replace=True, p=p
                    )
                    clist = self._store_customer_sampling[store.ID][0]
                    selected_customers = [clist[i] for i in chosen_idx]
                else:
                    selected_customers = [self._rng.choice(self.customers) for _ in range(expected_customers)]
            elif store.ID in self._store_customer_sampling:
                customers_list, weights_list = self._store_customer_sampling[store.ID]
                selected_customers = self._rng.choices(
                    customers_list, weights=weights_list, k=expected_customers
                )
            else:
                selected_customers = [self._rng.choice(self.customers) for _ in range(expected_customers)]

            for customer in selected_customers:
                # Generate shopping basket (pass store for format-based adjustments)
                basket = self.customer_journey_sim.generate_shopping_basket(customer.ID, store=store)
                # Apply holiday overlay to adjust basket composition/quantities
                try:
                    self._apply_holiday_overlay_to_basket(hour_datetime, basket)
                except Exception as e:
                    logger.debug(f"Failed to apply holiday overlay to basket: {e}")

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

    # ---------------- Holiday Overlay Helpers -----------------

    def _create_receipt(
        self, store: Store, customer: Customer, basket: Any, transaction_time: datetime,
        _campaign_id: str | None = None,  # noqa: ARG002 - intentionally unused, see TODO below
    ) -> dict[str, list[dict]]:
        """Create receipt, receipt lines, and inventory transactions.

        Args:
            store: Store where transaction occurred
            customer: Customer making purchase
            basket: ShoppingBasket with items to purchase
            transaction_time: Timestamp of transaction
            _campaign_id: Optional marketing campaign ID for attribution tracking.
                When provided, indicates this purchase was influenced by a marketing
                campaign. Used by fn_attribution_window KQL function for ROI analysis.
                NOTE: Parameter prefixed with underscore to indicate it's intentionally
                unused in this implementation (see TODO below).

        Returns:
            Dictionary with receipt, lines, and inventory_transactions

        Raises:
            ValueError: If basket has no items (business rule violation)

        Note:
            campaign_id population for historical data requires attribution window
            analysis (matching ad impressions to purchases within a time window).
            See issue #78 for implementation details. Real-time streaming already
            supports campaign_id via event_factory.py.
        """
        # TODO(#78): Implement campaign_id attribution for historical data generation.
        # This requires:
        # 1. Track ad impressions per customer during marketing generation
        # 2. When generating receipts, check if customer had impression within attribution window
        # 3. If yes, pass campaign_id to receipt record
        # For now, _campaign_id parameter is accepted but not used in historical generation.
        # Real-time streaming (event_factory.py) already implements this logic.

        # CRITICAL: Validate basket has at least 1 item
        # Empty receipts violate business rules and should never be generated
        if not basket.items or len(basket.items) == 0:
            raise ValueError(
                f"Cannot create receipt with empty basket for customer {customer.ID} "
                f"at store {store.ID}. All receipts must have at least 1 line."
            )

        receipt_id = (
            f"RCP{transaction_time.strftime('%Y%m%d%H%M')}"
            f"{store.ID:03d}{self._rng.randint(1000, 9999)}"
        )
        trace_id = self._generate_trace_id()

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

        # Apply promotions to basket using CustomerJourneySimulator
        discount_amount, basket_items_with_promos = (
            self.customer_journey_sim.apply_promotions_to_basket(
                basket=basket,
                customer_id=customer.ID,
                transaction_date=transaction_time,
            )
        )
        # Helpers for integer-cents math
        from retail_datagen.shared.models import ProductTaxability
        def _to_cents(d: Decimal) -> int:
            return int((d * 100).quantize(Decimal("1")))

        def _fmt_cents(c: int) -> str:
            sign = '-' if c < 0 else ''
            c = abs(c)
            return f"{sign}{c // 100}.{c % 100:02d}"

        def _tax_cents(amount_cents: int, rate: Decimal, taxability: ProductTaxability) -> int:
            # rate to basis points (1/100 of a percent), multiplier as integer percentage
            rate_bps = int((rate * 10000).quantize(Decimal("1")))
            mult_pct = 100 if taxability == ProductTaxability.TAXABLE else 50 if taxability == ProductTaxability.REDUCED_RATE else 0
            # Compute rounded cents: (amount_cents * rate_bps * mult_pct) / 1_000_000
            num = amount_cents * rate_bps * mult_pct
            return (num + 500_000) // 1_000_000

        # Create receipt lines and inventory transactions using integer cents
        lines: list[dict] = []
        inventory_transactions: list[dict] = []
        subtotal_cents = 0
        total_tax_cents = 0

        # Get store tax rate (with backward compatibility default)
        store_tax_rate = (
            store.tax_rate if store.tax_rate is not None else Decimal("0.07407")
        )

        for line_num, item_data in enumerate(basket_items_with_promos, 1):
            product = item_data["product"]
            qty = int(item_data["qty"])  # ensure int
            promo_code = item_data.get("promo_code")
            line_discount_cents = _to_cents(item_data.get("discount", Decimal("0.00")))

            # Calculate unit price and ext price in cents
            unit_price_cents = _to_cents(product.SalePrice)
            ext_before_cents = unit_price_cents * qty
            ext_after_cents = max(0, ext_before_cents - line_discount_cents)

            # Calculate tax for this line item based on POST-DISCOUNT price
            taxability = getattr(product, "taxability", ProductTaxability.TAXABLE)
            line_tax_cents = _tax_cents(ext_after_cents, store_tax_rate, taxability)

            # Accumulate totals
            subtotal_cents += ext_after_cents
            total_tax_cents += line_tax_cents

            line = {
                "TraceId": trace_id,
                "EventTS": transaction_time,
                "ReceiptId": receipt_id,
                "Line": line_num,
                "ProductID": product.ID,
                "Qty": qty,
                "UnitPrice": _fmt_cents(unit_price_cents),
                "ExtPrice": _fmt_cents(ext_after_cents),
                "UnitCents": unit_price_cents,
                "ExtCents": ext_after_cents,
                "PromoCode": promo_code,
            }
            lines.append(line)

            # Create inventory transaction (sale)
            key = (store.ID, product.ID)
            current_balance = self.inventory_flow_sim._store_inventory.get(key, 0)
            new_balance = max(0, current_balance - qty)
            self.inventory_flow_sim._store_inventory[key] = new_balance

            # Get current balance after this transaction
            balance = self.inventory_flow_sim.get_store_balance(store.ID, product.ID)

            inventory_transaction = {
                "TraceId": trace_id,
                "EventTS": transaction_time,
                "StoreID": store.ID,
                "ProductID": product.ID,
                "QtyDelta": -qty,  # Negative for sale
                "Reason": InventoryReason.SALE.value,
                "Source": receipt_id,
                "Balance": balance,
            }
            inventory_transactions.append(inventory_transaction)

        # Header-level totals (preserve existing formula: Subtotal - Discount + Tax)
        discount_amount_cents = _to_cents(discount_amount)
        total_cents = subtotal_cents - discount_amount_cents + total_tax_cents

        # Validate subtotal (sanity check)
        try:
            calculated_subtotal_cents = sum(_to_cents(Decimal(line["ExtPrice"])) for line in lines)
            if abs(calculated_subtotal_cents - subtotal_cents) > 1:
                logger.error(
                    f"Receipt {receipt_id}: Subtotal mismatch! "
                    f"Calculated={calculated_subtotal_cents}, Recorded={subtotal_cents}"
                )
        except (ValueError, TypeError, ArithmeticError) as e:
            logger.warning(f"Failed to validate subtotal for receipt {receipt_id}: {e}")

        # Create receipt header
        receipt = {
            "TraceId": trace_id,
            "EventTS": transaction_time,
            "StoreID": store.ID,
            "CustomerID": customer.ID,
            "ReceiptId": receipt_id,
            "ReceiptType": "SALE",
            "Subtotal": _fmt_cents(subtotal_cents),
            "DiscountAmount": _fmt_cents(discount_amount_cents),  # Phase 2.2: Promotional discounts
            "Tax": _fmt_cents(total_tax_cents),
            "Total": _fmt_cents(total_cents),
            "SubtotalCents": subtotal_cents,
            "TaxCents": total_tax_cents,
            "TotalCents": total_cents,
            "TenderType": tender_type.value,
        }

        return {
            "receipt": receipt,
            "lines": lines,
            "inventory_transactions": inventory_transactions,
        }


