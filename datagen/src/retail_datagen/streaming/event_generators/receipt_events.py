"""
Receipt event generation mixin.

Handles receipt creation, line items, and payment processing events.
"""

from datetime import datetime, timedelta

from retail_datagen.shared.models import TenderType
from retail_datagen.streaming.schemas import (
    PaymentProcessedPayload,
    ReceiptCreatedPayload,
    ReceiptLineAddedPayload,
)


class ReceiptEventsMixin:
    """
    Mixin providing receipt-related event generation.

    Handles receipt creation, line item additions, and payment processing
    with marketing attribution tracking.

    Requires parent class to provide:
        - rng: random.Random instance
        - state: EventGenerationState
        - customers: dict[int, Customer]
        - products: dict[int, ProductMaster]
    """

    def _generate_receipt_created(
        self, timestamp: datetime
    ) -> tuple[ReceiptCreatedPayload, str, str] | None:
        """Generate receipt created event.

        Respects marketing-driven purchase likelihood.
        """
        # Get customers who are currently in stores and haven't made a purchase yet
        eligible_sessions = [
            session
            for session in self.state.customer_sessions.values()
            if (
                timestamp < session["expected_exit_time"]
                and not session["has_made_purchase"]
                and timestamp >= session["entered_at"] + timedelta(minutes=2)
            )  # At least 2 minutes in store
        ]

        if not eligible_sessions:
            return None  # No eligible customers to make purchases

        # Apply purchase likelihood filtering - prioritize marketing-driven customers
        weighted_sessions = []
        for session in eligible_sessions:
            purchase_likelihood = session.get("purchase_likelihood", 0.4)  # Default 40%
            # Apply probability check
            if self.rng.random() < purchase_likelihood:
                # Weight marketing-driven customers higher for selection
                weight = 3 if session.get("marketing_driven", False) else 1
                weighted_sessions.extend([session] * weight)

        if not weighted_sessions:
            return None  # No customers decided to purchase this time

        session = self.rng.choice(weighted_sessions)
        store_id = session["store_id"]
        customer_id = session["customer_id"]
        receipt_id = f"RCP_{int(timestamp.timestamp())}_{self.rng.randint(1000, 9999)}"

        # Generate realistic receipt amounts - marketing customers spend more
        is_marketing_driven = session.get("marketing_driven", False)
        base_item_count = max(1, int(self.rng.gauss(4.2, 2.0)))  # From config

        if is_marketing_driven:
            # Marketing-driven customers buy 25% more items and spend 30% more
            item_count = int(base_item_count * 1.25)
            subtotal = self.rng.uniform(13.0, 260.0)  # 30% higher range
        else:
            item_count = base_item_count
            subtotal = self.rng.uniform(10.0, 200.0)

        tax_rate = 0.08  # 8% tax
        tax = round(subtotal * tax_rate, 2)
        total = subtotal + tax

        tender_type = self.rng.choice(list(TenderType))

        # Mark customer as having made a purchase and move them to checkout
        session["has_made_purchase"] = True
        session["current_zone"] = "CHECKOUT"
        session["expected_exit_time"] = timestamp + timedelta(
            minutes=self.rng.randint(2, 8)
        )  # Exit soon after purchase

        # Store receipt in active receipts for line items
        self.state.active_receipts[receipt_id] = {
            "store_id": store_id,
            "customer_id": customer_id,
            "item_count": item_count,
            "timestamp": timestamp,
            "marketing_driven": is_marketing_driven,
        }

        # Look up campaign_id for marketing-driven purchases (attribution tracking)
        # Uses O(1) customer_to_campaign index instead of O(n) linear search
        campaign_id = None
        if is_marketing_driven:
            campaign_id = self.state.customer_to_campaign.pop(customer_id, None)
            # Note: pop() removes entry after lookup to prevent unbounded growth
            # Each customer gets one attribution per marketing conversion

        payload = ReceiptCreatedPayload(
            store_id=store_id,
            customer_id=customer_id,
            receipt_id=receipt_id,
            subtotal=subtotal,
            tax=tax,
            total=total,
            tender_type=tender_type.value,
            item_count=item_count,
            campaign_id=campaign_id,  # Attribution tracking for marketing campaigns
        )

        return payload, receipt_id, f"store_{store_id}"

    def _generate_receipt_line_added(
        self, timestamp: datetime
    ) -> tuple[ReceiptLineAddedPayload, str, str] | None:
        """Generate receipt line added event."""
        if not self.state.active_receipts:
            return None

        receipt_id = self.rng.choice(list(self.state.active_receipts.keys()))
        receipt_info = self.state.active_receipts[receipt_id]

        # Generate line item
        product_id = self.rng.choice(list(self.products.keys()))
        product = self.products[product_id]
        quantity = self.rng.randint(1, 3)
        unit_price = float(product.SalePrice)
        extended_price = unit_price * quantity

        # Randomly apply promotion
        promo_code = None
        if self.rng.random() < 0.2:  # 20% chance of promotion
            promo_code = self.rng.choice(list(self.state.promotion_campaigns.keys()))

        payload = ReceiptLineAddedPayload(
            receipt_id=receipt_id,
            line_number=self.rng.randint(1, 10),
            product_id=product_id,
            quantity=quantity,
            unit_price=unit_price,
            extended_price=extended_price,
            promo_code=promo_code,
        )

        return payload, receipt_id, f"store_{receipt_info['store_id']}"

    def _generate_payment_processed(
        self, timestamp: datetime
    ) -> tuple[PaymentProcessedPayload, str, str] | None:
        """Generate payment processed event."""
        if not self.state.active_receipts:
            return None

        receipt_id = self.rng.choice(list(self.state.active_receipts.keys()))
        receipt_info = self.state.active_receipts[receipt_id]

        payment_method = self.rng.choice(list(TenderType)).value
        amount = round(self.rng.uniform(10.0, 200.0), 2)
        amount_cents = int(amount * 100)
        # Use 6-digit suffix (100000-999999) for consistency with PaymentsMixin
        # to minimize collision risk during high-volume periods
        transaction_id = (
            f"TXN_{int(timestamp.timestamp())}_{self.rng.randint(100000, 999999):06d}"
        )
        # Processing time varies by payment method
        processing_time_ms = self.rng.randint(500, 3000)
        store_id = receipt_info.get("store_id")
        customer_id = receipt_info.get("customer_id", self.rng.randint(1, 1000))

        payload = PaymentProcessedPayload(
            receipt_id=receipt_id,
            order_id=None,  # In-store payments have no order_id
            payment_method=payment_method,
            amount=amount,
            amount_cents=amount_cents,
            transaction_id=transaction_id,
            processing_time=timestamp,
            processing_time_ms=processing_time_ms,
            status="APPROVED",
            decline_reason=None,  # Approved payments have no decline reason
            store_id=store_id,
            customer_id=customer_id,
        )

        # Remove receipt from active receipts after payment
        if self.rng.random() < 0.8:  # 80% chance to complete receipt
            del self.state.active_receipts[receipt_id]

        return payload, receipt_id, f"store_{receipt_info['store_id']}"
