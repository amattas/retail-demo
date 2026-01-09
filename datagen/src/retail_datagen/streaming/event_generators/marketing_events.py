"""
Marketing event generation mixin.

Handles ad impressions, promotion applications, and online order events.
"""

from datetime import datetime, timedelta

from retail_datagen.shared.models import DeviceType, MarketingChannel, TenderType
from retail_datagen.streaming.schemas import (
    AdImpressionPayload,
    OnlineOrderCreatedPayload,
    PromotionAppliedPayload,
)


class MarketingEventsMixin:
    """
    Mixin providing marketing-related event generation.

    Handles ad impression tracking with conversion attribution, promotion
    application events, and online order creation.

    Requires parent class to provide:
        - rng: random.Random instance
        - state: EventGenerationState
        - stores: dict[int, Store]
        - dcs: dict[int, DistributionCenter]
        - customers: dict[int, Customer]
        - products: dict[int, ProductMaster]
        - generate_trace_id(timestamp): method to create trace IDs
    """

    def _generate_ad_impression(
        self, timestamp: datetime
    ) -> tuple[AdImpressionPayload, str, str]:
        """Generate ad impression event with conversion tracking."""
        channel = self.rng.choice(list(MarketingChannel))
        campaign_id = self.rng.choice(list(self.state.promotion_campaigns.keys()))
        creative_id = f"CRE_{self.rng.randint(1000, 9999)}"
        customer = self.rng.choice(list(self.customers.values()))
        customer_ad_id = customer.AdId
        impression_id = (
            f"IMP_{int(timestamp.timestamp())}_{self.rng.randint(1000, 9999)}"
        )
        cost = self.rng.uniform(0.10, 2.50)  # Cost per impression
        device_type = self.rng.choice(list(DeviceType))

        # Industry standard conversion rates by channel
        conversion_rates = {
            MarketingChannel.SOCIAL: 0.012,  # 1.2% - social media ads
            MarketingChannel.SEARCH: 0.035,  # 3.5% - search ads
            MarketingChannel.DISPLAY: 0.008,  # 0.8% - display ads
            MarketingChannel.EMAIL: 0.025,  # 2.5% - email campaigns
            MarketingChannel.VIDEO: 0.015,  # 1.5% - video ads
        }

        # Determine if this impression will convert to store visit
        conversion_rate = conversion_rates.get(channel, 0.015)
        will_convert = self.rng.random() < conversion_rate

        if will_convert:
            # Schedule conversion: customer will visit store within 1-48 hours
            conversion_delay_hours = self.rng.uniform(1, 48)
            conversion_time = timestamp + timedelta(hours=conversion_delay_hours)

            self.state.marketing_conversions[impression_id] = {
                "customer_id": customer.ID,
                "customer_ad_id": customer_ad_id,
                "campaign_id": campaign_id,
                "channel": channel.value,
                "scheduled_visit_time": conversion_time,
                "converted": False,
            }
            # Maintain O(1) lookup index for campaign attribution
            self.state.customer_to_campaign[customer.ID] = campaign_id

        payload = AdImpressionPayload(
            channel=channel.value,
            campaign_id=campaign_id,
            creative_id=creative_id,
            customer_ad_id=customer_ad_id,
            impression_id=impression_id,
            cost=cost,
            device_type=device_type.value,
        )

        return payload, impression_id, f"marketing_{channel.value}"

    def _generate_promotion_applied(
        self, timestamp: datetime
    ) -> tuple[PromotionAppliedPayload, str, str] | None:
        """Generate promotion applied event."""
        if not self.state.active_receipts:
            return None

        receipt_id = self.rng.choice(list(self.state.active_receipts.keys()))
        receipt_info = self.state.active_receipts[receipt_id]

        promo_code = self.rng.choice(list(self.state.promotion_campaigns.keys()))
        self.state.promotion_campaigns[promo_code]

        discount_amount = round(self.rng.uniform(5.0, 25.0), 2)
        discount_cents = int(discount_amount * 100)
        discount_type = self.rng.choices(
            ["PERCENTAGE", "FIXED_AMOUNT", "BOGO"],
            weights=[0.7, 0.25, 0.05],
        )[0]
        product_ids = [
            self.rng.choice(list(self.products.keys()))
            for _ in range(self.rng.randint(1, 3))
        ]
        store_id = receipt_info["store_id"]
        customer_id = receipt_info["customer_id"]

        payload = PromotionAppliedPayload(
            receipt_id=receipt_id,
            promo_code=promo_code,
            discount_amount=discount_amount,
            discount_cents=discount_cents,
            discount_type=discount_type,
            product_count=len(product_ids),
            product_ids=product_ids,
            store_id=store_id,
            customer_id=customer_id,
        )

        return payload, receipt_id, f"store_{receipt_info['store_id']}"

    def _generate_online_order_created(
        self, timestamp: datetime
    ) -> tuple[OnlineOrderCreatedPayload, str, str]:
        """Generate an online order created event with fulfillment details.

        Fulfillment mode distribution:
        - SHIP_FROM_DC: 60% (most common)
        - SHIP_FROM_STORE: 30% (ship-from-store programs)
        - BOPIS: 10% (buy online, pick up in store)
        """
        customer_id = self.rng.choice(list(self.customers.keys()))
        mode = self.rng.choices(
            ["SHIP_FROM_DC", "SHIP_FROM_STORE", "BOPIS"], weights=[0.60, 0.30, 0.10]
        )[0]

        if mode in ("SHIP_FROM_STORE", "BOPIS"):
            node_type = "STORE"
            node_id = self.rng.choice(list(self.stores.keys()))
        else:
            node_type = "DC"
            node_id = self.rng.choice(list(self.dcs.keys()))

        item_count = max(1, int(self.rng.gauss(3.5, 1.8)))
        subtotal = self.rng.uniform(15.0, 220.0)
        tax = round(subtotal * 0.08, 2)
        total = subtotal + tax
        tender_type = self.rng.choice(list(TenderType)).value

        order_id = f"ONL_{int(timestamp.timestamp())}_{self.rng.randint(1000, 9999)}"
        trace_id = self.generate_trace_id(timestamp)

        payload = OnlineOrderCreatedPayload(
            order_id=order_id,
            customer_id=customer_id,
            fulfillment_mode=mode,
            node_type=node_type,
            node_id=node_id,
            item_count=item_count,
            subtotal=subtotal,
            tax=tax,
            total=total,
            tender_type=tender_type,
        )

        partition_key = f"{node_type.lower()}_{node_id}"
        return payload, trace_id, partition_key
