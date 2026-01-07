"""
Marketing campaign generation and effectiveness tracking
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from decimal import Decimal

logger = logging.getLogger(__name__)


class MarketingMixin:
    """Marketing campaign generation and effectiveness tracking"""

    def _generate_marketing_activity(
        self, date: datetime, multiplier: float
    ) -> list[dict]:
        """Generate marketing impressions and campaign activity.

        Uses pandas DataFrame optimization when available, falls back to
        loop-based processing if pandas operations fail. Fallback is logged
        at warning level for operational visibility.
        """

        # Defensive check: Verify simulator exists
        if self.marketing_campaign_sim is None:
            logger.error(
                "Marketing simulator not initialized - skipping marketing generation"
            )
            return []

        # Lightweight trace (DEBUG to avoid perf impact)
        logger.debug(
            f"_generate_marketing_activity: date={date}, mult={multiplier}, active={len(self._active_campaigns)}"
        )

        marketing_records = []

        # Check if new campaigns should start
        new_campaign_type = self.marketing_campaign_sim.should_start_campaign(
            date, multiplier
        )
        logger.debug(f"should_start_campaign returned: {new_campaign_type}")
        if new_campaign_type:
            campaign_id = self.marketing_campaign_sim.start_campaign(
                new_campaign_type, date
            )
            logger.debug(f"start_campaign returned: {campaign_id}")

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
                logger.debug(
                    f"Campaign {campaign_id} added (total: {len(self._active_campaigns)})"
                )
            else:
                logger.error(f"Campaign {campaign_id} failed to create in simulator")
                logger.debug(
                    f"Simulator active campaigns: {list(self.marketing_campaign_sim._active_campaigns.keys())}"
                )
                # Critical failure - don't continue processing this day

        # Debug: Log state before sync
        logger.debug(
            f"Campaigns: fact_gen={len(self._active_campaigns)} sim={len(self.marketing_campaign_sim._active_campaigns)}"
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

        logger.debug(f"After sync: fact_gen campaigns={len(self._active_campaigns)}")

        # Performance guard: cap total impressions/day (scaled by multiplier)
        base_cap = (
            getattr(self.config.volume, "marketing_impressions_per_day", 10000) or 10000
        )
        daily_cap = max(1000, int(base_cap * max(0.5, min(multiplier, 2.0))))
        emitted = 0

        # Generate impressions for active campaigns
        for campaign_id in list(self._active_campaigns.keys()):
            logger.debug(f"Processing campaign {campaign_id}")

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

            logger.debug(
                f"Campaign: {campaign.get('type', 'unknown')} end={campaign.get('end_date', 'unknown')}"
            )

            if date > campaign["end_date"]:
                # Campaign has completed its scheduled run
                del self._active_campaigns[campaign_id]
                logger.debug(f"Campaign {campaign_id} completed on {date}")
                logger.info(f"    Campaign {campaign_id} DELETED (expired)")
                continue

            logger.debug(f"Campaign {campaign_id} active, generating impressions...")

            try:
                impressions = self.marketing_campaign_sim.generate_campaign_impressions(
                    campaign_id, date, multiplier
                )
            except Exception as e:
                logger.error(
                    f"generate_campaign_impressions failed for {campaign_id}: {e}"
                )
                impressions = []

            logger.debug(f"impressions returned={len(impressions)}")

            if not impressions:
                logger.warning(
                    f"    No impressions generated for campaign {campaign_id}"
                )

            # Note: Zero impressions are acceptable - campaign continues if not expired
            try:
                import pandas as _pd
                if impressions:
                    df = _pd.DataFrame(impressions)
                    if not df.empty:
                        n = len(df)
                        # CRM resolution mask (5%)
                        crm_mask = self._np_rng.random(n) < 0.05
                        # Map AdId -> CustomerID for known subset
                        adids = df.get("CustomerAdId")
                        cust_ids = [None] * n
                        if adids is not None and len(self._adid_to_customer_id) > 0:
                            mapped = [self._adid_to_customer_id.get(a) for a in adids]
                            for i in range(n):
                                if crm_mask[i]:
                                    cust_ids[i] = mapped[i]
                        # Event timestamps and trace ids
                        event_ts = [self._randomize_time_within_day(date) for _ in range(n)]
                        trace_ids = [self._generate_trace_id() for _ in range(n)]
                        # Build output records
                        out = _pd.DataFrame(
                            {
                                "TraceId": trace_ids,
                                "EventTS": event_ts,
                                "Channel": df["Channel"].apply(lambda c: c.value),
                                "CampaignId": df["CampaignId"],
                                "CreativeId": df["CreativeId"],
                                "CustomerAdId": df["CustomerAdId"],
                                "CustomerId": cust_ids,
                                "ImpressionId": df["ImpressionId"],
                                "Cost": df["Cost"].apply(lambda d: str(d)),
                                "CostCents": df["Cost"].apply(
                                    lambda d: int((d * 100).quantize(Decimal("1")))
                                ),
                                "Device": df["Device"].apply(lambda d: d.value),
                            }
                        )
                        recs = out.to_dict("records")
                        # Apply daily cap
                        take = max(0, min(daily_cap - emitted, len(recs)))
                        if take > 0:
                            marketing_records.extend(recs[:take])
                            emitted += take
                        if emitted >= daily_cap:
                            logger.warning(
                                f"Marketing daily cap reached ({daily_cap}) on {date}. Truncating impressions."
                            )
                            break
                else:
                    logger.warning(
                        f"    No impressions generated for campaign {campaign_id}"
                    )
            except Exception as e:
                # Fallback to original loop
                logger.warning(f"Failed to process impressions via optimized path, using fallback: {e}")
                for impression in impressions:
                    logger.debug(
                        f"      Creating marketing record: {impression.get('channel', 'unknown')}"
                    )
                    customer_id = None
                    if self._rng.random() < 0.05:
                        customer_id = self._adid_to_customer_id.get(
                            impression.get("CustomerAdId")
                        )
                    marketing_records.append(
                        {
                            "TraceId": self._generate_trace_id(),
                            "EventTS": self._randomize_time_within_day(date),
                            "Channel": impression["Channel"].value,
                            "CampaignId": impression["CampaignId"],
                            "CreativeId": impression["CreativeId"],
                            "CustomerAdId": impression["CustomerAdId"],
                            "CustomerId": customer_id,
                            "ImpressionId": impression["ImpressionId"],
                            "Cost": str(impression["Cost"]),
                            "CostCents": int((impression["Cost"] * 100).quantize(Decimal("1"))),
                            "Device": impression["Device"].value,
                        }
                    )
                    emitted += 1
                    if emitted >= daily_cap:
                        logger.warning(
                            f"Marketing daily cap reached ({daily_cap}) on {date}. Truncating impressions."
                        )
                        break

            if emitted >= daily_cap:
                break

        logger.debug(
            f"_generate_marketing_activity complete: {len(marketing_records)} total"
        )
        return marketing_records

    # NOTE: _generate_hourly_store_activity was removed in favor of inline hour-by-hour
    # generation to reduce memory usage. The logic is now inlined in _generate_daily_facts
    # starting at line ~1015 to write each hour to the database immediately instead of
    # accumulating all 24 hours in memory first.


    def _compute_marketing_multiplier(self, date: datetime) -> float:
        # Conservative boosts; configurable later
        year = date.year
        tg = self._thanksgiving_date(year)
        bf = tg + timedelta(days=1)
        xmas = datetime(year, 12, 25)
        if self._in_window(date, tg, 10, 1):
            return 1.7
        if date.date() == bf.date():
            return 2.5
        if self._in_window(date, xmas, 14, 0):
            return 1.5
        # Grill weekends
        if self._in_window(date, self._memorial_day(year), 2, 2) or self._in_window(date, datetime(year,7,4), 2, 2) or self._in_window(date, self._labor_day(year), 2, 2):
            return 1.5
        return 1.0


