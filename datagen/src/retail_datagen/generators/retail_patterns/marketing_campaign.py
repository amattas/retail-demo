"""
Marketing campaign simulation for realistic customer acquisition and engagement.

This module models different marketing channels, campaign targeting, and
impression generation with realistic costs and conversion patterns.
"""

import logging
import random
from datetime import datetime, timedelta
from decimal import Decimal

from retail_datagen.config.models import MarketingCostConfig
from retail_datagen.shared.models import Customer, DeviceType, MarketingChannel

from .common import CAMPAIGN_START_PROBABILITY, DEFAULT_MIN_DAILY_IMPRESSIONS

logger = logging.getLogger(__name__)


class MarketingCampaignSimulator:
    """
    Simulates realistic marketing campaigns and customer interactions.

    Models different marketing channels, campaign targeting, and
    impression generation with realistic costs and conversion patterns.
    """

    def __init__(
        self,
        customers: list[Customer],
        seed: int = 42,
        cost_config: MarketingCostConfig | None = None,
    ):
        """
        Initialize marketing campaign simulator.

        Args:
            customers: List of customer dimension records
            seed: Random seed for reproducible campaigns
            cost_config: Marketing cost configuration (uses defaults if None)
        """
        self.customers = customers
        self._rng = random.Random(seed)
        self._impression_counter = 0  # Counter for unique impression IDs
        self.cost_config = cost_config or MarketingCostConfig()

        # Campaign types and their characteristics
        self._campaign_types = {
            "seasonal_sale": {
                "channels": [
                    MarketingChannel.FACEBOOK,
                    MarketingChannel.GOOGLE,
                    MarketingChannel.EMAIL,
                ],
                "duration_days": 7,
                "daily_impressions": 1000,
                "cost_per_impression": Decimal("0.15"),
                "target_segments": ["budget_conscious", "convenience_focused"],
            },
            "product_launch": {
                "channels": [
                    MarketingChannel.INSTAGRAM,
                    MarketingChannel.YOUTUBE,
                    MarketingChannel.DISPLAY,
                ],
                "duration_days": 14,
                "daily_impressions": 2000,
                "cost_per_impression": Decimal("0.25"),
                "target_segments": ["quality_seeker", "brand_loyal"],
            },
            "loyalty_program": {
                "channels": [MarketingChannel.EMAIL, MarketingChannel.SOCIAL],
                "duration_days": 30,
                "daily_impressions": 500,
                "cost_per_impression": Decimal("0.05"),
                "target_segments": ["brand_loyal", "quality_seeker"],
            },
            "flash_sale": {
                "channels": [MarketingChannel.SOCIAL, MarketingChannel.SEARCH],
                "duration_days": 1,
                "daily_impressions": 5000,
                "cost_per_impression": Decimal("0.30"),
                "target_segments": ["budget_conscious", "convenience_focused"],
            },
        }

        # Device distribution for impressions
        self._device_distribution = {
            DeviceType.MOBILE: 0.6,
            DeviceType.DESKTOP: 0.3,
            DeviceType.TABLET: 0.1,
        }

        # Active campaigns tracking
        self._active_campaigns: dict[str, dict] = {}
        self._campaign_counter = 1

    def calculate_impression_cost(
        self, channel: MarketingChannel, device: DeviceType
    ) -> Decimal:
        """
        Calculate cost for a single impression based on channel and device.

        Args:
            channel: Marketing channel for the impression
            device: Device type for the impression

        Returns:
            Calculated cost as Decimal
        """
        # Get channel cost range based on channel type
        channel_cost_ranges = {
            MarketingChannel.EMAIL: (
                self.cost_config.email_cost_min,
                self.cost_config.email_cost_max,
            ),
            MarketingChannel.DISPLAY: (
                self.cost_config.display_cost_min,
                self.cost_config.display_cost_max,
            ),
            MarketingChannel.SOCIAL: (
                self.cost_config.social_cost_min,
                self.cost_config.social_cost_max,
            ),
            MarketingChannel.SEARCH: (
                self.cost_config.search_cost_min,
                self.cost_config.search_cost_max,
            ),
            MarketingChannel.VIDEO: (
                self.cost_config.video_cost_min,
                self.cost_config.video_cost_max,
            ),
            MarketingChannel.FACEBOOK: (
                self.cost_config.facebook_cost_min,
                self.cost_config.facebook_cost_max,
            ),
            MarketingChannel.GOOGLE: (
                self.cost_config.google_cost_min,
                self.cost_config.google_cost_max,
            ),
            MarketingChannel.INSTAGRAM: (
                self.cost_config.instagram_cost_min,
                self.cost_config.instagram_cost_max,
            ),
            MarketingChannel.YOUTUBE: (
                self.cost_config.youtube_cost_min,
                self.cost_config.youtube_cost_max,
            ),
        }

        # Get device multiplier based on device type
        device_multipliers = {
            DeviceType.MOBILE: self.cost_config.mobile_multiplier,
            DeviceType.TABLET: self.cost_config.tablet_multiplier,
            DeviceType.DESKTOP: self.cost_config.desktop_multiplier,
        }

        # Calculate base cost from channel range
        cost_min, cost_max = channel_cost_ranges.get(
            channel,
            (0.25, 0.25),  # Default fallback
        )
        base_cost = self._rng.uniform(cost_min, cost_max)

        # Apply device multiplier
        device_multiplier = device_multipliers.get(device, 1.0)
        final_cost = base_cost * device_multiplier

        # Return as Decimal with 4 decimal places for precision
        return Decimal(str(round(final_cost, 4)))

    def start_campaign(self, campaign_type: str, start_date: datetime) -> str:
        """
        Start a new marketing campaign.

        Args:
            campaign_type: Type of campaign to start
            start_date: Campaign start date

        Returns:
            Campaign ID
        """
        if campaign_type not in self._campaign_types:
            raise ValueError(f"Unknown campaign type: {campaign_type}")

        campaign_id = f"CAMP{start_date.strftime('%Y%m%d')}{self._campaign_counter:04d}"
        self._campaign_counter += 1

        campaign_config = self._campaign_types[campaign_type].copy()
        campaign_info = {
            "campaign_id": campaign_id,
            "type": campaign_type,
            "start_date": start_date,
            "end_date": start_date
            + timedelta(days=campaign_config["duration_days"] - 1),
            "config": campaign_config,
            "total_impressions": 0,
            "total_cost": Decimal("0"),
        }

        self._active_campaigns[campaign_id] = campaign_info
        return campaign_id

    def generate_campaign_impressions(
        self, campaign_id: str, date: datetime, traffic_multiplier: float = 1.0
    ) -> list[dict]:
        """
        Generate marketing impressions for a campaign on a given date.

        Args:
            campaign_id: Campaign to generate impressions for
            date: Date to generate impressions
            traffic_multiplier: Multiplier for impression volume

        Returns:
            List of marketing impression records
        """
        # Use .get() to safely handle race conditions in parallel processing
        # Campaign may be removed by another thread between check and access
        campaign = self._active_campaigns.get(campaign_id)
        if campaign is None:
            return []

        # Check if campaign is active on this date
        # Convert datetime to date for comparison if needed
        date_to_check = date.date() if isinstance(date, datetime) else date
        campaign_start = (
            campaign["start_date"].date()
            if isinstance(campaign["start_date"], datetime)
            else campaign["start_date"]
        )
        campaign_end = (
            campaign["end_date"].date()
            if isinstance(campaign["end_date"], datetime)
            else campaign["end_date"]
        )

        if not (campaign_start <= date_to_check <= campaign_end):
            return []

        config = campaign["config"]
        impressions = []

        logger.info(f"      Generating impressions for campaign {campaign_id}")
        logger.info(
            f"        Config: daily_impressions={config.get('daily_impressions', 'MISSING')}, channels={len(config.get('channels', []))}"
        )
        logger.info(f"        Traffic multiplier: {traffic_multiplier}")

        # Calculate base impressions from traffic
        base_impressions_from_traffic = config["daily_impressions"] * traffic_multiplier
        min_daily_impressions = config.get(
            "min_daily_impressions", DEFAULT_MIN_DAILY_IMPRESSIONS
        )
        num_channels = len(config["channels"])

        # Store closure (traffic_multiplier=0) results in zero impressions
        # Otherwise enforce minimum threshold to prevent rounding to zero
        if traffic_multiplier == 0:
            base_impressions = 0
        else:
            # Ensure minimum TOTAL impressions across all channels (account for integer division)
            # Multiply minimum by channels to ensure we meet threshold after division
            min_impressions_total = min_daily_impressions * num_channels
            base_impressions = max(
                min_impressions_total, int(base_impressions_from_traffic)
            )

        logger.info(f"        Calculated base_impressions: {base_impressions}")
        logger.info(f"        Number of channels: {len(config['channels'])}")

        # Generate impressions across different channels
        logger.info(
            f"        Starting channel loop with {len(config['channels'])} channels"
        )
        for channel in config["channels"]:
            channel_impressions = base_impressions // len(config["channels"])
            logger.info(
                f"          Channel {channel}: {channel_impressions} impressions to generate"
            )

            for _ in range(channel_impressions):
                # Select target customer
                if not self.customers:
                    logger.error(
                        "          ERROR: No customers available for targeting!"
                    )
                    break

                target_customer = self._rng.choice(self.customers)

                # Select device type
                device_options = list(self._device_distribution.keys())
                device_weights = list(self._device_distribution.values())
                device = self._rng.choices(device_options, weights=device_weights)[0]

                # Generate creative ID
                creative_id = f"CREAT{campaign_id[-4:]}{channel.value[:3]}{self._rng.randint(1, 99):02d}"

                # Generate unique impression ID using counter
                self._impression_counter += 1
                impression_id = f"IMP{self._impression_counter:010d}"

                # Calculate cost based on channel and device
                impression_cost = self.calculate_impression_cost(channel, device)

                impression = {
                    "Channel": channel,
                    "CampaignId": campaign_id,
                    "CreativeId": creative_id,
                    "CustomerAdId": target_customer.AdId,
                    "ImpressionId": impression_id,
                    "Cost": impression_cost,
                    "Device": device,
                    "EventTS": date,
                }

                impressions.append(impression)

                if len(impressions) % 100 == 0:
                    logger.debug(
                        f"          Generated {len(impressions)} impressions so far"
                    )

        # Update campaign totals
        campaign["total_impressions"] += len(impressions)
        campaign["total_cost"] += sum(imp["Cost"] for imp in impressions)

        logger.info(f"        Total impressions generated: {len(impressions)}")
        return impressions

    def should_start_campaign(
        self, date: datetime, traffic_multiplier: float = 1.0
    ) -> str | None:
        """
        Determine if a new campaign should start on a given date.

        Args:
            date: Date to check for new campaigns
            traffic_multiplier: Current traffic patterns

        Returns:
            Campaign type to start or None
        """
        # 90% of days should have active campaigns
        campaign_probability = CAMPAIGN_START_PROBABILITY

        if self._rng.random() < campaign_probability:
            # Select campaign type based on seasonality and current context
            available_types = list(self._campaign_types.keys())

            # Seasonal preferences
            if date.month in [11, 12]:  # Holiday season
                weights = [3, 1, 2, 4]  # Prefer seasonal sales and flash sales
            elif date.month in [8, 9]:  # Back to school
                weights = [2, 3, 1, 2]  # Prefer product launches
            else:
                weights = [1, 1, 1, 1]  # Equal weights

            return self._rng.choices(available_types, weights=weights)[0]

        return None
