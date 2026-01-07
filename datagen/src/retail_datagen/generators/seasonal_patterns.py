"""
Seasonal and temporal patterns for realistic retail data generation.

This module provides classes to model temporal variations in retail behavior
including daily patterns, weekly cycles, seasonal effects, and holiday impacts.
"""

import math
import random
from datetime import datetime, time
from enum import Enum


class Season(Enum):
    """Seasonal categories for modeling."""

    SPRING = "SPRING"
    SUMMER = "SUMMER"
    FALL = "FALL"
    WINTER = "WINTER"


class HolidayType(Enum):
    """Types of holidays affecting retail patterns."""

    MAJOR = "MAJOR"  # Christmas, Black Friday, etc.
    MINOR = "MINOR"  # Memorial Day, Labor Day, etc.
    SEASONAL = "SEASONAL"  # Back-to-school, Valentine's Day, etc.


class SeasonalPatterns:
    """
    Models seasonal variations in retail activity.

    Provides multipliers for various metrics based on time of year,
    accounting for holidays, seasonal shopping patterns, and weather effects.
    """

    def __init__(self, seed: int = 42):
        """
        Initialize seasonal patterns.

        Args:
            seed: Random seed for reproducible pattern generation
        """
        self._rng = random.Random(seed)

        # Holiday calendar with impact multipliers
        self._holidays = self._build_holiday_calendar()

        # Base seasonal multipliers
        self._seasonal_multipliers = {
            Season.SPRING: 1.0,  # Baseline
            Season.SUMMER: 0.9,  # Slightly lower retail activity
            Season.FALL: 1.2,  # Back-to-school boost
            Season.WINTER: 1.4,  # Holiday shopping surge
        }

        # Category-specific seasonal effects
        self._category_seasonal_effects = {
            "clothing": {
                Season.SPRING: 1.1,
                Season.SUMMER: 0.8,
                Season.FALL: 1.3,  # Fall fashion
                Season.WINTER: 1.2,
            },
            "electronics": {
                Season.SPRING: 0.9,
                Season.SUMMER: 0.8,
                Season.FALL: 1.1,  # Back-to-school
                Season.WINTER: 1.5,  # Holiday gifts
            },
            "groceries": {
                Season.SPRING: 1.0,
                Season.SUMMER: 1.1,  # BBQ season
                Season.FALL: 1.0,
                Season.WINTER: 1.2,  # Holiday entertaining
            },
            "home_garden": {
                Season.SPRING: 1.4,  # Gardening season
                Season.SUMMER: 1.2,
                Season.FALL: 0.9,
                Season.WINTER: 0.6,
            },
        }

    def _build_holiday_calendar(
        self,
    ) -> dict[str, dict[str, str | HolidayType | float | int]]:
        """Build calendar of holidays with their retail impact."""
        holidays = {
            # Major retail holidays
            "2024-11-29": {
                "name": "Black Friday",
                "type": HolidayType.MAJOR,
                "multiplier": 3.5,
                "duration_days": 3,
            },
            "2024-12-25": {
                "name": "Christmas",
                "type": HolidayType.MAJOR,
                "multiplier": 2.0,
                "duration_days": 7,
            },
            "2024-12-26": {
                "name": "Boxing Day",
                "type": HolidayType.MAJOR,
                "multiplier": 2.5,
                "duration_days": 1,
            },
            "2024-11-11": {
                "name": "Veterans Day",
                "type": HolidayType.MINOR,
                "multiplier": 1.3,
                "duration_days": 1,
            },
            # Seasonal events
            "2024-08-15": {
                "name": "Back to School",
                "type": HolidayType.SEASONAL,
                "multiplier": 1.8,
                "duration_days": 14,
            },
            "2024-02-14": {
                "name": "Valentine's Day",
                "type": HolidayType.SEASONAL,
                "multiplier": 1.4,
                "duration_days": 3,
            },
            "2024-05-12": {
                "name": "Mother's Day",
                "type": HolidayType.SEASONAL,
                "multiplier": 1.6,
                "duration_days": 3,
            },
            "2024-06-16": {
                "name": "Father's Day",
                "type": HolidayType.SEASONAL,
                "multiplier": 1.3,
                "duration_days": 3,
            },
            # Minor holidays
            "2024-01-01": {
                "name": "New Year's Day",
                "type": HolidayType.MINOR,
                "multiplier": 0.5,
                "duration_days": 1,
            },
            "2024-07-04": {
                "name": "Independence Day",
                "type": HolidayType.MINOR,
                "multiplier": 1.2,
                "duration_days": 1,
            },
            "2024-05-27": {
                "name": "Memorial Day",
                "type": HolidayType.MINOR,
                "multiplier": 1.2,
                "duration_days": 1,
            },
            "2024-09-02": {
                "name": "Labor Day",
                "type": HolidayType.MINOR,
                "multiplier": 1.1,
                "duration_days": 1,
            },
            "2024-10-31": {
                "name": "Halloween",
                "type": HolidayType.SEASONAL,
                "multiplier": 1.3,
                "duration_days": 7,
            },
            "2024-11-28": {
                "name": "Thanksgiving",
                "type": HolidayType.MINOR,
                "multiplier": 0.8,
                "duration_days": 1,
            },
        }
        return holidays

    def get_season(self, date: datetime) -> Season:
        """
        Determine season for a given date.

        Args:
            date: Date to classify

        Returns:
            Season enum value
        """
        month = date.month

        if month in [3, 4, 5]:
            return Season.SPRING
        elif month in [6, 7, 8]:
            return Season.SUMMER
        elif month in [9, 10, 11]:
            return Season.FALL
        else:  # 12, 1, 2
            return Season.WINTER

    def get_seasonal_multiplier(
        self, date: datetime, category: str = "general"
    ) -> float:
        """
        Get seasonal multiplier for a given date and category.

        Args:
            date: Date to get multiplier for
            category: Product category (optional)

        Returns:
            Seasonal multiplier (1.0 = baseline)
        """
        season = self.get_season(date)

        # Get category-specific multiplier if available
        if category in self._category_seasonal_effects:
            multiplier = self._category_seasonal_effects[category][season]
        else:
            multiplier = self._seasonal_multipliers[season]

        # Apply holiday effects
        holiday_multiplier = self.get_holiday_multiplier(date)

        return multiplier * holiday_multiplier

    def get_holiday_multiplier(self, date: datetime) -> float:
        """
        Get holiday impact multiplier for a given date.

        Args:
            date: Date to check for holiday effects

        Returns:
            Holiday multiplier (1.0 = no effect)
        """
        date_str = date.strftime("%Y-%m-%d")

        # Check direct holiday match
        if date_str in self._holidays:
            return float(self._holidays[date_str]["multiplier"])

        # Check for holidays with multi-day effects
        for holiday_date_str, holiday_info in self._holidays.items():
            holiday_date = datetime.strptime(holiday_date_str, "%Y-%m-%d")
            duration = holiday_info.get("duration_days", 1)

            # Compare dates without timezone info to avoid naive/aware comparison issues
            date_naive = date.replace(tzinfo=None) if hasattr(date, 'tzinfo') and date.tzinfo else date
            # Check if current date falls within holiday effect window
            if abs((date_naive - holiday_date).days) <= duration:
                # Diminishing effect based on distance from holiday
                distance = abs((date_naive - holiday_date).days)
                decay_factor = max(0.3, 1.0 - (distance / duration) * 0.7)
                return 1.0 + (float(holiday_info["multiplier"]) - 1.0) * decay_factor

        return 1.0  # No holiday effect

    def get_week_of_year_multiplier(self, date: datetime) -> float:
        """
        Get multiplier based on week of year (accounts for seasonal progression).

        Args:
            date: Date to get multiplier for

        Returns:
            Week-based multiplier
        """
        week_of_year = date.isocalendar()[1]

        # Create sinusoidal pattern with peak in winter (weeks 47-52 and 1-8)
        # and trough in summer (weeks 20-35)
        angle = (week_of_year / 52.0) * 2 * math.pi

        # Shift so peak is at week 52/1 (holiday season)
        shifted_angle = angle - (51 / 52.0) * 2 * math.pi

        # Create multiplier between 0.8 and 1.4
        base_multiplier = 1.1 + 0.3 * math.sin(shifted_angle)

        return base_multiplier


class TemporalPatterns:
    """
    Models daily and weekly temporal patterns in retail activity.

    Provides multipliers for different times of day, days of week,
    and other temporal variations in shopping behavior.
    """

    def __init__(self, seed: int = 42):
        """
        Initialize temporal patterns.

        Args:
            seed: Random seed for reproducible patterns
        """
        self._rng = random.Random(seed)

        # Day of week multipliers (Monday = 0, Sunday = 6)
        self._day_of_week_multipliers = {
            0: 0.7,  # Monday - slow start
            1: 0.8,  # Tuesday - building up
            2: 0.9,  # Wednesday - mid-week
            3: 1.0,  # Thursday - baseline
            4: 1.2,  # Friday - weekend prep
            5: 1.4,  # Saturday - peak shopping
            6: 1.1,  # Sunday - moderate activity
        }

        # Hour of day patterns (different for weekdays vs weekends)
        self._weekday_hour_patterns = self._build_weekday_hour_patterns()
        self._weekend_hour_patterns = self._build_weekend_hour_patterns()

        # Store operating hours (typical retail hours)
        self._store_hours = {
            "open_time": time(8, 0),  # 8:00 AM
            "close_time": time(22, 0),  # 10:00 PM
            "peak_start": time(17, 0),  # 5:00 PM
            "peak_end": time(20, 0),  # 8:00 PM
            "lunch_start": time(11, 30),  # 11:30 AM
            "lunch_end": time(13, 30),  # 1:30 PM
        }

    def _build_weekday_hour_patterns(self) -> dict[int, float]:
        """Build hourly traffic patterns for weekdays."""
        patterns = {}

        for hour in range(24):
            if hour < 8:  # Early morning (before store open)
                multiplier = 0.0
            elif hour < 11:  # Morning (8-11 AM)
                multiplier = 0.3 + (hour - 8) * 0.1  # Gradual increase
            elif hour < 14:  # Lunch period (11 AM - 2 PM)
                multiplier = 0.8 + 0.4 * math.sin((hour - 11) * math.pi / 3)
            elif hour < 17:  # Afternoon (2-5 PM)
                multiplier = 0.7  # Steady moderate traffic
            elif hour < 20:  # Evening peak (5-8 PM)
                multiplier = 1.2 + 0.3 * math.sin((hour - 17) * math.pi / 3)
            elif hour < 22:  # Late evening (8-10 PM)
                multiplier = 0.9 - (hour - 20) * 0.2  # Decreasing
            else:  # After hours
                multiplier = 0.0

            patterns[hour] = multiplier

        return patterns

    def _build_weekend_hour_patterns(self) -> dict[int, float]:
        """Build hourly traffic patterns for weekends."""
        patterns = {}

        for hour in range(24):
            if hour < 9:  # Late start on weekends
                multiplier = 0.0
            elif hour < 12:  # Late morning (9 AM - 12 PM)
                multiplier = 0.4 + (hour - 9) * 0.2  # Gradual increase
            elif hour < 17:  # Afternoon (12-5 PM)
                multiplier = 1.0 + 0.3 * math.sin((hour - 12) * math.pi / 5)
            elif hour < 20:  # Early evening (5-8 PM)
                multiplier = 1.1 - (hour - 17) * 0.1  # Slight decrease
            elif hour < 22:  # Late evening (8-10 PM)
                multiplier = 0.8 - (hour - 20) * 0.2
            else:  # After hours
                multiplier = 0.0

            patterns[hour] = multiplier

        return patterns

    def get_day_of_week_multiplier(self, date: datetime) -> float:
        """
        Get multiplier based on day of week.

        Args:
            date: Date to get multiplier for

        Returns:
            Day of week multiplier
        """
        day_of_week = date.weekday()  # Monday = 0, Sunday = 6
        return self._day_of_week_multipliers[day_of_week]

    def get_hour_of_day_multiplier(self, dt: datetime) -> float:
        """
        Get multiplier based on hour of day.

        Args:
            dt: Datetime to get multiplier for

        Returns:
            Hour of day multiplier
        """
        hour = dt.hour
        is_weekend = dt.weekday() >= 5  # Saturday = 5, Sunday = 6

        if is_weekend:
            return self._weekend_hour_patterns.get(hour, 0.0)
        else:
            return self._weekday_hour_patterns.get(hour, 0.0)

    def is_store_open(self, dt: datetime) -> bool:
        """
        Check if store is open at given datetime.

        Args:
            dt: Datetime to check

        Returns:
            True if store is open
        """
        current_time = dt.time()
        return (
            self._store_hours["open_time"]
            <= current_time
            <= self._store_hours["close_time"]
        )

    def get_temporal_multiplier(self, dt: datetime) -> float:
        """
        Get combined temporal multiplier for a given datetime.

        Args:
            dt: Datetime to get multiplier for

        Returns:
            Combined temporal multiplier
        """
        if not self.is_store_open(dt):
            return 0.0

        day_multiplier = self.get_day_of_week_multiplier(dt)
        hour_multiplier = self.get_hour_of_day_multiplier(dt)

        return day_multiplier * hour_multiplier

    def get_peak_shopping_hours(self, date: datetime) -> list[tuple[time, time]]:
        """
        Get peak shopping hours for a given date.

        Args:
            date: Date to get peak hours for

        Returns:
            List of (start_time, end_time) tuples for peak periods
        """
        is_weekend = date.weekday() >= 5

        if is_weekend:
            return [(time(12, 0), time(17, 0))]  # Weekend afternoon peak
        else:
            return [
                (
                    self._store_hours["lunch_start"],
                    self._store_hours["lunch_end"],
                ),  # Lunch
                (
                    self._store_hours["peak_start"],
                    self._store_hours["peak_end"],
                ),  # Evening
            ]


class EventPatterns:
    """
    Models special event patterns that affect retail activity.

    Handles weather effects, local events, promotional periods,
    and other non-regular patterns that impact shopping behavior.
    """

    def __init__(self, seed: int = 42):
        """
        Initialize event patterns.

        Args:
            seed: Random seed for reproducible events
        """
        self._rng = random.Random(seed)

        # Weather impact multipliers
        self._weather_multipliers = {
            "sunny": 1.1,
            "cloudy": 1.0,
            "rainy": 0.7,
            "snowy": 0.6,
            "stormy": 0.5,
        }

        # Promotional event types and their impacts
        self._promo_types = {
            "flash_sale": {"duration_hours": 6, "multiplier": 2.0, "probability": 0.02},
            "weekend_sale": {
                "duration_hours": 48,
                "multiplier": 1.5,
                "probability": 0.1,
            },
            "clearance": {
                "duration_hours": 168,
                "multiplier": 1.3,
                "probability": 0.05,
            },
            "seasonal_sale": {
                "duration_hours": 336,
                "multiplier": 1.4,
                "probability": 0.03,
            },
        }

    def simulate_weather_effect(self, date: datetime) -> float:
        """
        Simulate weather impact on shopping patterns.

        Args:
            date: Date to simulate weather for

        Returns:
            Weather multiplier
        """
        # Simple weather simulation based on season and randomness
        season = SeasonalPatterns().get_season(date)

        # Base weather probabilities by season
        weather_probs = {
            Season.SPRING: {"sunny": 0.4, "cloudy": 0.3, "rainy": 0.3},
            Season.SUMMER: {"sunny": 0.6, "cloudy": 0.3, "rainy": 0.1},
            Season.FALL: {"sunny": 0.3, "cloudy": 0.4, "rainy": 0.2, "snowy": 0.1},
            Season.WINTER: {
                "sunny": 0.2,
                "cloudy": 0.3,
                "rainy": 0.1,
                "snowy": 0.3,
                "stormy": 0.1,
            },
        }

        # Select weather based on probabilities
        weather_options = list(weather_probs[season].keys())
        weather_weights = list(weather_probs[season].values())

        weather = self._rng.choices(weather_options, weights=weather_weights)[0]

        return self._weather_multipliers.get(weather, 1.0)

    def check_promotional_events(self, dt: datetime) -> float:
        """
        Check for promotional events and return impact multiplier.

        Args:
            dt: Datetime to check for promotions

        Returns:
            Promotional event multiplier
        """
        # Use datetime as seed for consistent promotion schedule
        event_rng = random.Random(int(dt.timestamp() // 3600))  # Hourly consistency

        # Check each promotion type
        for promo_type, promo_info in self._promo_types.items():
            if event_rng.random() < promo_info["probability"]:
                return promo_info["multiplier"]

        return 1.0  # No promotional effect

    def get_event_multiplier(self, dt: datetime) -> float:
        """
        Get combined event multiplier for a given datetime.

        Args:
            dt: Datetime to get multiplier for

        Returns:
            Combined event multiplier
        """
        weather_multiplier = self.simulate_weather_effect(dt)
        promo_multiplier = self.check_promotional_events(dt)

        # Events combine multiplicatively but cap at reasonable limits
        combined = weather_multiplier * promo_multiplier
        return min(combined, 3.0)  # Cap at 3x normal activity


class CompositeTemporalPatterns:
    """
    Combines all temporal patterns into a unified interface.

    Provides a single method to get overall temporal multiplier
    considering all seasonal, daily, weekly, and event effects.
    """

    def __init__(self, seed: int = 42):
        """
        Initialize composite temporal patterns.

        Args:
            seed: Random seed for reproducible patterns
        """
        self.seasonal = SeasonalPatterns(seed)
        self.temporal = TemporalPatterns(seed + 100)
        self.events = EventPatterns(seed + 200)

    def get_overall_multiplier(self, dt: datetime, category: str = "general") -> float:
        """
        Get overall temporal multiplier combining all effects.

        Args:
            dt: Datetime to get multiplier for
            category: Product category for seasonal adjustments

        Returns:
            Overall temporal multiplier
        """
        seasonal_mult = self.seasonal.get_seasonal_multiplier(dt, category)
        temporal_mult = self.temporal.get_temporal_multiplier(dt)
        event_mult = self.events.get_event_multiplier(dt)

        # If store is closed, return 0
        if not self.temporal.is_store_open(dt):
            return 0.0

        # Combine all multipliers
        overall = seasonal_mult * temporal_mult * event_mult

        # Apply reasonable bounds
        return max(0.0, min(overall, 5.0))

    def get_traffic_pattern(self, dt: datetime) -> dict[str, float]:
        """
        Get detailed breakdown of traffic patterns.

        Args:
            dt: Datetime to analyze

        Returns:
            Dictionary with pattern breakdown
        """
        return {
            "seasonal": self.seasonal.get_seasonal_multiplier(dt),
            "day_of_week": self.temporal.get_day_of_week_multiplier(dt),
            "hour_of_day": self.temporal.get_hour_of_day_multiplier(dt),
            "holiday": self.seasonal.get_holiday_multiplier(dt),
            "weather": self.events.simulate_weather_effect(dt),
            "promotional": self.events.check_promotional_events(dt),
            "overall": self.get_overall_multiplier(dt),
            "store_open": self.temporal.is_store_open(dt),
        }
