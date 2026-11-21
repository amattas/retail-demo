"""Shared retail vocabularies and identifier formats.

Historical generation and the live stream must agree on these names and
formats. Batch modules import them from here, and ``build_notebooks.py``
inlines this module into the stream notebook, so both producers read one
definition.

The module holds:

- payment tenders, their mix, and the decline model
- the named promotion catalog and the evergreen fallback codes
- identifier formats: BLE IDs, ad IDs, beacon and sensor IDs, campaign IDs
- zone vocabularies (BLE zones and foot-traffic zones)
- marketing channels, their cost-per-impression bands, and device types
- online fulfillment modes

The live stream does not use every definition yet. The remaining differences
are listed, with their review IDs, in the known-gap allowlist of
``tests/generation/test_vocabulary.py``.
"""

from datetime import date

from pyspark.sql import Column
from pyspark.sql import functions as F

from retail_setup.dictionaries.models import StoreTypeProfile
from retail_setup.generation.runtime import (
    fixed_width_decimal,
    minimum_width_decimal,
    minimum_width_text,
)

# ---------------------------------------------------------------------------
# Payments
# ---------------------------------------------------------------------------

# In-store tenders: (method, mix weight, decline multiplier, processing_ms lo,
# processing_ms hi). Weights are relative.
TENDERS: list[tuple[str, float, float, int, int]] = [
    ("CREDIT_CARD", 0.4, 1.0, 1500, 4000),
    ("DEBIT_CARD", 0.3, 0.8, 1200, 3500),
    ("CASH", 0.2, 0.0, 500, 2000),
    ("MOBILE_PAY", 0.1, 1.2, 800, 2500),
]

# Online tenders (no cash): 60% credit / 25% debit / 10% PayPal / 5% other.
ONLINE_TENDERS: list[tuple[str, float, float, int, int]] = [
    ("CREDIT_CARD", 0.60, 1.0, 1500, 4000),
    ("DEBIT_CARD", 0.25, 0.8, 1200, 3500),
    ("PAYPAL", 0.10, 1.1, 2000, 5000),
    ("OTHER", 0.05, 0.5, 1500, 4000),
]

# Decline model: P(decline) = BASE_DECLINE x the tender's decline multiplier.
BASE_DECLINE = 0.025
DECLINE_REASONS = [
    "INSUFFICIENT_FUNDS",
    "CARD_EXPIRED",
    "INVALID_CVV",
    "NETWORK_ERROR",
    "FRAUD_SUSPECTED",
    "CARD_BLOCKED",
    "LIMIT_EXCEEDED",
]


def tender_names(tenders: list[tuple[str, float, float, int, int]] = TENDERS) -> list[str]:
    """Tender method names in table order."""
    return [name for name, *_ in tenders]


def decline_probability(
    method: str, tenders: list[tuple[str, float, float, int, int]] = TENDERS
) -> float:
    """Probability that a payment with ``method`` is declined."""
    for name, _, multiplier, _, _ in tenders:
        if name == method:
            return BASE_DECLINE * multiplier
    raise ValueError(f"unknown tender {method!r}")


# ---------------------------------------------------------------------------
# Promotions
# ---------------------------------------------------------------------------

# Named promo catalog. Seasonal codes apply only in their eligible months.
# Some codes carry a minimum line value; BOGO discounts every second unit and
# needs a quantity of at least two.
# (code, discount_pct, eligible_months | None, min_cents, kind)
PROMO_CATALOG: list[tuple[str, int, list[int] | None, int, str]] = [
    ("SAVE10", 10, None, 0, "PCT"),
    ("SAVE15", 15, None, 2500, "PCT"),  # min $25
    ("SAVE20", 20, None, 5000, "PCT"),  # min $50
    ("SAVE25", 25, None, 7500, "PCT"),  # min $75
    ("CLEARANCE30", 30, None, 0, "PCT"),
    ("BOGO50", 50, None, 0, "BOGO"),  # buy one get one 50% off (qty >= 2)
    ("NEWYEAR15", 15, [1, 2], 0, "PCT"),
    ("SPRINGSALE20", 20, [3, 4, 5], 0, "PCT"),
    ("SUMMER25", 25, [6, 7, 8], 0, "PCT"),
    ("BACKTOSCHOOL20", 20, [8, 9], 0, "PCT"),
    ("BFRIDAY30", 30, [11], 0, "PCT"),
    ("BFRIDAY40", 40, [11], 10000, "PCT"),  # min $100
    ("HOLIDAY20", 20, [12], 0, "PCT"),
]
# Evergreen fallback when the drawn catalog code is not eligible: always
# eligible, no minimum, percentage only.
EVERGREEN_PROMOS: list[tuple[str, int]] = [
    ("SAVE10", 10),
    ("CLEARANCE30", 30),
    ("DEAL15", 15),
]


def promo_eligible(index: int, month: int, line_cents: int, quantity: int) -> bool:
    """Per-line eligibility of ``PROMO_CATALOG[index]``.

    The code must be in season, the line value before discount must meet any
    minimum, and BOGO needs at least two units.
    """
    _, _, months, min_cents, kind = PROMO_CATALOG[index]
    if months is not None and month not in months:
        return False
    if line_cents < min_cents:
        return False
    return kind != "BOGO" or quantity >= 2


def promo_eligible_column(
    index: Column, month: Column, line_cents: Column, quantity: Column
) -> Column:
    """Column form of :func:`promo_eligible`."""
    expr: Column | None = None
    for i, (_, _, months, min_cents, kind) in enumerate(PROMO_CATALOG):
        eligible = F.lit(True) if months is None else month.isin(*months)
        if min_cents:
            eligible = eligible & (line_cents >= F.lit(min_cents))
        if kind == "BOGO":
            eligible = eligible & (quantity >= F.lit(2))
        condition = index == F.lit(i)
        expr = F.when(condition, eligible) if expr is None else expr.when(condition, eligible)
    assert expr is not None
    return expr.otherwise(F.lit(False))


# ---------------------------------------------------------------------------
# Identifier formats
# ---------------------------------------------------------------------------

BLE_ID_PREFIX = "BLE"
BLE_ID_BASE = 36
BLE_ID_WIDTH = 6
AD_ID_PREFIX = "AD"
AD_ID_WIDTH = 8
BEACON_ID_FORMAT = "BEACON_%03d_%s"
SENSOR_ID_FORMAT = "SENSOR_%03d_%s"
CAMPAIGN_ID_PREFIX = "CAMP"
CAMPAIGN_INDEX_WIDTH = 2

_BASE36 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def ble_id(customer_id: int) -> str:
    """BLE device ID of a customer: ``BLE`` + upper-case base-36 ID, padded to 6."""
    if customer_id < 0:
        raise ValueError("customer_id must be non-negative")
    digits = ""
    value = customer_id
    while True:
        value, remainder = divmod(value, BLE_ID_BASE)
        digits = _BASE36[remainder] + digits
        if value == 0:
            break
    return BLE_ID_PREFIX + digits.rjust(BLE_ID_WIDTH, "0")


def ble_id_column(customer_id: Column) -> Column:
    """Column form of :func:`ble_id`."""
    digits = F.upper(F.conv(customer_id.cast("string"), 10, BLE_ID_BASE))
    return F.concat(F.lit(BLE_ID_PREFIX), minimum_width_text(digits, BLE_ID_WIDTH))


def ad_id(customer_id: int) -> str:
    """Advertising ID of a customer: ``AD`` + decimal ID, padded to 8 digits."""
    return AD_ID_PREFIX + str(customer_id).rjust(AD_ID_WIDTH, "0")


def ad_id_column(customer_id: Column) -> Column:
    """Column form of :func:`ad_id`."""
    return F.concat(F.lit(AD_ID_PREFIX), minimum_width_decimal(customer_id, AD_ID_WIDTH))


def beacon_id(store_id: int, zone: str) -> str:
    return BEACON_ID_FORMAT % (store_id, zone)


def beacon_id_column(store_id: Column, zone: Column) -> Column:
    return F.format_string(BEACON_ID_FORMAT, store_id, zone)


def sensor_id(store_id: int, zone: str) -> str:
    return SENSOR_ID_FORMAT % (store_id, zone)


def sensor_id_column(store_id: Column, zone: Column) -> Column:
    return F.format_string(SENSOR_ID_FORMAT, store_id, zone)


def campaign_id(campaign_start: date, archetype_index: int) -> str:
    """Campaign ID: ``CAMP`` + window start (yyyyMMdd) + 2-digit archetype index."""
    if not 0 <= archetype_index < 10**CAMPAIGN_INDEX_WIDTH:
        raise ValueError("archetype_index exceeds 2 digits")
    return f"{CAMPAIGN_ID_PREFIX}{campaign_start:%Y%m%d}{archetype_index:02d}"


def campaign_id_column(campaign_start: Column, archetype_index: Column) -> Column:
    """Column form of :func:`campaign_id`."""
    return F.concat(
        F.lit(CAMPAIGN_ID_PREFIX),
        F.date_format(campaign_start, "yyyyMMdd"),
        fixed_width_decimal(archetype_index, CAMPAIGN_INDEX_WIDTH, "campaign archetype index"),
    )


# ---------------------------------------------------------------------------
# Zones
# ---------------------------------------------------------------------------

# BLE beacon zones a shopper's phone is seen in.
BLE_ZONES = ["ENTRANCE", "ELECTRONICS", "GROCERY", "CLOTHING", "CHECKOUT"]

# Foot-traffic sensor zones -> (dwell_lo_seconds, dwell_hi_seconds).
FOOT_TRAFFIC_ZONE_DWELL: dict[str, tuple[int, int]] = {
    "ENTRANCE_MAIN": (45, 90),
    "ENTRANCE_SIDE": (30, 75),
    "AISLES_A": (180, 420),
    "AISLES_B": (120, 300),
    "CHECKOUT": (90, 240),
}
FOOT_TRAFFIC_ZONES = list(FOOT_TRAFFIC_ZONE_DWELL)


def ble_zones_for_profile(profile: StoreTypeProfile | None) -> list[str]:
    """BLE zones for a store type: the profile's ``zones`` when it declares any.

    Profile zone names are normalized to upper case so they match the rest of
    the zone vocabulary. Without a profile the default ``BLE_ZONES`` apply.
    Generators switch to this helper when profile-driven zones land (B-7,
    B-18); until then they use ``BLE_ZONES`` directly.
    """
    if profile is None or not profile.zones:
        return list(BLE_ZONES)
    return list(dict.fromkeys(zone.strip().upper() for zone in profile.zones if zone.strip()))


# ---------------------------------------------------------------------------
# Marketing
# ---------------------------------------------------------------------------

# (archetype, channels, base impressions/day at the legacy 86-store fleet,
#  campaign duration in days). The archetype index in campaign IDs is the
#  1-based position in this list.
CAMPAIGN_ARCHETYPES: list[tuple[str, list[str], int, int]] = [
    ("seasonal_sale", ["FACEBOOK", "GOOGLE", "EMAIL"], 1000, 7),
    ("product_launch", ["INSTAGRAM", "YOUTUBE", "DISPLAY"], 2000, 14),
    ("loyalty_program", ["EMAIL", "SOCIAL"], 500, 30),
    ("flash_sale", ["SOCIAL", "SEARCH"], 5000, 1),  # runs ~1 day in 7
]

# channel -> (lo, hi) cost-per-impression band in dollars (uniform draw)
CHANNEL_COSTS: dict[str, tuple[float, float]] = {
    "EMAIL": (0.005, 0.05),
    "DISPLAY": (0.10, 0.50),
    "SOCIAL": (0.08, 0.25),
    "SEARCH": (0.15, 0.75),
    "FACEBOOK": (0.10, 0.40),
    "GOOGLE": (0.25, 1.50),
    "INSTAGRAM": (0.08, 0.35),
    "YOUTUBE": (0.30, 1.50),
}
MARKETING_CHANNELS = list(CHANNEL_COSTS)

# Device types are upper case; cost multipliers scale the channel band.
DEVICE_WEIGHTS: list[tuple[str, float]] = [("MOBILE", 0.6), ("DESKTOP", 0.3), ("TABLET", 0.1)]
DEVICE_MULTIPLIERS: dict[str, float] = {"MOBILE": 1.2, "DESKTOP": 0.8, "TABLET": 0.9}
DEVICE_TYPES = [device for device, _ in DEVICE_WEIGHTS]

# ---------------------------------------------------------------------------
# Online fulfillment
# ---------------------------------------------------------------------------

ONLINE_FULFILLMENT_MIX: list[tuple[str, float]] = [
    ("SHIP_FROM_DC", 0.60),
    ("SHIP_FROM_STORE", 0.30),
    ("BOPIS", 0.10),
]
FULFILLMENT_MODES = [mode for mode, _ in ONLINE_FULFILLMENT_MIX]
