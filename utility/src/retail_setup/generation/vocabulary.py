"""Shared retail vocabularies and identifier formats.

Historical generation and the live stream must agree on these names and
formats. Batch modules import them from here, and ``build_notebooks.py``
inlines this module into the stream notebook, so both producers read one
definition.

The module holds:

- payment tenders, their mix, and the decline model (decline, then a retry on
  another tender or a voided sale)
- the named promotion catalog
- identifier formats: BLE IDs, ad IDs, beacon and sensor IDs, campaign IDs
- zone vocabularies and the visit path through a store type's zones
- marketing channels, their cost-per-impression bands, and device types
- online fulfillment modes

The live stream does not use every definition yet. The remaining differences
are listed, with their review IDs, in the known-gap allowlist of
``tests/generation/test_vocabulary.py``.

Column helpers here (tender, decline and BLE draws) are the single
implementation both producers call, keyed on each producer's own IDs.
"""

from dataclasses import dataclass
from datetime import date

from pyspark.sql import Column
from pyspark.sql import functions as F

from retail_setup.dictionaries.models import StoreTypeProfile
from retail_setup.generation.runtime import (
    case_when,
    fixed_width_decimal,
    minimum_width_decimal,
    minimum_width_text,
    seeded_draws,
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


def tender_column(
    draws: seeded_draws,
    keys: list,
    tenders: list[tuple[str, float, float, int, int]] = TENDERS,
    salt: str = "tender",
) -> Column:
    """Tender drawn by the mix weights of ``tenders``."""
    return draws.pick_by_weights(keys, salt, [(name, weight) for name, weight, *_ in tenders])


def decline_probability_column(
    method: Column, tenders: list[tuple[str, float, float, int, int]] = TENDERS
) -> Column:
    """Column form of :func:`decline_probability` (0 for an unknown method)."""
    return case_when(
        (method == name, F.lit(BASE_DECLINE * multiplier)) for name, _, multiplier, _, _ in tenders
    ).otherwise(F.lit(0.0))


def payment_status_column(
    draws: seeded_draws,
    keys: list,
    method: Column,
    tenders: list[tuple[str, float, float, int, int]] = TENDERS,
    salt: str = "decline",
) -> Column:
    """``DECLINED`` with the tender's decline probability, else ``APPROVED``."""
    declined = draws.u(keys, salt) < decline_probability_column(method, tenders)
    return F.when(declined, F.lit("DECLINED")).otherwise(F.lit("APPROVED"))


# A declined shopper retries with another tender DECLINE_RETRY_SHARE of the
# time (the retry is approved); otherwise the sale is voided and never happens.
DECLINE_RETRY_SHARE = 0.8
APPROVED, RETRIED, VOIDED = "APPROVED", "RETRIED", "VOIDED"


def payment_outcome_column(
    draws: seeded_draws,
    keys: list,
    method: Column,
    tenders: list[tuple[str, float, float, int, int]] = TENDERS,
    salt: str = "decline",
) -> Column:
    """``APPROVED``, ``RETRIED`` (declined, then approved on a second tender) or ``VOIDED``.

    The first attempt declines with the tender's probability (the same draw as
    :func:`payment_status_column`); a declined sale is retried
    ``DECLINE_RETRY_SHARE`` of the time and voided otherwise.
    """
    declined = draws.u(keys, salt) < decline_probability_column(method, tenders)
    retried = draws.u(keys, f"{salt}-retry") < F.lit(DECLINE_RETRY_SHARE)
    return F.when(~declined, F.lit(APPROVED)).when(retried, F.lit(RETRIED)).otherwise(F.lit(VOIDED))


def retry_tender_column(
    draws: seeded_draws,
    keys: list,
    declined: Column,
    tenders: list[tuple[str, float, float, int, int]] = TENDERS,
    salt: str = "retry-tender",
) -> Column:
    """The second tender after ``declined``: drawn by the mix of the other tenders."""
    return case_when(
        (
            declined == name,
            draws.pick_by_weights(
                keys, salt, [(other, weight) for other, weight, *_ in tenders if other != name]
            ),
        )
        for name, *_ in tenders
    )


def outcome_shares(
    tenders: list[tuple[str, float, float, int, int]] = TENDERS,
) -> dict[str, float]:
    """Expected share of sales by payment outcome for a tender mix."""
    total = sum(weight for _, weight, *_ in tenders)
    declined = sum(weight * BASE_DECLINE * mult for _, weight, mult, _, _ in tenders) / total
    return {
        APPROVED: 1.0 - declined,
        RETRIED: declined * DECLINE_RETRY_SHARE,
        VOIDED: declined * (1.0 - DECLINE_RETRY_SHARE),
    }


def transaction_id_column(prefix: str, purchase_id: Column, attempt: Column | int) -> Column:
    """Payment transaction ID, unique by construction: ``<prefix>-<purchase>-<attempt>``."""
    attempt_col = attempt if isinstance(attempt, Column) else F.lit(attempt)
    return F.concat(F.lit(f"{prefix}-"), purchase_id, F.lit("-"), attempt_col.cast("string"))


def decline_reason_column(
    draws: seeded_draws, keys: list, status: Column, salt: str = "reason"
) -> Column:
    """A decline reason for declined payments, NULL otherwise."""
    index = (draws.h64(keys, salt) % len(DECLINE_REASONS)).cast("int")
    return F.when(
        status == "DECLINED",
        F.element_at(F.array(*[F.lit(reason) for reason in DECLINE_REASONS]), index + 1),
    )


def processing_ms_column(
    draws: seeded_draws,
    keys: list,
    method: Column,
    tenders: list[tuple[str, float, float, int, int]] = TENDERS,
    salt: str = "proc",
) -> Column:
    """Payment processing time in ms, uniform in the tender's range."""
    low: Column = F.lit(tenders[-1][3])
    high: Column = F.lit(tenders[-1][4])
    for name, _, _, lo, hi in tenders[:-1]:
        low = F.when(method == name, F.lit(lo)).otherwise(low)
        high = F.when(method == name, F.lit(hi)).otherwise(high)
    return (low + draws.u(keys, salt) * (high - low)).cast("long")


# ---------------------------------------------------------------------------
# Promotions
# ---------------------------------------------------------------------------

# Named promo catalog. A purchase with an offer gets at most one code for the
# whole basket (``baskets.basket_promotion``): seasonal codes apply only in
# their eligible months, minimums apply to the basket gross subtotal, and each
# code discounts only the lines in its department scope (``baskets.PROMO_SCOPES``).
# BOGO discounts every second unit and needs a line with at least two units.
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


def promo_eligible(index: int, month: int, basket_cents: int, max_quantity: int) -> bool:
    """Whether ``PROMO_CATALOG[index]`` may apply to a basket.

    The code must be in season, the basket gross subtotal must meet any
    minimum, and BOGO needs a line with at least two units. Department scope
    is checked per line by ``baskets.basket_promotion``.
    """
    _, _, months, min_cents, kind = PROMO_CATALOG[index]
    if months is not None and month not in months:
        return False
    if basket_cents < min_cents:
        return False
    return kind != "BOGO" or max_quantity >= 2


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


# Share of in-store visits whose phone is identified (its BLE ID is in
# dim_customers); the rest are anonymous.
BLE_KNOWN_SHARE = 0.3
ANONYMOUS_BLE_PREFIX = "ANON-"


def anonymous_ble_id_column(store_id: Column, draw: Column) -> Column:
    """Anonymous BLE device ID: ``ANON-<store>-<6 digits>`` from a non-negative hash."""
    return F.concat(
        F.lit(ANONYMOUS_BLE_PREFIX),
        store_id.cast("string"),
        F.lit("-"),
        (draw % F.lit(900000) + F.lit(100000)).cast("string"),
    )


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

# Generic zone dwell ranges (seconds), which the store types' zones borrow.
FOOT_TRAFFIC_ZONE_DWELL: dict[str, tuple[int, int]] = {
    "ENTRANCE_MAIN": (45, 90),
    "ENTRANCE_SIDE": (30, 75),
    "AISLES_A": (180, 420),
    "AISLES_B": (120, 300),
    "CHECKOUT": (90, 240),
}


# A shopper's path starts at the entrance, visits some department zones in a
# random order, and passes checkout when the visit buys (visit_zone_path). Each
# zone borrows the dwell range of the generic zone it overlaps.
BLE_ENTRY_ZONE = "ENTRANCE"
BLE_CHECKOUT_ZONE = "CHECKOUT"
BLE_ZONE_DWELL: dict[str, tuple[int, int]] = {
    "ENTRANCE": FOOT_TRAFFIC_ZONE_DWELL["ENTRANCE_MAIN"],
    "ELECTRONICS": FOOT_TRAFFIC_ZONE_DWELL["AISLES_B"],
    "GROCERY": FOOT_TRAFFIC_ZONE_DWELL["AISLES_A"],
    "CLOTHING": FOOT_TRAFFIC_ZONE_DWELL["AISLES_B"],
    "CHECKOUT": FOOT_TRAFFIC_ZONE_DWELL["CHECKOUT"],
}


def ble_zones_for_profile(profile: StoreTypeProfile | None) -> list[str]:
    """BLE zones for a store type: the profile's ``zones`` when it declares any.

    Profile zone names are normalized to upper case so they match the rest of
    the zone vocabulary. Without a profile the default ``BLE_ZONES`` apply.
    History (``sensors.py``) and the live stream both use these zones.
    """
    if profile is None or not profile.zones:
        return list(BLE_ZONES)
    return list(dict.fromkeys(zone.strip().upper() for zone in profile.zones if zone.strip()))


BLE_EXIT_ZONE = "EXIT"
# Dwell of zones outside BLE_ZONE_DWELL: exits pass like entrances, every other
# store-type zone (produce, tools, jewelry, ...) dwells like a secondary aisle.
BLE_EXIT_DWELL = FOOT_TRAFFIC_ZONE_DWELL["ENTRANCE_MAIN"]
BLE_DEPARTMENT_DWELL = FOOT_TRAFFIC_ZONE_DWELL["AISLES_B"]


@dataclass(frozen=True)
class BleLayout:
    """A store type's BLE zones by their role in a shopper's path."""

    entry: str
    middle: list[str]
    checkout: str | None
    exit: str | None
    dwell: dict[str, tuple[int, int]]

    @property
    def zones(self) -> list[str]:
        ordered = [self.entry, *self.middle, self.checkout, self.exit]
        return list(dict.fromkeys(zone for zone in ordered if zone is not None))


def ble_layout(profile: StoreTypeProfile | None) -> BleLayout:
    """Entry, department, checkout and exit zones with their dwell ranges.

    The entry is ``ENTRANCE`` (else the first zone), checkout and exit are the
    ``CHECKOUT`` and ``EXIT`` zones when the store type has them, and every
    other zone is a department zone a shopper may browse.
    """
    zones = ble_zones_for_profile(profile)
    entry = BLE_ENTRY_ZONE if BLE_ENTRY_ZONE in zones else zones[0]
    checkout = BLE_CHECKOUT_ZONE if BLE_CHECKOUT_ZONE in zones else None
    exit_zone = BLE_EXIT_ZONE if BLE_EXIT_ZONE in zones and BLE_EXIT_ZONE != entry else None
    middle = [zone for zone in zones if zone not in (entry, checkout, exit_zone)]
    if not middle:
        middle = [zone for zone in zones if zone != entry] or [entry]
    dwell = {
        zone: BLE_ZONE_DWELL.get(
            zone, BLE_EXIT_DWELL if zone == BLE_EXIT_ZONE else BLE_DEPARTMENT_DWELL
        )
        for zone in zones
    }
    return BleLayout(entry, middle, checkout, exit_zone, dwell)


# A shopper's visit (history and the live stream): the entrance, 1-3 of the
# store type's department zones in a random order, checkout when the visit
# buys, and the exit. Foot-traffic sensors count every zone a visitor passes;
# beacons detect the phones of BLE_DETECTED_SHARE of visits, 2-5 pings a zone.
BLE_MAX_MIDDLE_ZONES = 3
BLE_PINGS_PER_ZONE = (2, 5)
BLE_DETECTED_SHARE = 0.25
# Pings carry an RSSI uniform in [-80, -29] dBm.
RSSI_FLOOR = -80
RSSI_SPAN = 52


def zone_bound(zone: Column, dwell: dict[str, tuple[int, int]], index: int) -> Column:
    """Lower (``index`` 0) or upper (1) dwell seconds of ``zone``."""
    return case_when((zone == name, F.lit(bounds[index])) for name, bounds in dwell.items())


def visit_zone_path(draws: seeded_draws, key: list, converts: Column, layout: BleLayout) -> Column:
    """Ordered zones of a visit: entrance, browsed departments, checkout (buyers), exit.

    ``key`` holds the visit's key columns; every draw is keyed on it, so history
    and the stream lay out a visit the same way.
    """
    shuffled = F.array_sort(
        F.array(
            *[
                F.struct(
                    draws.u([*key, F.lit(zone)], "zone-order").alias("draw"),
                    F.lit(zone).alias("zone"),
                )
                for zone in layout.middle
            ]
        )
    )
    browse = min(BLE_MAX_MIDDLE_ZONES, len(layout.middle))
    middle_count = (F.floor(draws.u(key, "zone-count") * browse) + 1).cast("int")
    middle = F.transform(F.slice(shuffled, 1, middle_count), lambda entry: entry["zone"])
    no_zone = F.array().cast("array<string>")
    checkout = (
        F.when(converts, F.array(F.lit(layout.checkout))).otherwise(no_zone)
        if layout.checkout is not None
        else no_zone
    )
    exit_zone = F.array(F.lit(layout.exit)) if layout.exit is not None else no_zone
    return F.concat(F.array(F.lit(layout.entry)), middle, checkout, exit_zone)


def visit_zone_dwell(draws: seeded_draws, key: list, zones: Column, layout: BleLayout) -> Column:
    """Dwell seconds in each zone of a visit's path, uniform in the zone's range."""
    return F.transform(
        zones,
        lambda zone, i: (
            zone_bound(zone, layout.dwell, 0)
            + F.floor(
                draws.u([*key, i], "zone-dwell")
                * (zone_bound(zone, layout.dwell, 1) - zone_bound(zone, layout.dwell, 0) + 1)
            )
        ).cast("long"),
    )


def visit_zone_starts(dwell: Column) -> Column:
    """Seconds from the visit start at which each zone of the path is entered."""
    return F.transform(
        dwell,
        lambda _dwell, i: F.aggregate(
            F.slice(dwell, 1, i), F.lit(0).cast("long"), lambda acc, x: acc + x
        ),
    )


def visit_ping_counts(draws: seeded_draws, key: list, zones: Column) -> Column:
    """Beacon pings in each zone of a detected visit (2-5)."""
    low, high = BLE_PINGS_PER_ZONE
    return F.transform(
        zones,
        lambda _zone, i: (F.floor(draws.u([*key, i], "pings") * (high - low + 1)) + low).cast(
            "int"
        ),
    )


def checkout_index(zones: Column, layout: BleLayout) -> Column:
    """1-based position of the zone a buyer pays in: the last before any exit."""
    return F.size(zones) - (1 if layout.exit is not None else 0)


def zone_passage_shares(layout: BleLayout) -> dict[str, float]:
    """Share of visitors that pass each non-checkout zone of the layout.

    Every visitor passes the entrance and the exit; a department zone is one
    of 1-``BLE_MAX_MIDDLE_ZONES`` browsed zones. Checkout is passed by buyers.
    """
    browse = min(BLE_MAX_MIDDLE_ZONES, len(layout.middle))
    department = (browse + 1) / 2 / len(layout.middle)
    shares = {zone: department for zone in layout.middle}
    shares[layout.entry] = 1.0
    if layout.exit is not None:
        shares[layout.exit] = 1.0
    return shares


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

# channel -> (lo, hi) cost-per-impression band in dollars (uniform draw), at
# real CPMs ($2.50-$21 per thousand); the device multiplier keeps every
# impression between $0.002 and $0.03.
CHANNEL_COSTS: dict[str, tuple[float, float]] = {
    "EMAIL": (0.0025, 0.006),
    "DISPLAY": (0.003, 0.010),
    "SOCIAL": (0.005, 0.014),
    "SEARCH": (0.010, 0.025),
    "FACEBOOK": (0.006, 0.016),
    "GOOGLE": (0.008, 0.022),
    "INSTAGRAM": (0.006, 0.016),
    "YOUTUBE": (0.010, 0.025),
}
# Relative chance that an impression on a channel converts (attribution.py):
# search and email convert best, display worst.
CHANNEL_CONVERSION_WEIGHTS: dict[str, float] = {
    "SEARCH": 2.0,
    "EMAIL": 1.6,
    "GOOGLE": 1.5,
    "FACEBOOK": 1.0,
    "INSTAGRAM": 0.9,
    "SOCIAL": 0.8,
    "YOUTUBE": 0.7,
    "DISPLAY": 0.5,
}
# Each campaign rotates a small pool of creatives per channel.
CREATIVES_PER_CAMPAIGN = 4
CREATIVE_ID_PREFIX = "CRT"


def channel_conversion_weight(channel: Column) -> Column:
    """Column form of ``CHANNEL_CONVERSION_WEIGHTS`` (1.0 for an unknown channel)."""
    return case_when(
        (channel == name, F.lit(weight)) for name, weight in CHANNEL_CONVERSION_WEIGHTS.items()
    ).otherwise(F.lit(1.0))


def creative_id_column(
    draws: seeded_draws, keys: list, campaign: Column, channel: Column
) -> Column:
    """A creative from the campaign's small pool on the channel: ``CRT<campaign>-<channel>-<n>``."""
    index = (draws.h64(keys, "creative") % CREATIVES_PER_CAMPAIGN + 1).cast("int")
    return F.concat(
        F.lit(CREATIVE_ID_PREFIX),
        F.substring(campaign, len(CAMPAIGN_ID_PREFIX) + 1, 32),
        F.lit("-"),
        channel,
        F.lit("-"),
        index.cast("string"),
    )


def impression_cost_cents(draws: seeded_draws, keys: list, dollars: Column) -> Column:
    """Whole cents of a sub-cent cost, rounded at random so sums stay unbiased."""
    return F.floor(dollars * 100 + draws.u(keys, "cost-round")).cast("long")


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

# Order-level outcomes and stage delays shared by history (online_orders.py)
# and the live stream. Cancelled orders have zero totals and zero line
# amounts, no payment and no fulfillment; backordered (never BOPIS) orders
# wait ONLINE_BACKORDER_SECONDS for stock between picking and shipping, then
# ship and deliver like any other order.
ONLINE_CANCEL_RATE = 0.02
ONLINE_BACKORDER_RATE = 0.08
ONLINE_BACKORDER_SECONDS = (3 * 86400, 14 * 86400)
# Promo codes with their percentage discount; each order puts a promo on a
# share of its lines drawn uniformly from ONLINE_PROMO_LINE_SHARE.
ONLINE_PROMOS: list[tuple[str, int]] = [("PROMO05", 5), ("PROMO10", 10), ("PROMO20", 20)]
ONLINE_PROMO_LINE_SHARE = (0.10, 0.30)
# Stage delays in seconds, uniform in [low, high): picking after the order
# (BOPIS orders are picked for the pickup counter, ship orders for the
# carrier), shipping after picking (plus the backorder wait), delivery after
# shipping, and the BOPIS customer pickup after the order is ready.
ONLINE_PICK_SECONDS = {"BOPIS": (4 * 3600, 24 * 3600), "SHIP": (30 * 60, 240 * 60)}
ONLINE_SHIP_SECONDS = (2 * 3600, 4 * 3600)
ONLINE_DELIVER_SECONDS = (86400, 3 * 86400)
ONLINE_PICKUP_SECONDS = (3600, 24 * 3600)
# Relative weight of online orders in each UTC hour (0-23). Most customers
# are in US time zones, so orders peak in the US evening (00-03 UTC) and are
# fewest in the US night (07-10 UTC).
ONLINE_HOURLY_WEIGHTS = [
    6.5, 6.5, 6.0, 5.0, 3.8, 2.6, 1.6, 1.0, 0.8, 0.9, 1.3, 2.0,
    2.9, 3.6, 4.1, 4.4, 4.6, 4.7, 4.7, 4.8, 5.0, 5.4, 5.9, 6.3,
]  # fmt: skip
