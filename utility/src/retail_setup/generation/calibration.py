"""Calibrated thresholds shared by the generators and their consumers.

Rules (``fabric/rules/definitions.kql``), KQL functions, the semantic model and
the live stream keep literal values in their own languages. Each literal must
equal the constant here; ``tests/generation/test_calibration.py`` parses those
sources and fails when one drifts.

The stream generator parameters below sit next to the thresholds they are
calibrated against, so a normal truck stays under the dwell SLA and a late
truck exceeds it.
"""

from retail_setup.dictionaries.models import StoreTypeProfile

# ---------------------------------------------------------------------------
# Truck dwell (minutes from arrival to departure)
# ---------------------------------------------------------------------------

# One SLA for every consumer: the dwell-breach rule and the semantic model's
# breach-day measure. A typical consolidated load dwells 30-75 minutes, so a
# site-day averaging above 90 is a real outlier.
TRUCK_DWELL_SLA_MINUTES = 90
# Unload model shared by history and the live stream (network.unload_minutes):
# a truck dwells the base minutes plus its load at the site's unload rate
# (units per minute times a per-store dock factor), plus a little noise, never
# less than the minimum and never more than the maximum. A typical
# consolidated store load stays inside the normal range, under the SLA; only an
# unusually large load (a store served late after a backlog) breaches it.
TRUCK_NORMAL_DWELL_MINUTES_MIN = 30
TRUCK_NORMAL_DWELL_MINUTES_MAX = 75
TRUCK_UNLOAD_BASE_MINUTES = 25
TRUCK_UNLOAD_UNITS_PER_MINUTE = 120
TRUCK_UNLOAD_NOISE_MINUTES = 6
TRUCK_DOCK_FACTOR_RANGE = (0.8, 1.2)
TRUCK_UNLOAD_MINUTES_MAX = 180
# History: unloading (and the store's stock receipt) starts this long after
# the truck arrives.
TRUCK_CHECKIN_MINUTES = 15
# Rule evaluation window for completed lifecycles.
TRUCK_DWELL_RULE_WINDOW_MINUTES = 5

# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

# A stockout is reported when a store's on-hand balance drops below this
# quantity, that is when a sale takes it to zero: the crossing history's
# fact_stockouts records. Reorder points and priorities come from the shared
# replenishment policy (inventory_policy.reorder_levels / reorder_priority).
STOCKOUT_QUANTITY_THRESHOLD = 1
# Rule evaluation windows.
STOCKOUT_RULE_WINDOW_MINUTES = 5
URGENT_REORDER_RULE_WINDOW_MINUTES = 10

# ---------------------------------------------------------------------------
# Truck logistics (history and stream_supply)
# ---------------------------------------------------------------------------

# Reorders wait at the DC for the store's daily delivery slot, so one truck
# carries a store's consolidated load; then the next free truck of the DC's
# fleet loads for TRUCK_LOAD_MINUTES, drives the lane (one-way minutes, a stable
# draw per DC and store) and returns.
TRUCK_LOAD_MINUTES = 45
TRUCK_TRANSIT_MINUTES_MIN = 60
TRUCK_TRANSIT_MINUTES_MAX = 240
# Delivery routes (network.plan_routes): each DC truck serves a fixed set of
# stores, one delivery a day each, at slots spaced by the longest possible
# trip (load, both legs of the lane, the maximum unload) plus this turnaround,
# which also absorbs the hour lost when clocks spring forward.
TRUCK_TURNAROUND_MINUTES = 60

# Unload extras (network.unload_minutes): a truck unloading in the store's
# busy hours (local) takes longer, one at night is quicker; a truck that
# arrives TRUCK_LATE_MINUTES or more after plan has missed its dock
# appointment and waits for a door.
TRUCK_UNLOAD_BUSY_HOURS = (11, 19)
TRUCK_UNLOAD_NIGHT_HOURS = (22, 6)
TRUCK_UNLOAD_BUSY_FACTOR = 1.15
TRUCK_UNLOAD_NIGHT_FACTOR = 0.9
TRUCK_LATE_DOCK_WAIT_MINUTES = 10

# ---------------------------------------------------------------------------
# Delivery delays and logistics incidents (incidents.py)
# ---------------------------------------------------------------------------

# A truck is late when it arrives this many minutes or more after its planned
# arrival (the late-truck rule, the semantic model's late-shipment measures and
# the unload model's missed dock appointment).
TRUCK_LATE_MINUTES = 30
# Traffic: a run's chance of being late grows with the lane's length (from the
# first value on the shortest lane to the second on the longest), scaled by
# the weekday (Monday first: Friday getaway traffic, quiet Sundays); a late run
# loses this share of its lane minutes.
TRUCK_TRAFFIC_LATE_SHARE = (0.05, 0.20)
TRUCK_TRAFFIC_DAY_FACTORS = (1.0, 1.0, 1.0, 1.1, 1.4, 0.8, 0.6)
TRUCK_TRAFFIC_DELAY_SHARE = (0.15, 0.50)
# Random incidents per DC and year, and their extent.
DC_OUTAGES_PER_DC_YEAR = 1.5
LANE_DELAYS_PER_DC_YEAR = 4.0
SUPPLIER_SHORT_SHIPS_PER_DC_YEAR = 2.0
DC_OUTAGE_HOURS = (6, 36)
LANE_DELAY_HOURS = (2, 12)
LANE_DELAY_MINUTES = (45, 180)
SUPPLIER_SHORT_SHIP_DAYS = (3, 10)
# Share of a supplier order that arrives while the supplier short-ships.
SUPPLIER_FILL_RATE = (0.2, 0.6)
# Products split into this many supplier groups; a short-ship hits one group.
SUPPLIER_GROUPS = 6
# Severe-weather days (stream_calendar.severe_weather_column) open a WEATHER
# incident on the lanes into the geography with this probability in the
# climate's roughest month, scaled down by its weather sensitivity in others.
WEATHER_INCIDENT_SHARE = 0.6
WEATHER_INCIDENT_MAX_DAYS = 4
WEATHER_DELAY_MINUTES = (60, 240)
# Peak holiday weekends (Black Friday to Cyber Monday, the days after
# Christmas) strain each DC's crews with this probability.
HOLIDAY_CAPACITY_SHARE = 0.7
HOLIDAY_CAPACITY_DELAY_MINUTES = (60, 240)
# Incident severity by the delay it adds (minutes) or the share of supply lost.
INCIDENT_HIGH_DELAY_MINUTES = 150
INCIDENT_MEDIUM_DELAY_MINUTES = 90
INCIDENT_HIGH_OUTAGE_HOURS = 24
INCIDENT_MEDIUM_OUTAGE_HOURS = 12
INCIDENT_HIGH_SHORT_SHARE = 0.65
INCIDENT_MEDIUM_SHORT_SHARE = 0.5
# Rules: a late truck alerts on arrival; a lane alerts when its trucks average
# this delay over the window; an incident alerts when stores it affects report
# this many stockouts.
TRUCK_LATE_RULE_WINDOW_MINUTES = 5
LANE_DELAY_RULE_MINUTES = 60
LANE_DELAY_RULE_WINDOW_MINUTES = 60
INCIDENT_RULE_WINDOW_MINUTES = 5
INCIDENT_STOCKOUT_RULE_WINDOW_MINUTES = 60
INCIDENT_STOCKOUT_RULE_MIN_STOCKOUTS = 3

# ---------------------------------------------------------------------------
# DC replenishment (inventory_policy, stream_supply)
# ---------------------------------------------------------------------------

# Each DC keeps stock of every product, fed by supplier orders that arrive a
# stable 3-7 days later at DC_SUPPLIER_RECEIPT_HOUR (DC local). The DC orders
# when its position (on hand plus open supplier orders) falls to its daily
# demand times (lead time + review day + safety days), up to the lead time plus
# DC_ORDER_UP_TO_DAYS. It opens with DC_OPENING_DAYS of demand.
DC_SUPPLIER_LEAD_DAYS = (3, 7)
DC_SAFETY_DAYS = 4
DC_ORDER_UP_TO_DAYS = 14
DC_OPENING_DAYS = (10, 16)
DC_SUPPLIER_RECEIPT_HOUR = 4
# Weight of the newest day in the DC's smoothed daily demand.
DC_DEMAND_SMOOTHING = 0.1
# Online ship-from-DC orders add about this share to a DC's store demand.
DC_ONLINE_DEMAND_SHARE = 0.1

# ---------------------------------------------------------------------------
# Zone arrivals
# ---------------------------------------------------------------------------

# Arrival-surge rule: per (store, zone) over the window, alert when arrivals
# exceed the surge count or the arrival-weighted expected dwell exceeds the
# long-dwell seconds.
ZONE_SURGE_WINDOW_MINUTES = 5
ZONE_SURGE_ARRIVALS = 50
ZONE_LONG_DWELL_SECONDS = 600

# ---------------------------------------------------------------------------
# High-value receipts
# ---------------------------------------------------------------------------

# A receipt is high value when its total exceeds this multiple of the store
# type's calibrated average ticket (profile.avg_ticket_target).
HIGH_VALUE_TICKET_MULTIPLE = 3.0
HIGH_VALUE_RULE_WINDOW_MINUTES = 5
# Store type whose threshold the static reference rule carries.
REFERENCE_STORE_TYPE = "supercenter"


def high_value_receipt_threshold(profile: StoreTypeProfile) -> float:
    """Receipt total (dollars) above which a receipt counts as high value."""
    return round(HIGH_VALUE_TICKET_MULTIPLE * profile.avg_ticket_target, 2)


# ---------------------------------------------------------------------------
# Anomaly detection (series_decompose_anomalies)
# ---------------------------------------------------------------------------

ANOMALY_LOOKBACK_HOURS = 168
ANOMALY_THRESHOLD = 1.5
ANOMALY_SEASONALITY = -1  # autodetect
ANOMALY_TREND = "avg"
# Severity by |anomaly_score|, highest band first; below the last band is "low".
ANOMALY_SEVERITY_BANDS: list[tuple[str, float]] = [
    ("critical", 3.0),
    ("high", 2.0),
    ("medium", 1.5),
]


def anomaly_severity(score: float) -> str:
    """Severity label the anomaly functions assign to an anomaly score."""
    for label, bound in ANOMALY_SEVERITY_BANDS:
        if abs(score) >= bound:
            return label
    return "low"
