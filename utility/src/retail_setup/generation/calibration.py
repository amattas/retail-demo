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
# breach-day measure. Historical dwell is 30-120 minutes (mean 75), so a
# site-day averaging above 90 is a real outlier.
TRUCK_DWELL_SLA_MINUTES = 90
# Live stream unload model (stream_supply): a truck dwells the base minutes
# plus its load at the site's unload rate (units per minute times a per-store
# dock factor), plus a little noise, and never less than the minimum. A typical
# consolidated store load stays inside the normal range, under the SLA; only an
# unusually large load (a store served late after a backlog) breaches it.
TRUCK_NORMAL_DWELL_MINUTES_MIN = 30
TRUCK_NORMAL_DWELL_MINUTES_MAX = 75
TRUCK_UNLOAD_BASE_MINUTES = 25
TRUCK_UNLOAD_UNITS_PER_MINUTE = 120
TRUCK_UNLOAD_NOISE_MINUTES = 6
TRUCK_DOCK_FACTOR_RANGE = (0.8, 1.2)
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
# Live truck logistics (stream_supply)
# ---------------------------------------------------------------------------

# Reorders wait at the DC for the store's daily delivery slot, so one truck
# carries a store's consolidated load; then the next free truck of the DC's
# fleet loads for TRUCK_LOAD_MINUTES, drives the lane (one-way minutes, a stable
# draw per DC and store) and returns.
TRUCK_LOAD_MINUTES = 45
TRUCK_TRANSIT_MINUTES_MIN = 60
TRUCK_TRANSIT_MINUTES_MAX = 240

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
