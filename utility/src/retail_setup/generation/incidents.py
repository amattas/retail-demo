"""Logistics incidents and delivery timing, shared by history and the live stream.

**Incidents.** :func:`plan_incidents` lays out, deterministically from the seed,
every disruption of the supply network between two dates:

- ``DC_OUTAGE``: a DC stops dispatching for 6-36 hours (power, systems,
  equipment, labor); loads wait for the stores' next delivery slot after it.
- ``LANE_DELAY``: one DC-to-store lane runs slow for 2-12 hours (closures,
  accidents, construction); trucks on it arrive 45-180 minutes late.
- ``WEATHER``: severe weather in a geography (the severe-weather days of
  ``stream_calendar``, opened more often in the climate's rough season) slows
  the lanes into its stores for one to four days.
- ``HOLIDAY_CAPACITY``: strained DC crews over the Black Friday weekend and
  the days after Christmas delay loading.
- ``SUPPLIER_SHORT_SHIP``: one supplier group (a sixth of the products) ships
  only 20-60% of the DC's orders for 3-10 days, so the DC runs short.

Each incident has a start, an end, a severity, its DC (and lane store or
geography) and the set of stores it affects: every store the DC serves, the
lane's store, or the geography's stores. Random incidents occur a few times a
year per DC (``calibration``).

**Delivery timing.** :func:`plan_delivery` turns a store's load into a
planned and an actual schedule in the store's local time. The plan ships at
the store's next delivery slot whose arrival day the store is open (no
deliveries on holiday closures), loads, and drives the lane. The actual run
waits out DC outages (to the next slot after the outage), loads late under
holiday capacity limits, and arrives late from traffic (more likely on long
lanes and Fridays), lane incidents and weather. ``delay_minutes`` is the
actual minus the planned arrival; ``delay_reason`` names the largest cause.

History (``inventory_policy``) and the live stream (``stream_supply``) both
use these functions, so they disrupt the same lanes on the same days. Pure
Python; ``build_notebooks.py`` inlines this module into the notebooks.
"""

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from retail_setup.generation.calibration import (
    DC_OUTAGE_HOURS,
    DC_OUTAGES_PER_DC_YEAR,
    HOLIDAY_CAPACITY_DELAY_MINUTES,
    HOLIDAY_CAPACITY_SHARE,
    INCIDENT_HIGH_DELAY_MINUTES,
    INCIDENT_HIGH_OUTAGE_HOURS,
    INCIDENT_HIGH_SHORT_SHARE,
    INCIDENT_MEDIUM_DELAY_MINUTES,
    INCIDENT_MEDIUM_OUTAGE_HOURS,
    INCIDENT_MEDIUM_SHORT_SHARE,
    LANE_DELAY_HOURS,
    LANE_DELAY_MINUTES,
    LANE_DELAYS_PER_DC_YEAR,
    SUPPLIER_FILL_RATE,
    SUPPLIER_GROUPS,
    SUPPLIER_SHORT_SHIP_DAYS,
    SUPPLIER_SHORT_SHIPS_PER_DC_YEAR,
    TRUCK_LOAD_MINUTES,
    TRUCK_TRAFFIC_DAY_FACTORS,
    TRUCK_TRAFFIC_DELAY_SHARE,
    TRUCK_TRAFFIC_LATE_SHARE,
    TRUCK_TRANSIT_MINUTES_MAX,
    TRUCK_TRANSIT_MINUTES_MIN,
    WEATHER_DELAY_MINUTES,
    WEATHER_INCIDENT_MAX_DAYS,
    WEATHER_INCIDENT_SHARE,
)
from retail_setup.generation.inventory_policy import stable_int
from retail_setup.generation.stream_calendar import WEATHER_SIGMA, holiday_name, weather_climate

INCIDENT_TYPES = (
    "DC_OUTAGE",
    "LANE_DELAY",
    "WEATHER",
    "HOLIDAY_CAPACITY",
    "SUPPLIER_SHORT_SHIP",
)
# delay_reason values of a late truck (TRAFFIC has no incident).
DELAY_REASONS = ("DC_OUTAGE", "HOLIDAY_CAPACITY", "LANE_DELAY", "WEATHER", "TRAFFIC")
SEVERITIES = ("LOW", "MEDIUM", "HIGH")
# An incident planned this many days before a window can still be open in it
# (the longest incident is a supplier short-ship).
INCIDENT_LOOKBACK_DAYS = SUPPLIER_SHORT_SHIP_DAYS[1] + 1
# Severe-weather days needed after a window to size its last weather incident.
WEATHER_LOOKAHEAD_DAYS = WEATHER_INCIDENT_MAX_DAYS
# Holiday weekends that strain DC crews, and how many days each lasts.
HOLIDAY_CAPACITY_WINDOWS = {"BLACK_FRIDAY": 4, "DAY_AFTER_CHRISTMAS": 2}

_OUTAGE_CAUSES = (
    "power failure",
    "warehouse system outage",
    "dock equipment failure",
    "labor shortage",
)
_LANE_CAUSES = (
    "highway closure",
    "multi-vehicle accident",
    "road construction",
    "bridge restriction",
)
_TYPE_CODES = {
    "DC_OUTAGE": "OUT",
    "LANE_DELAY": "LANE",
    "WEATHER": "WX",
    "HOLIDAY_CAPACITY": "HOL",
    "SUPPLIER_SHORT_SHIP": "SUP",
}
_INCIDENT_UNIT = 1_000_000


def _incident_unit(seed: int, *keys: object) -> float:
    """Stable uniform draw in [0, 1)."""
    return stable_int(seed, *keys) % _INCIDENT_UNIT / _INCIDENT_UNIT


def _incident_between(seed: int, bounds: tuple[int, int], *keys: object) -> int:
    """Stable integer draw in ``[low, high]``."""
    low, high = bounds
    return low + stable_int(seed, *keys) % (high - low + 1)


def supplier_group(seed: int, product_id: int) -> int:
    """The supplier group a product comes from."""
    return stable_int(seed, int(product_id), "supplier") % SUPPLIER_GROUPS


def delay_severity(minutes: int) -> str:
    if minutes >= INCIDENT_HIGH_DELAY_MINUTES:
        return "HIGH"
    return "MEDIUM" if minutes >= INCIDENT_MEDIUM_DELAY_MINUTES else "LOW"


def outage_severity(hours: float) -> str:
    if hours >= INCIDENT_HIGH_OUTAGE_HOURS:
        return "HIGH"
    return "MEDIUM" if hours >= INCIDENT_MEDIUM_OUTAGE_HOURS else "LOW"


def short_ship_severity(fill_rate: float) -> str:
    lost = 1.0 - fill_rate
    if lost >= INCIDENT_HIGH_SHORT_SHARE:
        return "HIGH"
    return "MEDIUM" if lost >= INCIDENT_MEDIUM_SHORT_SHARE else "LOW"


@dataclass(frozen=True)
class Incident:
    """One logistics incident; times are UTC (aware)."""

    incident_id: str
    incident_type: str
    dc_id: int | None
    store_id: int | None
    started_at: datetime
    resolved_at: datetime
    severity: str
    affected_stores: tuple[int, ...]
    description: str
    # Minutes a delay incident adds to a run (loading or transit).
    delay_minutes: int = 0
    # Share of supplier orders a short-shipping supplier delivers.
    fill_rate: float = 1.0
    supplier_group: int | None = None
    geography_id: int | None = None

    def active_at(self, at: datetime) -> bool:
        return self.started_at <= at < self.resolved_at


@dataclass(frozen=True)
class IncidentNetwork:
    """The network the incidents run on: store routing, zones and geographies."""

    store_dc: Mapping[int, int]
    store_zones: Mapping[int, str] = field(default_factory=dict)
    store_geo: Mapping[int, int] = field(default_factory=dict)
    geo_state: Mapping[int, str] = field(default_factory=dict)
    dc_zones: Mapping[int, str] = field(default_factory=dict)

    def dc_ids(self) -> list[int]:
        return sorted({int(dc) for dc in self.store_dc.values()})

    def stores_of(self, dc_id: int) -> tuple[int, ...]:
        return tuple(sorted(int(s) for s, dc in self.store_dc.items() if int(dc) == int(dc_id)))

    def zone_of_store(self, store_id: int) -> str:
        return self.store_zones.get(int(store_id)) or "UTC"

    def zone_of_dc(self, dc_id: int) -> str:
        zone = self.dc_zones.get(int(dc_id))
        if zone:
            return zone
        stores = self.stores_of(dc_id)
        return self.zone_of_store(stores[0]) if stores else "UTC"


def local_instant(day: date, minute: int, zone: str) -> datetime:
    """UTC instant of a local wall-clock minute of ``day`` in ``zone``."""
    start = datetime.combine(day, time(), tzinfo=ZoneInfo(zone))
    return (start + timedelta(minutes=minute)).astimezone(timezone.utc)


def to_local(at: datetime, zone: str) -> datetime:
    """Naive local wall-clock time of a UTC instant in ``zone``."""
    return at.astimezone(ZoneInfo(zone)).replace(tzinfo=None)


def to_utc(local: datetime, zone: str) -> datetime:
    """UTC instant of a naive local wall-clock time in ``zone``."""
    return local.replace(tzinfo=ZoneInfo(zone)).astimezone(timezone.utc)


def _incident_id(kind: str, day: date, *parts: int) -> str:
    keys = "-".join(f"{int(part):03d}" for part in parts)
    return f"INC-{_TYPE_CODES[kind]}-{keys}-{day:%Y%m%d}"


def _dc_incidents(seed: int, network: IncidentNetwork, dc_id: int, day: date) -> list[Incident]:
    stores = network.stores_of(dc_id)
    zone = network.zone_of_dc(dc_id)
    found: list[Incident] = []
    if _incident_unit(seed, dc_id, day, "outage") < DC_OUTAGES_PER_DC_YEAR / 365.0:
        hours = _incident_between(seed, DC_OUTAGE_HOURS, dc_id, day, "outage-hours")
        start = local_instant(day, stable_int(seed, dc_id, day, "outage-minute") % 1440, zone)
        cause = _OUTAGE_CAUSES[stable_int(seed, dc_id, day, "outage-cause") % len(_OUTAGE_CAUSES)]
        found.append(
            Incident(
                _incident_id("DC_OUTAGE", day, dc_id),
                "DC_OUTAGE",
                dc_id,
                None,
                start,
                start + timedelta(hours=hours),
                outage_severity(hours),
                stores,
                f"DC {dc_id} outage ({cause}): no dispatches for {hours}h",
            )
        )
    if stores and _incident_unit(seed, dc_id, day, "lane") < LANE_DELAYS_PER_DC_YEAR / 365.0:
        store = stores[stable_int(seed, dc_id, day, "lane-store") % len(stores)]
        hours = _incident_between(seed, LANE_DELAY_HOURS, dc_id, day, "lane-hours")
        minutes = _incident_between(seed, LANE_DELAY_MINUTES, dc_id, day, "lane-minutes")
        # Lane incidents start in the daytime, 05:00-20:59 local.
        minute = 300 + stable_int(seed, dc_id, day, "lane-minute") % 960
        start = local_instant(day, minute, network.zone_of_store(store))
        cause = _LANE_CAUSES[stable_int(seed, dc_id, day, "lane-cause") % len(_LANE_CAUSES)]
        found.append(
            Incident(
                _incident_id("LANE_DELAY", day, dc_id, store),
                "LANE_DELAY",
                dc_id,
                store,
                start,
                start + timedelta(hours=hours),
                delay_severity(minutes),
                (store,),
                f"Lane DC {dc_id} to store {store} ({cause}): trucks {minutes} min late",
                delay_minutes=minutes,
            )
        )
    if _incident_unit(seed, dc_id, day, "supplier") < SUPPLIER_SHORT_SHIPS_PER_DC_YEAR / 365.0:
        days = _incident_between(seed, SUPPLIER_SHORT_SHIP_DAYS, dc_id, day, "supplier-days")
        low, high = SUPPLIER_FILL_RATE
        fill = round(low + (high - low) * _incident_unit(seed, dc_id, day, "supplier-fill"), 2)
        group = stable_int(seed, dc_id, day, "supplier-group") % SUPPLIER_GROUPS
        start = local_instant(day, 0, zone)
        found.append(
            Incident(
                _incident_id("SUPPLIER_SHORT_SHIP", day, dc_id),
                "SUPPLIER_SHORT_SHIP",
                dc_id,
                None,
                start,
                local_instant(day + timedelta(days=days), 0, zone),
                short_ship_severity(fill),
                stores,
                f"Supplier group {group} short-shipping DC {dc_id}: "
                f"{round(fill * 100)}% of orders arrive for {days} days",
                fill_rate=fill,
                supplier_group=group,
            )
        )
    name = holiday_name(day)
    if name in HOLIDAY_CAPACITY_WINDOWS and (
        _incident_unit(seed, dc_id, day, "holiday") < HOLIDAY_CAPACITY_SHARE
    ):
        minutes = _incident_between(
            seed, HOLIDAY_CAPACITY_DELAY_MINUTES, dc_id, day, "holiday-minutes"
        )
        days = HOLIDAY_CAPACITY_WINDOWS[name]
        found.append(
            Incident(
                _incident_id("HOLIDAY_CAPACITY", day, dc_id),
                "HOLIDAY_CAPACITY",
                dc_id,
                None,
                local_instant(day, 0, zone),
                local_instant(day + timedelta(days=days), 0, zone),
                delay_severity(minutes),
                stores,
                f"DC {dc_id} holiday peak ({name.replace('_', ' ').lower()}): "
                f"loading runs {minutes} min late",
                delay_minutes=minutes,
            )
        )
    return found


def _weather_incidents(
    seed: int, network: IncidentNetwork, geo: int, day: date, severe: set[tuple[int, date]]
) -> list[Incident]:
    if (geo, day) not in severe or (geo, day - timedelta(days=1)) in severe:
        return []
    sigma = WEATHER_SIGMA[weather_climate(network.geo_state.get(geo))]
    share = WEATHER_INCIDENT_SHARE * sigma[day.month - 1] / max(sigma)
    if _incident_unit(seed, geo, day, "weather-incident") >= share:
        return []
    days = 1
    while days < WEATHER_INCIDENT_MAX_DAYS and (geo, day + timedelta(days=days)) in severe:
        days += 1
    minutes = _incident_between(seed, WEATHER_DELAY_MINUTES, geo, day, "weather-minutes")
    by_dc: dict[int, list[int]] = {}
    for store, store_geo in network.store_geo.items():
        if int(store_geo) == geo and int(store) in network.store_dc:
            by_dc.setdefault(int(network.store_dc[int(store)]), []).append(int(store))
    found = []
    for dc_id, stores in sorted(by_dc.items()):
        zone = network.zone_of_store(min(stores))
        found.append(
            Incident(
                _incident_id("WEATHER", day, geo, dc_id),
                "WEATHER",
                dc_id,
                None,
                local_instant(day, 0, zone),
                local_instant(day + timedelta(days=days), 0, zone),
                delay_severity(minutes),
                tuple(sorted(stores)),
                f"Severe weather in geography {geo} ({network.geo_state.get(geo) or 'unknown'}): "
                f"lanes from DC {dc_id} run {minutes} min late for {days} day(s)",
                delay_minutes=minutes,
                geography_id=geo,
            )
        )
    return found


def plan_incidents(
    seed: int,
    network: IncidentNetwork,
    start: date,
    end: date,
    severe_days: Iterable[tuple[int, date]] = (),
) -> list[Incident]:
    """Every incident that starts between ``start`` and ``end`` (inclusive).

    ``severe_days`` holds ``(geography_id, date)`` pairs of severe weather
    (``stream_calendar.severe_weather_column``); it must cover the window plus
    ``WEATHER_LOOKAHEAD_DAYS`` after it and the day before it. Each incident
    depends only on the seed and its own day, so overlapping windows agree.
    """
    if end < start:
        return []
    severe = {(int(geo), day) for geo, day in severe_days}
    geos = sorted(
        {int(geo) for store, geo in network.store_geo.items() if int(store) in network.store_dc}
    )
    found: list[Incident] = []
    day = start
    while day <= end:
        for dc_id in network.dc_ids():
            found.extend(_dc_incidents(seed, network, dc_id, day))
        for geo in geos:
            found.extend(_weather_incidents(seed, network, geo, day, severe))
        day += timedelta(days=1)
    return sorted(found, key=lambda incident: (incident.started_at, incident.incident_id))


class IncidentCalendar:
    """Queries over a set of incidents at UTC instants."""

    def __init__(self, incidents: Iterable[Incident]) -> None:
        self.incidents = tuple(
            sorted(incidents, key=lambda incident: (incident.started_at, incident.incident_id))
        )

    def active(self, at: datetime) -> list[Incident]:
        return [incident for incident in self.incidents if incident.active_at(at)]

    def _of(self, kind: str, dc_id: int | None = None) -> list[Incident]:
        return [
            incident
            for incident in self.incidents
            if incident.incident_type == kind and (dc_id is None or incident.dc_id == dc_id)
        ]

    def outage_end(self, dc_id: int, at: datetime) -> datetime | None:
        """End of the DC outage (chained outages included) holding ``at``, else None."""
        end = None
        current = at
        outages = self._of("DC_OUTAGE", int(dc_id))
        while True:
            covering = [o.resolved_at for o in outages if o.active_at(current)]
            if not covering:
                return end
            end = current = max(covering)

    def held_dcs(self, at: datetime) -> frozenset[int]:
        """DCs in an outage at ``at``."""
        return frozenset(
            int(incident.dc_id)
            for incident in self._of("DC_OUTAGE")
            if incident.dc_id is not None and incident.active_at(at)
        )

    def supplier_fill(self, dc_id: int, group: int, at: datetime) -> float:
        """Share of a supplier order for the group that arrives at the DC at ``at``."""
        fills = [
            incident.fill_rate
            for incident in self._of("SUPPLIER_SHORT_SHIP", int(dc_id))
            if incident.supplier_group == group and incident.active_at(at)
        ]
        return min(fills, default=1.0)

    def store_view(self, store_id: int, dc_id: int, zone: str) -> "StoreIncidentView":
        """The incidents that time one store's loads, in its local time."""

        def windows(kinds: Iterable[Incident]) -> tuple[tuple[datetime, datetime, int], ...]:
            return tuple(
                (
                    to_local(incident.started_at, zone),
                    to_local(incident.resolved_at, zone),
                    incident.delay_minutes,
                )
                for incident in kinds
            )

        store_id, dc_id = int(store_id), int(dc_id)
        return StoreIncidentView(
            outages=windows(self._of("DC_OUTAGE", dc_id)),
            capacity=windows(self._of("HOLIDAY_CAPACITY", dc_id)),
            lanes=windows(i for i in self._of("LANE_DELAY", dc_id) if i.store_id == store_id),
            weather=windows(i for i in self._of("WEATHER") if store_id in i.affected_stores),
        )

    def dc_view(self, dc_id: int, zone: str) -> "DcIncidentView":
        """The supplier short-ships of one DC, in its local time."""
        return DcIncidentView(
            short_ships=tuple(
                (
                    to_local(incident.started_at, zone),
                    to_local(incident.resolved_at, zone),
                    int(incident.supplier_group or 0),
                    incident.fill_rate,
                )
                for incident in self._of("SUPPLIER_SHORT_SHIP", int(dc_id))
            )
        )


IncidentWindow = tuple[datetime, datetime, int]


def _window_delay(windows: Iterable[IncidentWindow], at: datetime) -> int:
    return max((minutes for start, end, minutes in windows if start <= at < end), default=0)


@dataclass(frozen=True)
class StoreIncidentView:
    """A store's incident windows (local naive start, end, delay minutes)."""

    outages: tuple[IncidentWindow, ...] = ()
    capacity: tuple[IncidentWindow, ...] = ()
    lanes: tuple[IncidentWindow, ...] = ()
    weather: tuple[IncidentWindow, ...] = ()

    def outage_end(self, at: datetime) -> datetime | None:
        end = None
        current = at
        while True:
            covering = [stop for start, stop, _ in self.outages if start <= current < stop]
            if not covering:
                return end
            end = current = max(covering)


@dataclass(frozen=True)
class DcIncidentView:
    """A DC's supplier short-ships (local naive start, end, group, fill rate)."""

    short_ships: tuple[tuple[datetime, datetime, int, float], ...] = ()

    def fill_rate(self, group: int, at: datetime) -> float:
        return min(
            (fill for start, end, g, fill in self.short_ships if g == group and start <= at < end),
            default=1.0,
        )


@dataclass(frozen=True)
class DeliveryTiming:
    """A load's planned and actual schedule (store-local naive times).

    ``slot`` is the delivery slot the load actually leaves at (after any
    outage hold); loading starts there unless holiday capacity delays it.
    """

    planned_loading: datetime
    slot: datetime
    loading: datetime
    departure: datetime
    planned_arrival: datetime
    arrival: datetime
    delay_minutes: int
    delay_reason: str | None


def traffic_delay_minutes(
    seed: int, store_id: int, dc_id: int, transit_minutes: int, planned_loading: datetime
) -> int:
    """Minutes traffic adds to a run (0 for most runs), keyed by its planned loading."""
    span = TRUCK_TRANSIT_MINUTES_MAX - TRUCK_TRANSIT_MINUTES_MIN
    length = min(1.0, max(0.0, (transit_minutes - TRUCK_TRANSIT_MINUTES_MIN) / span))
    low, high = TRUCK_TRAFFIC_LATE_SHARE
    chance = (low + (high - low) * length) * TRUCK_TRAFFIC_DAY_FACTORS[planned_loading.weekday()]
    key = (int(store_id), int(dc_id), f"{planned_loading:%Y%m%d%H%M}")
    if _incident_unit(seed, *key, "traffic") >= chance:
        return 0
    low_share, high_share = TRUCK_TRAFFIC_DELAY_SHARE
    share = low_share + (high_share - low_share) * _incident_unit(seed, *key, "traffic-share")
    return max(1, round(transit_minutes * share))


def next_local_slot(since: datetime, slot_minute: int) -> datetime:
    """The first local slot (minute of the day) at or after ``since``."""
    slot = datetime.combine(since.date(), time()) + timedelta(minutes=int(slot_minute))
    return slot if slot >= since else slot + timedelta(days=1)


def plan_delivery(
    seed: int,
    *,
    store_id: int,
    dc_id: int,
    ready: datetime,
    slot_minute: int,
    transit_minutes: int,
    view: StoreIncidentView | None = None,
    closed: Callable[[date], bool] | None = None,
) -> DeliveryTiming:
    """Planned and actual schedule of a load that is ready at ``ready`` (local).

    The plan ships at the first slot at or after ``ready`` whose arrival day
    the store is open. A DC outage moves loading to the first open slot after
    it; holiday capacity limits delay loading; traffic, lane incidents (at
    departure) and weather (at the planned arrival) delay the run.
    """
    run = timedelta(minutes=TRUCK_LOAD_MINUTES + int(transit_minutes))

    def open_slot(since: datetime) -> datetime:
        slot = next_local_slot(since, slot_minute)
        for _ in range(14):
            if closed is None or not closed((slot + run).date()):
                break
            slot = next_local_slot(slot + timedelta(minutes=1), slot_minute)
        return slot

    planned = open_slot(ready)
    slot = planned
    if view is not None:
        for _ in range(60):
            end = view.outage_end(slot)
            if end is None:
                break
            slot = open_slot(end)
    hold = round((slot - planned).total_seconds() / 60)
    capacity = _window_delay(view.capacity, slot) if view is not None else 0
    loading = slot + timedelta(minutes=capacity)
    departure = loading + timedelta(minutes=TRUCK_LOAD_MINUTES)
    planned_arrival = planned + run
    traffic = traffic_delay_minutes(seed, store_id, dc_id, transit_minutes, planned)
    lane = _window_delay(view.lanes, departure) if view is not None else 0
    weather = (
        _window_delay(view.weather, departure + timedelta(minutes=transit_minutes))
        if view is not None
        else 0
    )
    arrival = departure + timedelta(minutes=int(transit_minutes) + traffic + lane + weather)
    delay = round((arrival - planned_arrival).total_seconds() / 60)
    causes = [
        ("DC_OUTAGE", hold),
        ("HOLIDAY_CAPACITY", capacity),
        ("LANE_DELAY", lane),
        ("WEATHER", weather),
        ("TRAFFIC", traffic),
    ]
    reason = max(causes, key=lambda cause: cause[1])[0] if delay > 0 else None
    return DeliveryTiming(
        planned, slot, loading, departure, planned_arrival, arrival, delay, reason
    )
