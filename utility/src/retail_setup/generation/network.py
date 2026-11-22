"""Store-to-DC routing, delivery routes and DC truck fleets.

History (``inventory.py``) and the live stream (``stream_supply``) share all of
it, so both deliver the same way:

- **Routing.** Each store is served by a DC in its own state, else in its
  region, else any DC. Among those candidates the store goes to the DC serving
  the fewest stores so far (stores in ID order, ties to the lower DC ID), so DCs
  in one state share the load instead of the lowest ID serving everything.
- **Routes.** Each DC truck runs a fixed daily route: a set of stores in one
  time zone, each with a daily delivery slot. Slots on a route are spaced by
  the longest possible trip (load, the lane both ways, the maximum unload, a
  turnaround), so a truck on its route is never in two places at once. A
  store's reorders of one day leave on its next slot as one consolidated load.
- **Fleets.** ``dim_trucks`` gives each DC one truck per route (at least
  ``MIN_DC_FLEET``) plus a shared pool (``DCID`` NULL) for overflow loads; a DC
  without assigned trucks falls back to the pool.
- **Unloading.** Dwell is driven by load size and a per-store dock factor
  (``unload_minutes``), capped at ``TRUCK_UNLOAD_MINUTES_MAX``.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_setup.generation.calibration import (
    TRUCK_DOCK_FACTOR_RANGE,
    TRUCK_LOAD_MINUTES,
    TRUCK_NORMAL_DWELL_MINUTES_MIN,
    TRUCK_TURNAROUND_MINUTES,
    TRUCK_UNLOAD_BASE_MINUTES,
    TRUCK_UNLOAD_MINUTES_MAX,
    TRUCK_UNLOAD_NOISE_MINUTES,
    TRUCK_UNLOAD_UNITS_PER_MINUTE,
)
from retail_setup.generation.inventory_policy import lane_transit_minutes, stable_int

MIN_DC_FLEET = 2
POOL_TRUCK_SHARE = 0.15
MINUTES_PER_DAY = 1440


def assign_dcs(
    stores: Iterable[tuple[int, str, str]], dcs: Iterable[tuple[int, str, str]]
) -> dict[int, int]:
    """``{store_id: dc_id}``: same state, else same region, else any DC, least loaded.

    ``stores`` and ``dcs`` hold ``(id, state, region)``.
    """
    sites = sorted((int(dc), state, region) for dc, state, region in dcs)
    if not sites:
        raise ValueError("routing requires at least one distribution center")
    load = {dc: 0 for dc, _, _ in sites}
    assigned: dict[int, int] = {}
    for store_id, state, region in sorted((int(s), st, rg) for s, st, rg in stores):
        candidates = (
            [dc for dc, dc_state, _ in sites if dc_state == state]
            or [dc for dc, _, dc_region in sites if dc_region == region]
            or [dc for dc, _, _ in sites]
        )
        best = min(candidates, key=lambda dc: (load[dc], dc))
        load[best] += 1
        assigned[store_id] = best
    return assigned


@dataclass(frozen=True)
class DeliveryRoute:
    """A store's place on its DC's delivery routes."""

    dc_id: int
    route: int
    slot_minute: int
    transit_minutes: int


def route_budget_minutes(transit_minutes: int) -> int:
    """Longest possible round trip to a store, plus the turnaround."""
    return (
        TRUCK_LOAD_MINUTES
        + 2 * transit_minutes
        + TRUCK_UNLOAD_MINUTES_MAX
        + TRUCK_TURNAROUND_MINUTES
    )


def plan_routes(
    seed: int, store_dc: Mapping[int, int], store_zones: Mapping[int, str]
) -> dict[int, DeliveryRoute]:
    """Daily delivery routes of every DC, packed first-fit into trucks.

    Stores of one DC and time zone (in ID order) fill a truck's day with their
    round-trip budgets; a route starts at a stable minute and each store's slot
    follows the previous store's budget, wrapping around midnight. Slots are in
    the store's local time.
    """
    groups: dict[int, dict[str, list[int]]] = {}
    for store_id, dc_id in store_dc.items():
        zone = store_zones.get(int(store_id), "UTC")
        groups.setdefault(int(dc_id), {}).setdefault(zone, []).append(int(store_id))
    routes: dict[int, DeliveryRoute] = {}
    for dc_id in sorted(groups):
        index = 0
        for zone in sorted(groups[dc_id]):
            bins: list[list[tuple[int, int, int]]] = []
            used: list[int] = []
            for store_id in sorted(groups[dc_id][zone]):
                transit = lane_transit_minutes(seed, dc_id, store_id)
                budget = route_budget_minutes(transit)
                slot = next(
                    (b for b, minutes in enumerate(used) if minutes + budget <= MINUTES_PER_DAY),
                    None,
                )
                if slot is None:
                    bins.append([])
                    used.append(0)
                    slot = len(bins) - 1
                bins[slot].append((store_id, transit, budget))
                used[slot] += budget
            for offset, members in enumerate(bins):
                minute = stable_int(seed, dc_id, zone, offset, "route") % MINUTES_PER_DAY
                for store_id, transit, budget in members:
                    routes[store_id] = DeliveryRoute(dc_id, index + offset, minute, transit)
                    minute = (minute + budget) % MINUTES_PER_DAY
            index += len(bins)
    return routes


def routes_per_dc(routes: Mapping[int, DeliveryRoute]) -> dict[int, int]:
    """Number of daily routes each DC runs."""
    counts: dict[int, int] = {}
    for route in routes.values():
        counts[route.dc_id] = max(counts.get(route.dc_id, 0), route.route + 1)
    return counts


def fleet_sizes(dc_routes: Mapping[int, int], dc_ids: Iterable[int]) -> dict[int, int]:
    """Trucks assigned to each DC: one per route, at least ``MIN_DC_FLEET``."""
    return {int(dc): max(MIN_DC_FLEET, int(dc_routes.get(int(dc), 0))) for dc in sorted(dc_ids)}


def pool_size(assigned_trucks: int) -> int:
    """Shared pool trucks so that they are POOL_TRUCK_SHARE of the fleet."""
    return max(1, round(assigned_trucks * POOL_TRUCK_SHARE / (1 - POOL_TRUCK_SHARE)))


def route_truck(route: DeliveryRoute, fleets: Mapping[int, tuple[int, ...]]) -> int:
    """The DC truck that runs a store's route."""
    fleet = fleets[route.dc_id]
    return fleet[route.route % len(fleet)]


def next_slot_ms(since_ms: int, slot_minute: int, zone: str | None = None) -> int:
    """First instant at or after ``since_ms`` whose wall clock in ``zone`` is the slot."""
    tz = ZoneInfo(zone) if zone else timezone.utc
    local_day = datetime.fromtimestamp(since_ms / 1000, tz).date()
    for days in range(3):
        start = datetime.combine(local_day + timedelta(days=days), time(), tzinfo=tz)
        candidate = round((start + timedelta(minutes=slot_minute)).timestamp() * 1000)
        if candidate >= since_ms:
            return candidate
    raise RuntimeError("no delivery slot within two days")


def dock_factor(seed: int, store_id: int) -> float:
    """How slowly a store's dock unloads relative to the calibrated rate."""
    low, high = TRUCK_DOCK_FACTOR_RANGE
    return low + (high - low) * (stable_int(seed, int(store_id), "dock") % 1001) / 1000


def estimated_unload_minutes(seed: int, store_id: int, units: int) -> int:
    """Planned dwell of a load: base time plus the units at the site's rate."""
    handling = units / TRUCK_UNLOAD_UNITS_PER_MINUTE * dock_factor(seed, store_id)
    return min(
        TRUCK_UNLOAD_MINUTES_MAX,
        max(TRUCK_NORMAL_DWELL_MINUTES_MIN, round(TRUCK_UNLOAD_BASE_MINUTES + handling)),
    )


def unload_minutes(seed: int, store_id: int, units: int, shipment_id: str) -> int:
    """Actual dwell: the estimate plus a little noise per shipment, within the bounds."""
    span = 2 * TRUCK_UNLOAD_NOISE_MINUTES + 1
    noise = stable_int(seed, shipment_id, "unload") % span - TRUCK_UNLOAD_NOISE_MINUTES
    return min(
        TRUCK_UNLOAD_MINUTES_MAX,
        max(
            TRUCK_NORMAL_DWELL_MINUTES_MIN, estimated_unload_minutes(seed, store_id, units) + noise
        ),
    )


def _store_sites(
    dim_stores: DataFrame, dim_geographies: DataFrame, dim_distribution_centers: DataFrame
) -> tuple[list[tuple[int, str, str]], list[tuple[int, str, str]], dict[int, str]]:
    geo = {
        int(row["ID"]): (row["State"], row["Region"])
        for row in dim_geographies.select("ID", "State", "Region").collect()
    }
    stores, zones = [], {}
    for row in dim_stores.select("ID", "GeographyID", "TimeZone").collect():
        state, region = geo[int(row["GeographyID"])]
        stores.append((int(row["ID"]), state, region))
        zones[int(row["ID"])] = row["TimeZone"] or "UTC"
    dcs = []
    for row in dim_distribution_centers.select("ID", "GeographyID").collect():
        state, region = geo[int(row["GeographyID"])]
        dcs.append((int(row["ID"]), state, region))
    return stores, dcs, zones


def store_routes(
    seed: int,
    dim_stores: DataFrame,
    dim_geographies: DataFrame,
    dim_distribution_centers: DataFrame,
) -> dict[int, DeliveryRoute]:
    """Every store's DC and delivery route, from the (small) dimensions."""
    stores, dcs, zones = _store_sites(dim_stores, dim_geographies, dim_distribution_centers)
    return plan_routes(seed, assign_dcs(stores, dcs), zones)


def store_dc_map(
    dim_stores: DataFrame, dim_geographies: DataFrame, dim_distribution_centers: DataFrame
) -> DataFrame:
    """One ``(store_id, dc_id)`` row per store (see :func:`assign_dcs`)."""
    stores, dcs, _ = _store_sites(dim_stores, dim_geographies, dim_distribution_centers)
    spark = SparkSession.getActiveSession() or dim_stores.sparkSession
    rows = sorted(assign_dcs(stores, dcs).items())
    return spark.createDataFrame(rows, "store_id long, dc_id long").select(
        F.col("store_id"), F.col("dc_id")
    )


def dc_fleets(
    trucks: Iterable[tuple[int, int | float | None]], dc_ids: Iterable[int]
) -> dict[int, tuple[int, ...]]:
    """Truck IDs of each DC's fleet from ``(truck_id, DCID)`` pairs.

    A DC with no assigned trucks uses the pool trucks (``DCID`` is NULL).
    """
    assigned: dict[int, list[int]] = {}
    pool: list[int] = []
    for truck_id, dc_id in trucks:
        if dc_id is None:
            pool.append(int(truck_id))
        else:
            assigned.setdefault(int(dc_id), []).append(int(truck_id))
    fleets: dict[int, tuple[int, ...]] = {}
    for dc in sorted(int(dc) for dc in dc_ids):
        fleet = sorted(assigned.get(dc) or pool)
        if not fleet:
            raise ValueError(f"DC {dc} has neither assigned nor shared trucks")
        fleets[dc] = tuple(fleet)
    return fleets
