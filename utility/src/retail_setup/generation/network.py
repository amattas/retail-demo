"""Store-to-DC routing and DC truck fleets, shared by history and the live stream.

Each store is served by its nearest distribution center by geography (same
state, then same region, then the lowest DC ID). Each DC's fleet is the trucks
assigned to it in ``dim_trucks``, sized to the stores it serves; a DC without
assigned trucks falls back to the shared pool (trucks with no DC).
"""

from collections.abc import Iterable
from math import ceil

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


# A truck's round trip (load, drive, unload, drive back) takes about 6-7 hours
# and each store takes one delivery a day, so 0.4 trucks per store keeps a DC's
# fleet roughly 70% busy. Every DC keeps at least MIN_DC_FLEET trucks, and the
# shared pool adds POOL_TRUCK_SHARE of the whole fleet.
TRUCKS_PER_STORE = 0.4
MIN_DC_FLEET = 2
POOL_TRUCK_SHARE = 0.15


def nearest_dc(state: str, region: str, dcs: Iterable[tuple[int, str, str]]) -> int:
    """The DC serving a store: same state, then same region, then the lowest DC ID.

    ``dcs`` holds ``(dc_id, state, region)``. Python form of :func:`store_dc_map`.
    """
    return min(
        dcs,
        key=lambda dc: (0 if dc[1] == state else 1 if dc[2] == region else 2, dc[0]),
    )[0]


def fleet_sizes(stores_served: dict[int, int], dc_ids: Iterable[int]) -> dict[int, int]:
    """Trucks assigned to each DC for the number of stores it serves."""
    return {
        int(dc): max(MIN_DC_FLEET, ceil(stores_served.get(int(dc), 0) * TRUCKS_PER_STORE))
        for dc in sorted(dc_ids)
    }


def pool_size(assigned_trucks: int) -> int:
    """Shared pool trucks so that they are POOL_TRUCK_SHARE of the fleet."""
    return max(1, round(assigned_trucks * POOL_TRUCK_SHARE / (1 - POOL_TRUCK_SHARE)))


def store_dc_map(
    dim_stores: DataFrame, dim_geographies: DataFrame, dim_distribution_centers: DataFrame
) -> DataFrame:
    """One ``(store_id, dc_id)`` row per store: its nearest DC by geography."""
    geo = dim_geographies.select(F.col("ID").alias("geo_id"), "State", "Region")
    stores = (
        dim_stores.select(F.col("ID").alias("store_id"), "GeographyID")
        .join(geo, F.col("GeographyID") == F.col("geo_id"))
        .select("store_id", F.col("State").alias("s_state"), F.col("Region").alias("s_region"))
    )
    dcs = (
        dim_distribution_centers.select(F.col("ID").alias("dc_id"), "GeographyID")
        .join(geo, F.col("GeographyID") == F.col("geo_id"))
        .select("dc_id", F.col("State").alias("d_state"), F.col("Region").alias("d_region"))
    )
    scored = stores.crossJoin(dcs).withColumn(
        "_score",
        F.when(F.col("s_state") == F.col("d_state"), 0)
        .when(F.col("s_region") == F.col("d_region"), 1)
        .otherwise(2),
    )
    nearest = Window.partitionBy("store_id").orderBy("_score", "dc_id")
    return (
        scored.withColumn("_r", F.row_number().over(nearest))
        .filter(F.col("_r") == 1)
        .select(
            F.col("store_id").cast("long").alias("store_id"),
            F.col("dc_id").cast("long").alias("dc_id"),
        )
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
