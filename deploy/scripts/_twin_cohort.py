"""Select a bounded twin network from recorded routes, never inferred links."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from itertools import combinations
from typing import Any

MAX_ROUTE_ROWS = 10_000
WINDOW_DAYS = 14

ROUTES_SQL = """
WITH anchor AS (
    SELECT MAX(event_ts) AS source_as_of
    FROM [ag].[fact_truck_moves] WHERE event_ts <= ?
),
ranked AS (
    SELECT moves.dc_id, moves.truck_id, moves.store_id, moves.shipment_id,
           moves.event_ts, anchor.source_as_of,
           ROW_NUMBER() OVER (
               PARTITION BY moves.dc_id, moves.truck_id, moves.store_id, moves.shipment_id
               ORDER BY moves.event_ts DESC, moves.trace_id DESC
           ) AS position
    FROM [ag].[fact_truck_moves] AS moves CROSS JOIN anchor
    WHERE moves.event_ts >= DATEADD(day, -14, anchor.source_as_of)
      AND moves.event_ts <= anchor.source_as_of
)
SELECT TOP (10001) dc_id, truck_id, store_id, shipment_id, event_ts, source_as_of
FROM ranked WHERE position = 1
ORDER BY event_ts DESC, dc_id, truck_id, store_id, shipment_id
"""


def _positive_id(value: Any) -> int:
    if type(value) is not int or value < 1:
        raise ValueError("Twin entity IDs must be positive integers from the source.")
    return value


def utc_time(value: Any) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise ValueError("Twin source timestamps are required.")
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def select_cohort(
    routes: list[dict[str, Any]], *, as_of: datetime,
) -> dict[str, Any]:
    if not routes or len(routes) > MAX_ROUTE_ROWS:
        raise ValueError("Twin route discovery requires 1-10000 bounded source rows.")
    as_of = utc_time(as_of)
    start = as_of - timedelta(days=WINDOW_DAYS)
    groups: dict[int, dict[int, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    shipments: set[str] = set()
    for record in routes:
        dc, truck, store = (
            _positive_id(record[name]) for name in ("dc_id", "truck_id", "store_id")
        )
        shipment = record["shipment_id"]
        if not isinstance(shipment, str) or not shipment.strip() or shipment in shipments:
            raise ValueError("Twin shipment identity is missing, duplicated or ambiguous.")
        shipments.add(shipment)
        timestamp = utc_time(record["event_ts"])
        if not start <= timestamp <= as_of:
            raise ValueError("Twin route timestamps fall outside the observed source window.")
        groups[dc][truck].append({
            "dc_id": dc, "truck_id": truck, "store_id": store,
            "shipment_id": shipment, "event_ts": timestamp,
        })

    candidates = []
    for dc, trucks in sorted(groups.items()):
        if len(trucks) > 100:
            raise ValueError("Twin discovery exceeds the bounded fleet size per warehouse.")
        for first, second in combinations(sorted(trucks), 2):
            rows = trucks[first] + trucks[second]
            coverage = len({row["store_id"] for row in rows})
            latest = min(max(row["event_ts"] for row in trucks[key]) for key in (first, second))
            candidates.append(((-min(5, coverage), -latest.timestamp(), -len(rows), dc, first, second), rows))
    if not candidates:
        raise ValueError("No recorded warehouse network contains two trucks; no links were invented.")
    score, selected = min(candidates, key=lambda value: value[0])
    dc, first, second = score[-3:]
    selected.sort(key=lambda row: (-row["event_ts"].timestamp(), row["shipment_id"]))

    # Include each truck's latest recorded destination before filling the store cohort.
    stores = {
        next(row["store_id"] for row in selected if row["truck_id"] == truck)
        for truck in (first, second)
    }
    for row in selected:
        if len(stores) == 5:
            break
        stores.add(row["store_id"])
    selected = [row for row in selected if row["store_id"] in stores]
    if len(selected) > 1000:
        raise ValueError("The selected twin network exceeds 1000 recorded shipments.")
    return {
        "dc_id": dc,
        "truck_ids": [first, second],
        "store_ids": sorted(stores),
        "shipment_ids": sorted(row["shipment_id"] for row in selected),
        "source_as_of": as_of.isoformat(),
        "window_start": start.isoformat(),
        "routes": [
            {**row, "event_ts": row["event_ts"].isoformat()} for row in selected
        ],
    }


def validate_product_ids(values: list[int]) -> list[int]:
    if not 1 <= len(values) <= 20:
        raise ValueError("A twin cohort requires 1-20 observed cargo products.")
    parsed = [_positive_id(value) for value in values]
    if len(set(parsed)) != len(parsed):
        raise ValueError("Twin cargo product IDs must be unique.")
    return sorted(parsed)


def products_query(cohort: dict[str, Any]) -> tuple[str, tuple[Any, ...]]:
    """Use DB-API parameters even though cohort identifiers are already validated."""
    trucks = [_positive_id(value) for value in cohort["truck_ids"]]
    stores = [_positive_id(value) for value in cohort["store_ids"]]
    if len(trucks) != 2 or not 1 <= len(stores) <= 5:
        raise ValueError("Invalid truck/store cohort for cargo discovery.")
    sql = f"""
WITH routes AS (
    SELECT DISTINCT truck_id, shipment_id
    FROM [ag].[fact_truck_moves]
    WHERE dc_id = ?
      AND truck_id IN ({", ".join("?" for _ in trucks)})
      AND store_id IN ({", ".join("?" for _ in stores)})
      AND event_ts >= ? AND event_ts <= ?
)
SELECT TOP (20) cargo.product_id, COUNT(*) AS movement_count
FROM [ag].[fact_truck_inventory] AS cargo
JOIN routes ON routes.truck_id = cargo.truck_id AND routes.shipment_id = cargo.shipment_id
JOIN [ag].[dim_products] AS product ON product.ID = cargo.product_id
WHERE cargo.event_ts <= ? AND cargo.action IN ('LOAD', 'UNLOAD')
GROUP BY cargo.product_id
ORDER BY COUNT(*) DESC, cargo.product_id
"""
    end = utc_time(cohort["source_as_of"]).replace(tzinfo=None)
    start = utc_time(cohort["window_start"]).replace(tzinfo=None)
    return sql, (_positive_id(cohort["dc_id"]), *trucks, *stores, start, end, end)
