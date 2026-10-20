"""Bounded availability signals derived from the deployed historical/ML contract."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

SCENARIO_SQL = """
WITH selected_stores AS (
    SELECT ID FROM [ag].[dim_stores] WHERE ID BETWEEN 1 AND 5
),
selected_products AS (
    SELECT ID AS product_id FROM [ag].[dim_products] WHERE ID BETWEEN 1 AND 20
),
demand AS (
    SELECT store_id, product_id, SUM(predicted_units) AS forecast_units_7d,
           MIN(source_as_of) AS forecast_as_of, COUNT(DISTINCT forecast_date) AS forecast_days,
           COUNT(*) AS forecast_rows, COUNT(DISTINCT source_as_of) AS forecast_versions
    FROM [au].[demand_forecast]
    WHERE forecast_date > CAST(source_as_of AS date)
      AND forecast_date <= DATEADD(day, 7, CAST(source_as_of AS date))
      AND store_id IN (SELECT ID FROM selected_stores)
      AND product_id IN (SELECT product_id FROM selected_products)
    GROUP BY store_id, product_id
)
SELECT risk.store_id, risk.product_id, product.ProductName AS product_name,
       risk.current_inventory, demand.forecast_units_7d,
       risk.stockout_probability, risk.days_of_inventory,
       risk.forecast_horizon_days AS risk_horizon_days,
       risk.inventory_as_of AS source_as_of, demand.forecast_as_of, demand.forecast_days,
       demand.forecast_rows, demand.forecast_versions
FROM [au].[stockout_risk] AS risk
JOIN selected_stores AS store ON store.ID = risk.store_id
JOIN selected_products AS selected ON selected.product_id = risk.product_id
JOIN [ag].[dim_products] AS product ON product.ID = risk.product_id
JOIN demand ON demand.store_id = risk.store_id AND demand.product_id = risk.product_id
ORDER BY risk.store_id, risk.product_id
"""

FIELDS = (
    ("event_ts", "datetime"),
    ("source_as_of", "datetime"),
    ("forecast_as_of", "datetime"),
    ("case_id", "string"),
    ("store_id", "long"),
    ("product_id", "long"),
    ("product_name", "string"),
    ("current_inventory", "long"),
    ("forecast_units_7d", "real"),
    ("stockout_probability", "real"),
    ("risk_horizon_days", "long"),
    ("days_of_inventory", "real"),
    ("shortage_units", "long"),
    ("scenario", "string"),
    ("provenance", "string"),
)


def scenario_signals(
    rows: list[dict[str, Any]], observed_at: datetime
) -> list[dict[str, Any]]:
    if not rows or len(rows) > 100:
        raise ValueError(
            "Availability scenario requires 1-100 real store/product source rows."
        )
    if observed_at.tzinfo is None:
        raise ValueError("Scenario monitoring time must be timezone-aware.")
    if (
        len({row["store_id"] for row in rows}) > 5
        or len({row["product_id"] for row in rows}) > 20
    ):
        raise ValueError(
            "Availability scenario is limited to five stores and twenty products."
        )
    result = []
    keys = set()
    for row in rows:
        store_id, product_id = int(row["store_id"]), int(row["product_id"])
        if not 1 <= store_id <= 5 or not 1 <= product_id <= 20:
            raise ValueError("Availability identifiers are outside the fixed Plan cohort.")
        key = (store_id, product_id)
        if key in keys:
            raise ValueError("Availability source is not unique by store/product.")
        keys.add(key)
        source_time = row["source_as_of"]
        if isinstance(source_time, str):
            source_time = datetime.fromisoformat(source_time.replace("Z", "+00:00"))
        if not isinstance(source_time, datetime):
            raise ValueError("Availability source time is missing.")
        if source_time.tzinfo is None:
            source_time = source_time.replace(tzinfo=UTC)
        if not 0 <= (observed_at - source_time).total_seconds() <= 7 * 86400:
            raise ValueError(
                "Availability source is stale or future-dated; refresh inputs and required ML first."
            )
        forecast_time = row["forecast_as_of"]
        if isinstance(forecast_time, str):
            forecast_time = datetime.fromisoformat(forecast_time.replace("Z", "+00:00"))
        if (
            not isinstance(forecast_time, datetime)
            or row["forecast_days"] != 7
            or row["forecast_rows"] != 7
            or row["forecast_versions"] != 1
        ):
            raise ValueError("Availability forecast must cover seven distinct days.")
        if forecast_time.tzinfo is None:
            forecast_time = forecast_time.replace(tzinfo=UTC)
        if not 0 <= (observed_at - forecast_time).total_seconds() <= 7 * 86400:
            raise ValueError(
                "Forecast source is stale or future-dated; refresh inputs and required ML first."
            )
        inventory = row["current_inventory"]
        if inventory is None:
            raise ValueError("Availability inventory is unknown.")
        inventory = max(0, int(inventory))
        forecast = float(row["forecast_units_7d"])
        probability = float(row["stockout_probability"])
        risk_horizon = int(row["risk_horizon_days"])
        cover = float(row["days_of_inventory"])
        if (
            not all(math.isfinite(value) for value in (forecast, probability, cover))
            or forecast < 0
            or not 0 <= probability <= 1
            or cover < 0
            or risk_horizon < 1
        ):
            raise ValueError("Availability metrics violate the ML contract.")
        case_id = str(
            uuid5(
                NAMESPACE_URL,
                f"retail-availability:{store_id}:{product_id}:"
                f"{source_time.astimezone(UTC).isoformat()}:{forecast_time.astimezone(UTC).isoformat()}",
            )
        )
        result.append(
            {
                "event_ts": observed_at.astimezone(UTC).isoformat(),
                "source_as_of": source_time.astimezone(UTC).isoformat(),
                "forecast_as_of": forecast_time.astimezone(UTC).isoformat(),
                "case_id": case_id,
                "store_id": store_id,
                "product_id": product_id,
                "product_name": str(row["product_name"]),
                "current_inventory": inventory,
                "forecast_units_7d": forecast,
                "stockout_probability": probability,
                "risk_horizon_days": risk_horizon,
                "days_of_inventory": cover,
                "shortage_units": max(0, math.ceil(forecast) - inventory),
                "scenario": "Protect weekend availability",
                "provenance": "Synthetic retail demo; observed from current Gold forecast/risk outputs. "
                "Reservations and confirmed inbound are unverified, not assumed available.",
            }
        )
    return result


def signal_ingestion_query(signals: list[dict[str, Any]]) -> str:
    if not signals or len(signals) > 100:
        raise ValueError("Expected a bounded, nonempty signal batch.")
    values = []
    for row in signals:
        cells = []
        for name, kind in FIELDS:
            value = row[name]
            if kind == "datetime":
                value = datetime.fromisoformat(value)
                cells.append(f"datetime({value.astimezone(UTC).isoformat()})")
            else:
                cells.append(json.dumps(value, ensure_ascii=True, allow_nan=False))
        values.append(", ".join(cells))
    columns = ", ".join(f"{name}:{kind}" for name, kind in FIELDS)
    return (
        ".set-or-append ops_availability_signals <|\n"
        f"datatable({columns})[\n" + ",\n".join(values) + "\n]"
    )


def validate_signal_readback(
    expected: list[dict[str, Any]], observed: list[dict[str, Any]],
) -> None:
    actual = {row["case_id"]: row for row in observed}
    if len(actual) != len(observed) or set(actual) != {row["case_id"] for row in expected}:
        raise ValueError("Seeded availability case identity readback does not match.")
    for wanted in expected:
        for name, kind in FIELDS:
            value = wanted[name]
            found = actual[wanted["case_id"]][name]
            if kind == "datetime":
                value = datetime.fromisoformat(value)
                if isinstance(found, str):
                    found = datetime.fromisoformat(found.replace("Z", "+00:00"))
                if isinstance(found, datetime) and found.tzinfo is None:
                    found = found.replace(tzinfo=UTC)
            if found != value:
                raise ValueError(f"Seeded availability readback differs at {name}.")
