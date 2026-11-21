"""Bounded availability signals derived from the deployed historical/ML contract."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5

# The agent's alert rules (fabric/operations/availability.instructions.md).
HIGH_STOCKOUT_PROBABILITY = 0.7
LOW_DAYS_OF_INVENTORY = 3
MAX_SIGNALS = 20
# The native Plan's locked synthetic cohort (fabric/planning/retail-replenishment.Plan).
PLAN_STORE_IDS = range(1, 6)
PLAN_PRODUCT_IDS = range(1, 21)
# The agent query and the case notebook reject evidence older than this.
SIGNAL_MAX_AGE_DAYS = 7
# Action states that suppress further alerts for a case (availability.instructions.md).
ACTED_STATUSES = ("DRAFT", "APPROVED", "RESOLVED")


def meets_alert_rule(signal: dict[str, Any]) -> bool:
    """Return whether a signal row satisfies one of the agent's two alert rules."""

    probability = signal.get("stockout_probability")
    cover = signal.get("days_of_inventory")
    return (probability is not None and float(probability) >= HIGH_STOCKOUT_PROBABILITY) or (
        cover is not None and float(cover) < LOW_DAYS_OF_INVENTORY
    )


def current_alert_cases(
    signals: list[dict[str, Any]],
    actions: list[dict[str, Any]],
    observed_at: datetime,
) -> list[dict[str, Any]]:
    """Reference semantics of the agent's canonical KQL query.

    Keep the newest signal per store/product so a superseded case cannot alert
    twice, drop cases whose source or forecast time is outside the seven-day
    window, then drop cases that already have a DRAFT, APPROVED or RESOLVED
    action. ``availability.instructions.md`` expresses the same steps in KQL.
    """

    def as_time(value: Any) -> datetime:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value if value.tzinfo else value.replace(tzinfo=UTC)

    latest: dict[tuple[int, int], dict[str, Any]] = {}
    for signal in signals:
        key = (int(signal["store_id"]), int(signal["product_id"]))
        if key not in latest or as_time(signal["event_ts"]) > as_time(latest[key]["event_ts"]):
            latest[key] = signal
    oldest = observed_at - timedelta(days=SIGNAL_MAX_AGE_DAYS)
    acted = {action["case_id"] for action in actions if action["status"] in ACTED_STATUSES}
    return [
        signal
        for signal in latest.values()
        if all(
            oldest <= as_time(signal[field]) <= observed_at
            for field in ("source_as_of", "forecast_as_of")
        )
        and signal["case_id"] not in acted
    ]


NO_ALERTABLE_CASE = (
    "No seeded availability case meets an agent rule (stockout_probability >= "
    f"{HIGH_STOCKOUT_PROBABILITY} or days_of_inventory < {LOW_DAYS_OF_INVENTORY}), so the "
    "agent would never alert; it was left stopped. Refresh the stockout and forecast "
    "outputs, republish the signals, then activate it."
)


def require_alertable(signals: list[dict[str, Any]]) -> int:
    """Return how many signals meet a rule; refuse a watchlist with none."""

    count = sum(1 for signal in signals if meets_alert_rule(signal))
    if count == 0:
        raise ValueError(NO_ALERTABLE_CASE)
    return count


def scenario_sql(
    *,
    plan_cohort: bool,
    dialect: str = "tsql",
    catalog: str = "",
    silver: str = "ag",
    gold: str = "au",
) -> str:
    """Select a watchlist of the riskiest observed store/product positions.

    Every position that meets an agent alert rule ranks first, then the
    positions closest to one. Nothing is relabeled or invented: when no
    position is at risk, the agent has nothing to alert on and activation is
    refused (``require_alertable``). Only positions with a complete seven-day
    forecast qualify. ``source_as_of`` is the risk snapshot's as-of time
    (``predicted_at``), not each SKU's last ledger change, so a stocked-out
    SKU with no recent movement is not dropped as stale. With ``plan_cohort``
    the watchlist stays inside the native Plan's locked cohort so every case
    can be reviewed there.

    ``dialect="tsql"`` targets the Lakehouse SQL endpoint used at deploy time;
    ``dialect="spark"`` is the same query for the scheduled refresh notebook
    (``fabric/lakehouse/17-refresh-availability-signals.ipynb``).
    """

    if dialect == "tsql":
        def table(schema: str, name: str) -> str:
            return f"[{schema}].[{name}]"

        horizon_end = "DATEADD(day, 7, CAST(source_as_of AS date))"
        top, limit = f"TOP ({MAX_SIGNALS})\n       ", ""
    elif dialect == "spark":
        def table(schema: str, name: str) -> str:
            return ".".join(part for part in (catalog, schema, name) if part)

        horizon_end = "date_add(CAST(source_as_of AS date), 7)"
        top, limit = "", f"\nLIMIT {MAX_SIGNALS}"
    else:
        raise ValueError(f"Unsupported scenario dialect: {dialect!r}")
    cohort = (
        f"AND risk.store_id BETWEEN {PLAN_STORE_IDS.start} AND {PLAN_STORE_IDS.stop - 1}\n"
        f"  AND risk.product_id BETWEEN {PLAN_PRODUCT_IDS.start} AND {PLAN_PRODUCT_IDS.stop - 1}"
        if plan_cohort
        else ""
    )
    return f"""
WITH demand AS (
    SELECT store_id, product_id, SUM(predicted_units) AS forecast_units_7d,
           MIN(source_as_of) AS forecast_as_of, COUNT(DISTINCT forecast_date) AS forecast_days,
           COUNT(*) AS forecast_rows, COUNT(DISTINCT source_as_of) AS forecast_versions
    FROM {table(gold, "demand_forecast")}
    WHERE forecast_date > CAST(source_as_of AS date)
      AND forecast_date <= {horizon_end}
    GROUP BY store_id, product_id
)
SELECT {top}risk.store_id, risk.product_id, product.ProductName AS product_name,
       risk.current_inventory, demand.forecast_units_7d,
       risk.stockout_probability, risk.days_of_inventory,
       risk.forecast_horizon_days AS risk_horizon_days,
       risk.predicted_at AS source_as_of, demand.forecast_as_of, demand.forecast_days,
       demand.forecast_rows, demand.forecast_versions
FROM {table(gold, "stockout_risk")} AS risk
JOIN {table(silver, "dim_products")} AS product ON product.ID = risk.product_id
JOIN demand ON demand.store_id = risk.store_id AND demand.product_id = risk.product_id
WHERE demand.forecast_days = 7 AND demand.forecast_rows = 7 AND demand.forecast_versions = 1
  {cohort}
ORDER BY
    CASE WHEN risk.stockout_probability >= {HIGH_STOCKOUT_PROBABILITY}
           OR risk.days_of_inventory < {LOW_DAYS_OF_INVENTORY} THEN 0 ELSE 1 END,
    risk.stockout_probability DESC, risk.days_of_inventory ASC,
    risk.store_id, risk.product_id{limit}
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
    ("plan_cohort", "bool"),
)


def scenario_signals(
    rows: list[dict[str, Any]], observed_at: datetime, *, plan_cohort: bool = False
) -> list[dict[str, Any]]:
    if not rows or len(rows) > MAX_SIGNALS:
        raise ValueError(
            f"Availability scenario requires 1-{MAX_SIGNALS} real store/product source rows."
        )
    if observed_at.tzinfo is None:
        raise ValueError("Scenario monitoring time must be timezone-aware.")
    result = []
    keys = set()
    snapshot_times = []
    for row in rows:
        store_id, product_id = int(row["store_id"]), int(row["product_id"])
        if store_id < 1 or product_id < 1:
            raise ValueError("Availability identifiers must be positive source IDs.")
        if plan_cohort and (
            store_id not in PLAN_STORE_IDS or product_id not in PLAN_PRODUCT_IDS
        ):
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
        snapshot_times.append(source_time)
        if source_time > observed_at:
            raise ValueError(
                f"Availability source for store {store_id} product {product_id} is "
                f"future-dated ({source_time.isoformat()} > {observed_at.isoformat()})."
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
        if forecast_time > observed_at:
            raise ValueError(
                f"Forecast source for store {store_id} product {product_id} is "
                f"future-dated ({forecast_time.isoformat()} > {observed_at.isoformat()})."
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
        result.append(
            {
                "event_ts": observed_at.astimezone(UTC).isoformat(),
                "source_as_of": None,
                "forecast_as_of": forecast_time.astimezone(UTC).isoformat(),
                "case_id": None,
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
                "plan_cohort": plan_cohort,
            }
        )
    # Every case carries the snapshot's as-of time. Older sources reported a
    # SKU's last ledger change, which dropped unchanged (often stocked-out) SKUs.
    snapshot = max(snapshot_times).astimezone(UTC).isoformat()
    for signal in result:
        signal["source_as_of"] = snapshot
        signal["case_id"] = case_identity(
            signal["store_id"], signal["product_id"], snapshot, signal["forecast_as_of"]
        )
    for label, field in (("Availability", "source_as_of"), ("Forecast", "forecast_as_of")):
        latest = max(datetime.fromisoformat(signal[field]) for signal in result)
        age_days = (observed_at - latest).total_seconds() / 86400
        if age_days > SIGNAL_MAX_AGE_DAYS:
            raise ValueError(
                f"{label} snapshot is stale: newest {field} {latest.isoformat()} is "
                f"{age_days:.1f} days old (limit {SIGNAL_MAX_AGE_DAYS}); refresh inputs "
                "and required ML first."
            )
    return result


def case_identity(store_id: int, product_id: int, source_as_of: str, forecast_as_of: str) -> str:
    """Deterministic case id for one position and one source/forecast snapshot."""

    return str(
        uuid5(
            NAMESPACE_URL,
            f"retail-availability:{store_id}:{product_id}:{source_as_of}:{forecast_as_of}",
        )
    )


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
