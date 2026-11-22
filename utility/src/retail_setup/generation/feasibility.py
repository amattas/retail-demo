"""Configuration feasibility: settings combinations known to fail a deployment.

Checked by ``retail-setup configure``, ``render``, and the setup interface's
configuration page (which disables Start on any error). Every check is
Spark-free and uses generator-faithful expectations from ``estimate``.

Covered failure modes:

- required ML needs at least 540 days of history (churn feature window, 90-day
  forward labels, and purged train/calibration/test splits);
- required market-basket recommendations need multi-item baskets; a store
  type whose ticket target is below one item's cost produces only single-item
  receipts, and too few multi-item baskets leave no repeated product pairs;
- required demand forecasting needs at least one store/product selling
  ``MIN_DAILY_SALES`` units a day;
- very large volumes exceed the 12-hour Fabric notebook activity limit, both
  in receipt lines and in the inventory simulation, which steps every
  store-SKU series through every day of history in Python;
- the largest semantic-model table exceeds the Direct Lake on OneLake row
  guardrail of small capacities (a warning naming the capacity it needs) or of
  every capacity (an error): Direct Lake on OneLake has no DirectQuery
  fallback.

Not covered, because they can't be predicted without running the simulation or
are already guaranteed by the generator: customer segmentation and churn (the
generator keeps at least ``MIN_CUSTOMER_COUNT`` customers), stockout label
balance (simulated inventory), and fixed-width ID overflow (impossible within
``store_count`` <= 2000 and ``months`` <= 120).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.estimate import (
    DIRECT_LAKE_ROW_LIMITS,
    catalog_expectations,
    direct_lake_capacity,
    largest_tables,
)

MIN_REQUIRED_ML_HISTORY_DAYS = 540
# Average items beyond the first; below this baskets are effectively single-item.
MIN_EXTRA_BASKET_ITEMS = 0.1
# Multi-item baskets needed for market basket to find repeated product pairs.
MIN_MULTI_ITEM_BASKETS = 1_000
# 06-ml-demand-forecast MIN_DAILY_SALES default.
MIN_DAILY_SALES = 0.5
# Negative-binomial shape the generator uses for additional basket items.
_BASKET_SHAPE = 3.0
# Measured live: a 16-node Spark pool generated ~58M receipt lines with the
# whole setup pipeline in about an hour (~3.5M lines per node-hour). Notebook
# activities time out after 12 hours.
LINES_PER_NODE_HOUR = 3_500_000
_ACTIVITY_HOURS = 12
_LARGEST_POOL_NODES = 16
_SMALLEST_POOL_NODES = 2  # F64
# Inventory simulation throughput: store-SKU days stepped per node-hour by the
# Python policy (mapInArrow workers, one series at a time). A conservative
# planning figure, not a measured limit.
INVENTORY_SKU_DAYS_PER_NODE_HOUR = 1_000_000_000


@dataclass(frozen=True)
class Finding:
    level: Literal["error", "warning"]
    message: str


def assess(cfg: GenerationConfig, *, ml_required: bool, profile_name: str) -> list[Finding]:
    """Return errors (block deployment) and warnings (allowed, with a caution)."""

    findings: list[Finding] = []
    # GenerationConfig's validators always resolve both dates.
    assert cfg.start_date is not None and cfg.end_date is not None
    days = (cfg.end_date - cfg.start_date).days + 1
    receipts = cfg.store_count * days * cfg.transactions_per_store_day
    expectations = catalog_expectations(cfg)

    if ml_required and days < MIN_REQUIRED_ML_HISTORY_DAYS:
        findings.append(
            Finding(
                "error",
                f"profile {profile_name!r} requires at least {MIN_REQUIRED_ML_HISTORY_DAYS} "
                f"days of history for required ML; the configured window has {days}. "
                "Use 18 or more months.",
            )
        )

    if expectations is not None and ml_required:
        extra = expectations.basket_mean - 1.0
        if extra < MIN_EXTRA_BASKET_ITEMS:
            findings.append(
                Finding(
                    "error",
                    f"store type {cfg.store_type!r} generates single-item baskets: its "
                    "average-ticket target is below the cost of one item, so market-basket "
                    f"recommendations (required ML for profile {profile_name!r}) cannot be "
                    "produced. Choose another store type, or the core profile.",
                )
            )
        else:
            multi_share = 1.0 - (_BASKET_SHAPE / (_BASKET_SHAPE + extra)) ** _BASKET_SHAPE
            multi_baskets = receipts * multi_share
            if multi_baskets < MIN_MULTI_ITEM_BASKETS:
                findings.append(
                    Finding(
                        "error",
                        f"only about {multi_baskets:,.0f} multi-item baskets would be generated; "
                        f"market-basket recommendations need at least "
                        f"{MIN_MULTI_ITEM_BASKETS:,}. Add stores or months.",
                    )
                )
        top_units = (
            cfg.transactions_per_store_day
            * expectations.basket_mean
            * expectations.top_sku_unit_share
        )
        if top_units < MIN_DAILY_SALES:
            findings.append(
                Finding(
                    "error",
                    f"no product would sell {MIN_DAILY_SALES} units a day in a store (best "
                    f"is about {top_units:.2f}), so demand forecasting (required ML) has no "
                    "eligible store/product. Choose a store type with a smaller catalog.",
                )
            )

    if expectations is not None and expectations.sku_count:
        series = cfg.store_count * expectations.sku_count
        sku_days = series * days
        largest = INVENTORY_SKU_DAYS_PER_NODE_HOUR * _LARGEST_POOL_NODES * _ACTIVITY_HOURS
        smallest = INVENTORY_SKU_DAYS_PER_NODE_HOUR * _SMALLEST_POOL_NODES * _ACTIVITY_HOURS
        if sku_days > 2 * largest:
            findings.append(
                Finding(
                    "error",
                    f"the inventory simulation would step {series:,} store-SKU series through "
                    f"{days:,} days (about {sku_days / 1e9:,.0f} billion steps), more than "
                    f"Fabric can run within its {_ACTIVITY_HOURS}-hour notebook limit. Reduce "
                    "stores, months, or brands per product.",
                )
            )
        elif sku_days > smallest:
            findings.append(
                Finding(
                    "warning",
                    f"the inventory simulation steps {series:,} store-SKU series through "
                    f"{days:,} days (about {sku_days / 1e9:,.1f} billion steps); it can take "
                    "several hours on an F64 capacity. Use a larger capacity or fewer stores, "
                    "months, or brands per product.",
                )
            )

    if expectations is not None:
        name, rows = max(largest_tables(cfg).items(), key=lambda item: item[1])
        capacity = direct_lake_capacity(rows)
        smallest_tier, smallest_limit = DIRECT_LAKE_ROW_LIMITS[0]
        largest_limit = DIRECT_LAKE_ROW_LIMITS[-1][1]
        if capacity is None:
            findings.append(
                Finding(
                    "error",
                    f"{name} would hold about {rows / 1e9:,.1f} billion rows, above the Direct "
                    f"Lake row guardrail of every capacity ({largest_limit / 1e9:,.0f} billion "
                    "rows per table), so the semantic model could not load it. Reduce stores "
                    "or months.",
                )
            )
        elif rows > smallest_limit:
            findings.append(
                Finding(
                    "warning",
                    f"{name} would hold about {rows / 1e6:,.0f} million rows, above the Direct "
                    f"Lake row guardrail of {smallest_tier} capacities "
                    f"({smallest_limit / 1e6:,.0f} million rows per table). Reports need a "
                    f"capacity of {capacity} or larger (Direct Lake on OneLake has no "
                    "DirectQuery fallback); otherwise reduce stores or months.",
                )
            )

    if expectations is not None:
        lines = receipts * expectations.basket_mean
        largest = LINES_PER_NODE_HOUR * _LARGEST_POOL_NODES * _ACTIVITY_HOURS
        smallest = LINES_PER_NODE_HOUR * _SMALLEST_POOL_NODES * _ACTIVITY_HOURS
        if lines > 2 * largest:
            findings.append(
                Finding(
                    "error",
                    f"about {lines / 1e9:,.1f} billion receipt lines is more than Fabric can "
                    f"generate within its {_ACTIVITY_HOURS}-hour notebook limit, even on the "
                    "largest Spark pool. Reduce stores or months.",
                )
            )
        elif lines > smallest:
            findings.append(
                Finding(
                    "warning",
                    f"about {lines / 1e6:,.0f} million receipt lines can take several hours "
                    f"and may exceed the {_ACTIVITY_HOURS}-hour notebook limit on an F64 "
                    "capacity. Use a larger capacity or fewer stores or months.",
                )
            )
    return findings
