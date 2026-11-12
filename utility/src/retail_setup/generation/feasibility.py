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
- very large volumes exceed the 12-hour Fabric notebook activity limit.

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
from retail_setup.generation.estimate import catalog_expectations

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


@dataclass(frozen=True)
class Finding:
    level: Literal["error", "warning"]
    message: str


def assess(cfg: GenerationConfig, *, ml_required: bool, profile_name: str) -> list[Finding]:
    """Return errors (block deployment) and warnings (allowed, with a caution)."""

    findings: list[Finding] = []
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
