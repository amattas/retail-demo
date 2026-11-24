"""Rough record-count estimates for a generation config (no Spark required).

``retail-setup configure`` shows these so the operator can sanity-check the
output volume (and the time it implies) before committing to a run. The numbers
are deliberately approximate: receipt volume is shaped by hourly/daily/monthly
weights and per-store multipliers, but the dominant driver is
``store_count * days * transactions_per_store_day``.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from retail_setup.config.generation import GenerationConfig

# Fallback average items/basket if the store profile can't be read.
_DEFAULT_BASKET_LAMBDA = 12.0


def _basket_lambda(cfg: GenerationConfig) -> float:
    """Average items per in-store basket for the configured store type."""

    profile = cfg.resolved_dictionary_root / cfg.store_type / "profile.json"
    try:
        return float(json.loads(profile.read_text(encoding="utf-8"))["basket_lambda"])
    except (OSError, ValueError, KeyError, TypeError):
        return _DEFAULT_BASKET_LAMBDA


# Mirrors of the generator's basket calibration inputs:
# behavior.retail_catalog (popularity ~ SalePrice^-1.15, unit value includes
# repeat purchases), behavior.repeat_purchase_probability, dims taxability, and
# dims.compute_pricing (expected SalePrice ~= 0.92 x BasePrice).
_POPULARITY_PRICE_EXPONENT = 1.15
_EXPECTED_SALE_TO_BASE = 0.92
_CONSUMABLE = re.compile("grocery|fresh|food|household|personal|health|beauty|pet|baby")


# Mirrors of the visit and beacon model (stream_calendar.VISIT_CONVERSION_CALIBRATION,
# vocabulary.BLE_DETECTED_SHARE, BLE_MAX_MIDDLE_ZONES, BLE_PINGS_PER_ZONE): a
# detected visit passes the entrance, one to three departments, checkout when
# it buys and the exit, with two to five pings a zone.
_VISIT_CONVERSION = 0.225
_BLE_DETECTED_SHARE = 0.25
_BLE_MAX_MIDDLE_ZONES = 3
_BLE_PINGS_PER_ZONE = (2, 5)
_BLE_ZONES_PER_VISIT = 2 + (_BLE_MAX_MIDDLE_ZONES + 1) / 2 + _VISIT_CONVERSION
BLE_PINGS_PER_RECEIPT = (
    _BLE_DETECTED_SHARE / _VISIT_CONVERSION * _BLE_ZONES_PER_VISIT * sum(_BLE_PINGS_PER_ZONE) / 2
)
# Share of online lines a store fulfils (ship-from-store and BOPIS,
# vocabulary.ONLINE_FULFILLMENT_MIX); they post to the store ledger.
_STORE_FULFILLED_ONLINE_SHARE = 0.40

# Direct Lake on OneLake row guardrails per table, by Fabric capacity SKU.
# Above a table's guardrail the semantic model cannot load it, and Direct
# Lake on OneLake has no DirectQuery fallback.
DIRECT_LAKE_ROW_LIMITS: tuple[tuple[str, int], ...] = (
    ("F2-F32", 300_000_000),
    ("F64", 1_500_000_000),
    ("F128", 3_000_000_000),
    ("F256", 6_000_000_000),
    ("F512", 12_000_000_000),
    ("F1024", 24_000_000_000),
)


@dataclass(frozen=True)
class CatalogExpectations:
    """Generator-faithful expectations derived from one store type's dictionaries."""

    basket_mean: float
    # Expected share of all units sold that go to the single most popular SKU.
    top_sku_unit_share: float
    # SKUs in dim_products: each base product under up to brands_per_product brands.
    sku_count: int = 0


@dataclass(frozen=True)
class DepartmentLine:
    """Expected value of one basket line drawn from a department."""

    # Line value including repeat purchases of the same item.
    value: float
    # The taxable part of ``value``.
    taxable: float
    # Share of the department's units that go to its most popular product.
    top_unit_share: float


def department_lines(products: list[dict]) -> dict[str, DepartmentLine]:
    """Generator-faithful expected line value for every department in ``products``."""

    lines: dict[str, DepartmentLine] = {}
    for department in dict.fromkeys(item["Department"] for item in products):
        items = [item for item in products if item["Department"] == department]
        repeat = 0.22 if _CONSUMABLE.search(department.lower()) else 0.04
        prices = [_EXPECTED_SALE_TO_BASE * float(item["BasePrice"]) for item in items]
        weights = [price**-_POPULARITY_PRICE_EXPONENT for price in prices]
        total = sum(weights)
        value = taxable = 0.0
        for item, price, weight in zip(items, prices, weights):
            unit_value = weight / total * price / (1.0 - repeat)
            # Groceries are untaxed and clothing fully taxed under the
            # default state rule (generation.baskets.line_tax_cents).
            grocery = item["Department"] in {"Fresh", "Grocery"}
            tax_share = 0.0 if grocery and "Candy" not in item["Category"] else 1.0
            value += unit_value
            taxable += unit_value * tax_share
        lines[department] = DepartmentLine(value, taxable, max(weights) / total / (1.0 - repeat))
    return lines


def mean_tax_rate(taxes: list[dict] | dict) -> float:
    """Average combined sales-tax rate across ``_shared/tax_rates.json`` rows."""

    rows = taxes if isinstance(taxes, list) else next(iter(taxes.values()))
    return sum(float(row["CombinedRate"]) for row in rows) / len(rows)


def affordable_basket(
    profile: dict,
    department_weights: dict[str, float],
    lines: dict[str, DepartmentLine],
    tax_rate: float,
) -> float:
    """Items per basket the ticket target buys when lines follow ``department_weights``.

    The generator shrinks baskets toward ``avg_ticket_target``, so this (capped
    at ``basket_lambda``) is the expected basket size.
    """

    weighted = {d: w for d, w in department_weights.items() if d in lines}
    total_weight = sum(weighted.values())
    line = sum(w * lines[d].value for d, w in weighted.items()) / total_weight
    taxable = sum(w * lines[d].taxable for d, w in weighted.items()) / total_weight
    return float(profile["avg_ticket_target"]) / (
        (line + taxable * tax_rate) * (1.0 - float(profile["promo_rate"]) * 0.20)
    )


@lru_cache(maxsize=32)
def _catalog_expectations(
    root: str, store_type: str, brands_per_product: int
) -> CatalogExpectations | None:
    base = Path(root)
    try:
        profile = json.loads((base / store_type / "profile.json").read_text("utf-8"))
        products = json.loads((base / store_type / "products.json").read_text("utf-8"))
        taxes = json.loads((base / "_shared" / "tax_rates.json").read_text("utf-8"))
        products = products if isinstance(products, list) else next(iter(products.values()))
        tax_rate = mean_tax_rate(taxes)
        department_weights = profile["department_weights"]
        lines = department_lines(products)
        total_weight = sum(w for d, w in department_weights.items() if d in lines)
        # Each base product is split across `brands_per_product` SKUs.
        top_share = max(
            (
                w * lines[d].top_unit_share / brands_per_product
                for d, w in department_weights.items()
                if d in lines
            ),
            default=0.0,
        )
        brands = json.loads((base / store_type / "brands.json").read_text("utf-8"))
        brands = brands if isinstance(brands, list) else next(iter(brands.values()))
        affordable = affordable_basket(profile, department_weights, lines, tax_rate)
        return CatalogExpectations(
            basket_mean=min(affordable, float(profile["basket_lambda"])),
            top_sku_unit_share=top_share / total_weight,
            sku_count=len(products) * max(1, min(brands_per_product, len(brands))),
        )
    except (OSError, ValueError, KeyError, TypeError, ZeroDivisionError, StopIteration):
        return None


def catalog_expectations(cfg: GenerationConfig) -> CatalogExpectations | None:
    """Expectations for `cfg`'s store type, or None when dictionaries are unreadable."""

    return _catalog_expectations(
        str(cfg.resolved_dictionary_root), cfg.store_type, cfg.brands_per_product
    )


def calibrated_basket_mean(cfg: GenerationConfig) -> float | None:
    """Expected items per in-store basket after the generator's ticket calibration.

    The generator shrinks baskets so the average ticket approaches the store
    type's ``avg_ticket_target``: mean = min(target / expected line cost,
    basket_lambda). When one line already costs more than the target, every
    basket has exactly one item. Returns None when the dictionaries are
    unreadable.
    """

    expectations = catalog_expectations(cfg)
    return None if expectations is None else expectations.basket_mean


def inventory_series(cfg: GenerationConfig) -> int | None:
    """Store-SKU series the inventory simulation steps through every day."""
    expectations = catalog_expectations(cfg)
    return None if expectations is None else cfg.store_count * expectations.sku_count


def largest_tables(cfg: GenerationConfig) -> dict[str, int]:
    """Approximate rows of the biggest semantic-model tables."""
    counts = estimate_record_counts(cfg)
    return {
        "fact_ble_pings": counts["BLE pings"],
        "fact_receipt_lines": counts["Receipt lines"],
        "fact_store_inventory_txn": counts["Store inventory moves"],
    }


def direct_lake_capacity(rows: int) -> str | None:
    """The smallest capacity whose Direct Lake guardrail holds ``rows`` in one table."""
    for capacity, limit in DIRECT_LAKE_ROW_LIMITS:
        if rows <= limit:
            return capacity
    return None


def direct_lake_summary(cfg: GenerationConfig) -> str:
    """One line naming the largest table and the capacity Direct Lake needs for it."""
    name, rows = max(largest_tables(cfg).items(), key=lambda item: item[1])
    capacity = direct_lake_capacity(rows)
    if capacity == DIRECT_LAKE_ROW_LIMITS[0][0]:
        needs = "fits every capacity"
    elif capacity is not None:
        needs = f"needs {capacity} or larger"
    else:
        needs = "exceeds every capacity's guardrail"
    return f"Direct Lake: largest table {name} ~ {rows:,} rows; {needs}"


def estimate_record_counts(cfg: GenerationConfig) -> dict[str, int]:
    """Approximate per-group record counts for a generation config.

    Returns an ordered mapping of group name -> estimated row count, ending with a
    ``"Total (approx)"`` entry. Covers the dominant tables; smaller fact tables
    (foot traffic, zone changes, store ops, marketing, DC and truck ledgers) are
    omitted, so the real total is somewhat higher.
    """

    days = (cfg.end_date - cfg.start_date).days + 1
    store_days = cfg.store_count * days
    basket = calibrated_basket_mean(cfg) or _basket_lambda(cfg)

    receipts = round(store_days * cfg.transactions_per_store_day)
    returns = round(receipts * cfg.return_rate)
    receipt_lines = round(receipts * basket)
    online_headers = round(cfg.online_orders_per_day * days)
    # Online baskets are sized to the same ticket target as store baskets.
    online_lines = round(online_headers * basket)
    payments = receipts + returns + online_headers
    # Beacon pings of detected visits (buyers and browsers) outgrow receipt lines.
    ble_pings = round(receipts * BLE_PINGS_PER_RECEIPT)
    # The store ledger: one move per sold or store-fulfilled line plus each
    # store-SKU's opening stock (deliveries and returns add a few more).
    series = inventory_series(cfg) or 0
    store_moves = round(receipt_lines + online_lines * _STORE_FULFILLED_ONLINE_SHARE + series)

    groups = {
        "Customers (dim)": cfg.customer_count,
        "In-store receipts": receipts + returns,
        "Receipt lines": receipt_lines,
        "Online orders": online_headers,
        "Online order lines": online_lines,
        "Payments": payments,
        "BLE pings": ble_pings,
        "Store inventory moves": store_moves,
    }
    groups["Total (approx)"] = sum(groups.values())
    return groups
