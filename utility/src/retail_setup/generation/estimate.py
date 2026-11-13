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
# Weighted avg lines/online-order from the 60/30/10 basket buckets
# (1-3, 2-5, 5-8 lines): 0.6*2 + 0.3*3.5 + 0.1*6.5 ~= 2.9.
_ONLINE_LINES_PER_ORDER = 2.9


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


@dataclass(frozen=True)
class CatalogExpectations:
    """Generator-faithful expectations derived from one store type's dictionaries."""

    basket_mean: float
    # Expected share of all units sold that go to the single most popular SKU.
    top_sku_unit_share: float


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
        taxes = taxes if isinstance(taxes, list) else next(iter(taxes.values()))
        tax_rate = sum(float(row["CombinedRate"]) for row in taxes) / len(taxes)
        department_weights = profile["department_weights"]
        line = taxable = total_weight = top_share = 0.0
        for department, department_weight in department_weights.items():
            items = [item for item in products if item["Department"] == department]
            if not items:
                continue
            repeat = 0.22 if _CONSUMABLE.search(department.lower()) else 0.04
            prices = [_EXPECTED_SALE_TO_BASE * float(item["BasePrice"]) for item in items]
            weights = [price**-_POPULARITY_PRICE_EXPONENT for price in prices]
            total = sum(weights)
            for item, price, weight in zip(items, prices, weights):
                unit_value = weight / total * price / (1.0 - repeat)
                if item["Department"] in {"Fresh", "Grocery"} and "Candy" not in item["Category"]:
                    tax_share = 0.0
                elif item["Department"] in {"Clothing", "Apparel"}:
                    tax_share = 0.5
                else:
                    tax_share = 1.0
                line += department_weight * unit_value
                taxable += department_weight * unit_value * tax_share
            total_weight += department_weight
            # Each base product is split across `brands_per_product` SKUs.
            top_share = max(
                top_share,
                department_weight * max(weights) / total / (1.0 - repeat) / brands_per_product,
            )
        line, taxable = line / total_weight, taxable / total_weight
        affordable = float(profile["avg_ticket_target"]) / (
            (line + taxable * tax_rate) * (1.0 - float(profile["promo_rate"]) * 0.20)
        )
        return CatalogExpectations(
            basket_mean=min(affordable, float(profile["basket_lambda"])),
            top_sku_unit_share=top_share / total_weight,
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


def estimate_record_counts(cfg: GenerationConfig) -> dict[str, int]:
    """Approximate per-group record counts for a generation config.

    Returns an ordered mapping of group name -> estimated row count, ending with a
    ``"Total (approx)"`` entry. Covers the dominant tables; smaller fact tables
    (sensors, store ops, marketing, inventory) are omitted, so the real total is
    somewhat higher.
    """

    days = (cfg.end_date - cfg.start_date).days + 1
    store_days = cfg.store_count * days
    basket = calibrated_basket_mean(cfg) or _basket_lambda(cfg)

    receipts = round(store_days * cfg.transactions_per_store_day)
    returns = round(receipts * cfg.return_rate)
    receipt_lines = round(receipts * basket)
    online_headers = round(cfg.online_orders_per_day * days)
    online_lines = round(online_headers * _ONLINE_LINES_PER_ORDER)
    payments = receipts + returns + online_headers

    groups = {
        "Customers (dim)": cfg.customer_count,
        "In-store receipts": receipts + returns,
        "Receipt lines": receipt_lines,
        "Online orders": online_headers,
        "Online order lines": online_lines,
        "Payments": payments,
    }
    groups["Total (approx)"] = sum(groups.values())
    return groups
