"""Rough record-count estimates for a generation config (no Spark required).

``retail-setup configure`` shows these so the operator can sanity-check the
output volume (and the time it implies) before committing to a run. The numbers
are deliberately approximate: receipt volume is shaped by hourly/daily/monthly
weights and per-store multipliers, but the dominant driver is
``store_count * days * transactions_per_store_day``.
"""

from __future__ import annotations

import json

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


def estimate_record_counts(cfg: GenerationConfig) -> dict[str, int]:
    """Approximate per-group record counts for a generation config.

    Returns an ordered mapping of group name -> estimated row count, ending with a
    ``"Total (approx)"`` entry. Covers the dominant tables; smaller fact tables
    (sensors, store ops, marketing, inventory) are omitted, so the real total is
    somewhat higher.
    """

    days = (cfg.end_date - cfg.start_date).days + 1
    store_days = cfg.store_count * days
    basket = _basket_lambda(cfg)

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
