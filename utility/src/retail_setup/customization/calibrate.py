"""Deterministic basket calibration for custom business layouts.

The generator draws each basket line from a department by ``department_weights``
and then shrinks baskets until the average ticket approaches
``avg_ticket_target``. Copilot tends to answer with revenue-like department
shares ("appliances are 5% of sales"). Read as line shares, those make
big-ticket items a large share of all lines, so with a realistic ticket target
almost every basket has one item; market-basket ML then has no pairs and
configuration feasibility rejects the layout.

:func:`calibrate_department_weights` keeps the ticket target and the catalog,
and moves the weights from revenue share toward line share
(``weight * line_value ** -alpha``) only as far as needed to reach a realistic
basket. The result depends only on the spec, so the same spec always produces
the same dictionaries.
"""

from __future__ import annotations

from retail_setup.generation.estimate import affordable_basket, department_lines

# Items per basket a custom layout should afford at its ticket target (capped
# at its basket_lambda). The hardware preset affords about 3.2 and luxury,
# whose ceiling is 1.6, about 1.56; two items keeps market-basket pairs and
# demand per SKU well inside the feasibility limits.
MIN_CUSTOM_BASKET_ITEMS = 2.0
# Bump when the calibration changes so saved materializations are rebuilt.
CALIBRATION_VERSION = 1
_SEARCH_STEPS = 48
_WEIGHT_DIGITS = 6


def _shifted(weights: dict[str, float], values: dict[str, float], alpha: float) -> dict[str, float]:
    shifted = {d: w * values[d] ** -alpha for d, w in weights.items()}
    total = sum(shifted.values())
    return {d: w / total for d, w in shifted.items()}


def calibrate_department_weights(
    profile: dict, products: list[dict], tax_rate: float
) -> dict[str, float]:
    """Return department weights whose expected basket reaches a realistic size.

    Weights are returned unchanged when the ticket target already affords
    ``min(basket_lambda, MIN_CUSTOM_BASKET_ITEMS)`` items. Otherwise the
    smallest shift toward line share that reaches it is used; when even full
    line share cannot reach it, full line share (the largest basket the
    catalog allows) is used.
    """

    weights = {d: float(w) for d, w in profile["department_weights"].items()}
    lines = department_lines(products)
    if set(weights) - set(lines):
        return weights
    target = min(float(profile["basket_lambda"]), MIN_CUSTOM_BASKET_ITEMS)
    values = {d: lines[d].value for d in weights}

    def basket(alpha: float) -> float:
        return affordable_basket(profile, _shifted(weights, values, alpha), lines, tax_rate)

    if basket(0.0) >= target:
        return weights
    low, high = 0.0, 1.0
    if basket(high) >= target:
        for _ in range(_SEARCH_STEPS):
            middle = (low + high) / 2.0
            if basket(middle) >= target:
                high = middle
            else:
                low = middle
    calibrated = _shifted(weights, values, high)
    # Rounded for stable JSON; every department keeps a positive weight.
    return {d: max(round(w, _WEIGHT_DIGITS), 10.0**-_WEIGHT_DIGITS) for d, w in calibrated.items()}
