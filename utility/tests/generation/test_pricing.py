"""Pure-Python tests for the pricing helper (no Spark required).

Validates the datagen ``PricingCalculator`` rules ported into ``dims.compute_pricing``:
- MSRP = BasePrice +/-15%
- SalePrice = MSRP (60% of the time) OR MSRP discounted 5-35% (40%)
- Cost = 50-85% of SalePrice
- Always Cost < SalePrice <= MSRP
"""

import numpy as np

from retail_setup.generation.dims import compute_pricing


def test_invariant_holds_across_many_draws():
    rng = np.random.default_rng(123)
    for base in [0.99, 4.50, 19.99, 250.0, 1299.95]:
        for _ in range(2000):
            cost, msrp, sale = compute_pricing(base, rng)
            assert 0 < cost < sale <= msrp, (base, cost, sale, msrp)


def test_msrp_within_15_percent_of_base():
    rng = np.random.default_rng(7)
    base = 100.0
    msrps = [compute_pricing(base, rng)[1] for _ in range(5000)]
    # rounding to cents can nudge a hair past the bound; allow 1 cent slack
    assert min(msrps) >= base * 0.85 - 0.01
    assert max(msrps) <= base * 1.15 + 0.01
    assert abs(float(np.mean(msrps)) - base) < 1.0  # roughly centered on base


def test_sale_equals_msrp_about_60_percent():
    rng = np.random.default_rng(11)
    base = 50.0
    n = 20000
    equal = 0
    for _ in range(n):
        _, msrp, sale = compute_pricing(base, rng)
        if sale == msrp:
            equal += 1
    frac = equal / n
    assert 0.55 < frac < 0.65, frac  # ~60% keep MSRP as sale price


def test_discounted_sales_in_5_to_35_band():
    rng = np.random.default_rng(13)
    base = 80.0
    for _ in range(20000):
        _, msrp, sale = compute_pricing(base, rng)
        if sale < msrp:
            disc = 1 - sale / msrp
            # cents rounding widens the band slightly at the edges
            assert 0.04 < disc < 0.36, (msrp, sale, disc)


def test_cost_ratio_in_50_to_85_band():
    rng = np.random.default_rng(17)
    base = 120.0
    ratios = []
    for _ in range(20000):
        cost, _, sale = compute_pricing(base, rng)
        ratios.append(cost / sale)
    assert min(ratios) >= 0.50 - 0.02
    assert max(ratios) <= 0.85 + 0.01


def test_returns_python_floats():
    rng = np.random.default_rng(1)
    cost, msrp, sale = compute_pricing(10.0, rng)
    assert all(type(v) is float for v in (cost, msrp, sale))
