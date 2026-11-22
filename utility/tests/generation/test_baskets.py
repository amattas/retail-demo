"""Basket-level lines: complements, merged SKUs, one promotion with real thresholds."""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from datetime import date, timedelta
from itertools import combinations

import pytest

from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation import baskets
from retail_setup.generation.vocabulary import PROMO_CATALOG

pytestmark = pytest.mark.spark

PROFILE = load_dictionaries(default_dictionary_root(), "supercenter").profile
MINIMUMS = {code: minimum for code, _, _, minimum, _ in PROMO_CATALOG}
MONTHS = {code: months for code, _, months, _, _ in PROMO_CATALOG}


def test_promo_scopes_resolve_to_store_type_departments() -> None:
    scopes = baskets.promo_scopes(list(PROFILE.department_weights))
    assert scopes["SAVE20"] is None
    assert scopes["BACKTOSCHOOL20"] == ["Baby & Kids", "Clothing", "Electronics", "Office Supplies"]
    assert "Grocery" in (scopes["BOGO50"] or [])
    # A code whose departments the store type lacks applies to every department.
    assert baskets.promo_scopes(["Tools", "Lumber"])["BOGO50"] is None


def test_merge_lines_collapses_repeated_skus(spark) -> None:
    from pyspark.sql import functions as F

    frame = spark.sql("""
        SELECT array(
            named_struct('line_num', 1, 'product_id', 5L, 'quantity', 1, 'unit_cents', 100L,
                         'taxability', 'TAXABLE', 'department', 'Grocery'),
            named_struct('line_num', 2, 'product_id', 7L, 'quantity', 2, 'unit_cents', 300L,
                         'taxability', 'TAXABLE', 'department', 'Grocery'),
            named_struct('line_num', 3, 'product_id', 5L, 'quantity', 3, 'unit_cents', 100L,
                         'taxability', 'TAXABLE', 'department', 'Grocery')
        ) AS lines
    """)
    merged = frame.select(baskets.merge_lines(F.col("lines")).alias("m")).first().m
    assert [(line.line_num, line.product_id, line.quantity) for line in merged] == [
        (1, 5, 4),
        (2, 7, 2),
    ]


def _random_baskets(spark, count: int, month: int):
    """Offered baskets of one to six lines across the supercenter departments."""
    from pyspark.sql import functions as F

    departments = list(PROFILE.department_weights)
    lines = F.transform(
        F.sequence(F.lit(1), (F.abs(F.hash("id")) % 6 + 1).cast("int")),
        lambda i: F.struct(
            i.alias("line_num"),
            (F.col("id") * 10 + i).cast("long").alias("product_id"),
            (F.abs(F.hash("id", i)) % 3 + 1).cast("int").alias("quantity"),
            (F.abs(F.hash(i, "id")) % 6000 + 99).cast("long").alias("unit_cents"),
            F.lit("TAXABLE").alias("taxability"),
            F.element_at(
                F.array(*[F.lit(name) for name in departments]),
                (F.abs(F.hash("id", i)) % len(departments) + 1).cast("int"),
            ).alias("department"),
        ),
    )
    return spark.range(count).select(
        F.concat(F.lit("R"), F.col("id").cast("string")).alias("receipt_id"),
        lines.alias("lines"),
        F.lit(month).alias("month"),
    )


@pytest.mark.parametrize("month", [6, 11])
def test_one_code_per_basket_within_scope_season_and_threshold(spark, month) -> None:
    from pyspark.sql import functions as F

    from retail_setup.generation.runtime import seeded_draws

    draws = seeded_draws(4)
    frame = _random_baskets(spark, 3000, month)
    priced = frame.select(
        "lines",
        baskets.price_basket(
            draws,
            F.col("receipt_id"),
            F.col("lines"),
            F.col("month"),
            F.lit(True),
            F.lit(800).cast("long"),
            PROFILE,
        ).alias("priced"),
    ).collect()
    scopes = baskets.promo_scopes(list(PROFILE.department_weights))
    seen: Counter[str] = Counter()
    for row in priced:
        gross = sum(line.unit_cents * line.quantity for line in row.lines)
        codes = {line.promo_code for line in row.priced if line.promo_code}
        assert len(codes) == 1, row
        code = codes.pop()
        seen[code] += 1
        assert gross >= MINIMUMS[code]
        assert MONTHS[code] is None or month in MONTHS[code]
        for line in row.priced:
            in_scope = scopes[code] is None or line.department in scopes[code]
            if line.promo_code:
                assert in_scope and line.discount_cents > 0
            assert line.ext_cents == line.unit_cents * line.quantity - line.discount_cents
            assert line.tax_cents == (line.ext_cents * 800 * 100 + 500_000) // 1_000_000
            if code == "BOGO50" and line.promo_code:
                assert line.quantity >= 2
                assert line.discount_cents == math.floor(
                    line.quantity // 2 * line.unit_cents * 0.5 + 0.5
                )
    # Seasonal codes show up in their months, threshold codes on big baskets.
    assert ("BFRIDAY40" in seen) == (month == 11)
    assert seen["SUMMER25"] > 0 if month == 6 else seen["SUMMER25"] == 0
    assert seen["SAVE25"] > 0


def test_state_taxability_rules() -> None:
    from retail_setup.dictionaries.loader import load_tax_rules

    rules = baskets.tax_rules_by_state(load_tax_rules(default_dictionary_root()))
    # Alabama taxes groceries, Pennsylvania exempts clothing, New York exempts
    # clothing under $110 an item; other states exempt groceries only.
    assert baskets.tax_percent("NON_TAXABLE", 500, rules["AL"]) > 0
    assert baskets.tax_percent("NON_TAXABLE", 500, rules.get("CA", baskets.DEFAULT_TAX_RULE)) == 0
    assert baskets.tax_percent("REDUCED_RATE", 500_000, rules["PA"]) == 0
    assert baskets.tax_percent("REDUCED_RATE", 10_999, rules["NY"]) == 0
    assert baskets.tax_percent("REDUCED_RATE", 11_000, rules["NY"]) == 100
    # A $5,000 coat in a state without a rule pays the full rate, not half.
    assert baskets.tax_percent("REDUCED_RATE", 500_000) == 100
    assert baskets.tax_percent("TAXABLE", 100, rules["PA"]) == 100


def test_line_tax_applies_the_state_rule(spark) -> None:
    from pyspark.sql import functions as F

    from retail_setup.dictionaries.loader import load_tax_rules

    rules = baskets.tax_rules_by_state(load_tax_rules(default_dictionary_root()))
    cases = [
        (state, taxability, unit)
        for state in ("AL", "CA", "NY", "PA", "TN", None)
        for taxability in ("TAXABLE", "NON_TAXABLE", "REDUCED_RATE")
        for unit in (999, 25_000)
    ]
    rows = (
        spark.createDataFrame(cases, "state string, taxability string, unit long")
        .select(
            "state",
            "taxability",
            "unit",
            baskets.line_tax_cents(
                F.col("unit") * 2,
                F.col("taxability"),
                F.lit(900).cast("long"),
                baskets.tax_rule_column(rules, F.col("state")),
                F.col("unit"),
            ).alias("tax"),
        )
        .collect()
    )
    for row in rows:
        rule = rules.get(row.state, baskets.DEFAULT_TAX_RULE)
        percent = baskets.tax_percent(row.taxability, row.unit, rule)
        assert row.tax == (row.unit * 2 * 900 * percent + 500_000) // 1_000_000, row


def test_baskets_without_an_offer_get_no_code(spark) -> None:
    from pyspark.sql import functions as F

    from retail_setup.generation.runtime import seeded_draws

    priced = (
        _random_baskets(spark, 200, 6)
        .select(
            baskets.basket_promotion(
                seeded_draws(4),
                F.col("receipt_id"),
                F.col("lines"),
                F.col("month"),
                F.lit(False),
                PROFILE,
            ).alias("p")
        )
        .collect()
    )
    assert all(not line.promo_code and line.discount_cents == 0 for r in priced for line in r.p)


def test_complement_partners_are_fixed_same_department_products(spark) -> None:
    from pyspark.sql import functions as F

    products = spark.createDataFrame(
        [(i, "A" if i <= 6 else "B") for i in range(1, 11)] + [(11, "Solo")],
        "product_id long, department string",
    )
    pairs = {
        row.product_id: row.partner_id for row in baskets.complement_partners(products, 3).collect()
    }
    assert set(pairs) == set(range(1, 11))  # a one-product department has no partner
    for product, partner in pairs.items():
        assert partner != product
        assert (product <= 6) == (partner <= 6)
    again = baskets.complement_partners(products.orderBy(F.desc("product_id")), 3).collect()
    assert {row.product_id: row.partner_id for row in again} == pairs


# ---------------------------------------------------------------------------
# Generated receipts: affinities, thresholds and price response
# ---------------------------------------------------------------------------

START = date(2025, 4, 7)
DAYS = 84


@pytest.fixture(scope="module")
def sales(spark):
    from pyspark.sql import functions as F

    from retail_setup.config.generation import GenerationConfig
    from retail_setup.generation.dims import generate_dimensions
    from retail_setup.generation.pricing import PriceSchedule, shelf_price_struct
    from retail_setup.generation.receipts import generate_receipts_group

    cfg = GenerationConfig(
        start_date=START,
        end_date=START + timedelta(days=DAYS - 1),
        store_count=2,
        dc_count=1,
        seed=21,
        transactions_per_store_day=150,
    )
    dicts = load_dictionaries(default_dictionary_root(), "supercenter")
    dims = generate_dimensions(spark, dicts, cfg)
    group = generate_receipts_group(spark, dims, dicts.profile, cfg)
    receipts = {
        row.receipt_id_ext: row
        for row in group["fact_receipts"]
        .select("receipt_id_ext", "gross_subtotal_cents", "discount_cents", "subtotal_cents")
        .collect()
    }
    lines = (
        group["fact_receipt_lines"]
        .select(
            "receipt_id_ext",
            "event_date",
            "product_id",
            "quantity",
            "unit_cents",
            "ext_cents",
            "promo_code",
        )
        .collect()
    )
    products = baskets.basket_products(dims["dim_products"], cfg.seed)
    partners = {row.product_id: row.partner_id for row in products.collect()}
    departments = {row.product_id: row.department for row in products.collect()}
    schedule = PriceSchedule(cfg.seed, cfg.anchor_date)
    days = spark.range(DAYS).select(F.date_add(F.lit(START), F.col("id").cast("int")).alias("day"))
    prices = (
        products.crossJoin(days)
        .where(F.col("launch_date") <= F.col("day"))
        .select(
            "product_id",
            shelf_price_struct(
                schedule, F.col("product_id"), F.col("SalePrice"), F.col("inflation"), F.col("day")
            )["price_cents"].alias("price_cents"),
        )
        .groupBy("product_id", "price_cents")
        .count()
        .collect()
    )
    return {
        "receipts": receipts,
        "lines": lines,
        "partners": partners,
        "departments": departments,
        "price_days": prices,
    }


def test_generated_promotions_and_lines_are_coherent(sales) -> None:
    by_receipt: dict[str, list] = defaultdict(list)
    for line in sales["lines"]:
        by_receipt[line.receipt_id_ext].append(line)
    promo_receipts = 0
    for receipt_id, lines in by_receipt.items():
        products = [line.product_id for line in lines]
        assert len(products) == len(set(products)), receipt_id  # repeated SKUs merged
        codes = {line.promo_code for line in lines if line.promo_code}
        assert len(codes) <= 1, receipt_id
        header = sales["receipts"][receipt_id]
        assert header.gross_subtotal_cents - header.discount_cents == header.subtotal_cents
        if codes:
            promo_receipts += 1
            assert header.gross_subtotal_cents >= MINIMUMS[codes.pop()], receipt_id
    share = promo_receipts / len(by_receipt)
    print(f"STAT receipts with a promotion={share:.3f}")
    assert 0.10 < share < 0.25  # supercenter promo_rate 0.18


def test_complementary_products_co_occur_and_independent_ones_do_not(sales) -> None:
    baskets_by_receipt: dict[str, set[int]] = defaultdict(set)
    for line in sales["lines"]:
        baskets_by_receipt[line.receipt_id_ext].add(line.product_id)
    n = len(baskets_by_receipt)
    single: Counter[int] = Counter()
    pairs: Counter[tuple[int, int]] = Counter()
    for products in baskets_by_receipt.values():
        single.update(products)
        pairs.update(combinations(sorted(products), 2))

    def pooled_lift(candidates) -> float:
        observed = expected = 0.0
        for a, b in candidates:
            key = (min(a, b), max(a, b))
            observed += pairs[key]
            expected += single[a] * single[b] / n
        return observed / expected

    partners = {a: b for a, b in sales["partners"].items() if b is not None}
    complements = [(a, b) for a, b in partners.items() if single[a] and single[b]]
    # Independent pairs: products of different departments that are not partners.
    popular = [product for product, _ in single.most_common(300)]
    departments = sales["departments"]
    independent = [
        (a, b)
        for a, b in combinations(popular, 2)
        if departments[a] != departments[b] and partners.get(a) != b and partners.get(b) != a
    ]
    complement_lift, independent_lift = pooled_lift(complements), pooled_lift(independent)
    print(f"STAT lift complements={complement_lift:.2f} independent={independent_lift:.2f}")
    assert complement_lift > 3.0
    assert 0.6 < independent_lift < 1.4


def test_unit_sales_fall_as_shelf_prices_rise(sales) -> None:
    units: Counter[tuple[int, int]] = Counter()
    for line in sales["lines"]:
        units[(line.product_id, line.unit_cents)] += line.quantity
    cells: dict[int, list[tuple[float, float, float]]] = defaultdict(list)
    for row in sales["price_days"]:
        rate = (units[(row.product_id, row.price_cents)] + 0.5) / row["count"]
        cells[row.product_id].append((math.log(row.price_cents), math.log(rate), row["count"]))
    numerator = denominator = 0.0
    used = 0
    for product, rows in cells.items():
        if len(rows) < 2:
            continue
        weight = sum(w for _, _, w in rows)
        x_bar = sum(x * w for x, _, w in rows) / weight
        y_bar = sum(y * w for _, y, w in rows) / weight
        numerator += sum(w * (x - x_bar) * (y - y_bar) for x, y, w in rows)
        denominator += sum(w * (x - x_bar) ** 2 for x, _, w in rows)
        used += 1
    slope = numerator / denominator
    print(f"STAT within-SKU price elasticity={slope:.2f} over {used} SKUs")
    assert used > 100
    assert slope < -0.3, slope
