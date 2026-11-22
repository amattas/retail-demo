"""Basket-level line handling shared by history and the live stream.

Both producers build a purchase's lines as an array of structs and pass it
through the same steps:

1. :func:`with_complements` - each line may bring its complementary product
   (a fixed partner in the same department, see :func:`complement_partners`),
   so baskets carry real product affinities.
2. :func:`merge_lines` - repeated SKUs collapse into one line with the summed
   quantity, numbered in first-seen order.
3. :func:`basket_promotion` - an offered purchase gets at most one promotion
   code: one of the ``PROMO_CATALOG`` codes in season whose minimum is met by
   the basket subtotal and that has an eligible line. The code discounts only
   the lines in its department scope (``promo_scopes``); BOGO discounts every
   second unit of eligible lines with two or more units.

Line structs carry ``line_num``, ``product_id``, ``quantity``, ``unit_cents``,
``taxability`` and ``department``; complements also need ``partner_id``,
``partner_cents``, ``partner_taxability`` and ``partner_ok`` (the partner is
launched).
"""

from collections.abc import Callable, Iterable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.dictionaries.models import StoreTypeProfile, TaxRuleEntry
from retail_setup.generation.pricing import elasticity_column, inflation_column
from retail_setup.generation.runtime import bind, case_when, seeded_draws
from retail_setup.generation.vocabulary import PROMO_CATALOG

# Share of lines that bring their complementary product along.
COMPLEMENT_RATE = 0.15
# Promotion code -> department-name tokens it applies to; None applies to all.
# A code whose tokens match none of a store type's departments applies to all.
PROMO_SCOPES: dict[str, list[str] | None] = {
    "SAVE10": None,
    "SAVE15": None,
    "SAVE20": None,
    "SAVE25": None,
    "CLEARANCE30": [
        "seasonal", "clothing", "apparel", "footwear", "accessor", "home", "toy",
        "electronic", "sport", "garden", "lawn",
    ],
    "BOGO50": [
        "grocery", "fresh", "food", "household", "personal", "health", "beauty", "pet", "baby",
    ],
    "NEWYEAR15": ["health", "sport", "beauty", "fitness"],
    "SPRINGSALE20": ["garden", "lawn", "home", "clothing", "apparel", "paint"],
    "SUMMER25": ["garden", "lawn", "sport", "seasonal", "outdoor", "clothing", "apparel"],
    "BACKTOSCHOOL20": ["office", "clothing", "apparel", "electronic", "baby", "kids"],
    "BFRIDAY30": [
        "electronic", "toy", "home", "clothing", "apparel", "jewel", "accessor", "tool",
    ],
    "BFRIDAY40": [
        "electronic", "toy", "home", "clothing", "apparel", "jewel", "accessor", "tool",
    ],
    "HOLIDAY20": ["toy", "seasonal", "electronic", "jewel", "fragrance", "home", "accessor"],
}  # fmt: skip
# Seasonal codes are favored while in season.
SEASONAL_CODE_WEIGHT = 3.0


def promo_scopes(departments: list[str]) -> dict[str, list[str] | None]:
    """Each code's department names among ``departments`` (None = every department)."""
    scopes: dict[str, list[str] | None] = {}
    for code, *_rest in PROMO_CATALOG:
        tokens = PROMO_SCOPES.get(code)
        matched = (
            [name for name in departments if any(t in name.lower() for t in tokens)]
            if tokens is not None
            else []
        )
        scopes[code] = sorted(matched) if matched else None
    return scopes


def receipt_offer(draws: seeded_draws, key: Column, promo_rate: float) -> Column:
    """Whether a purchase presents a promotion (``promo_rate`` of purchases)."""
    return draws.u([key], "offer") < F.lit(promo_rate)


def _in_scope(department: Column, scope: list[str] | None) -> Column:
    return F.lit(True) if scope is None else department.isin(*scope)


def _discountable(scope: list[str] | None, kind: str) -> Callable[[Column], Column]:
    """Whether a line can take a code of this scope and kind (BOGO needs 2+ units)."""

    def check(line: Column) -> Column:
        units = F.lit(True) if kind != "BOGO" else line["quantity"] >= 2
        return _in_scope(line["department"], scope) & units

    return check


def basket_promotion(
    draws: seeded_draws,
    key: Column,
    lines: Column,
    month: Column,
    offer: Column,
    profile: StoreTypeProfile,
) -> Column:
    """Lines with ``promo_code`` and ``discount_cents`` from one basket-level code.

    The code is drawn among the codes eligible for the basket (in season,
    basket gross subtotal at or above the code's minimum, at least one
    discountable line); lines outside its scope, and BOGO lines with one unit,
    keep no code and no discount. Purchases without an offer get no codes.
    """
    scopes = promo_scopes(list(profile.department_weights))
    gross = F.aggregate(
        lines, F.lit(0).cast("long"), lambda acc, line: acc + line["unit_cents"] * line["quantity"]
    )

    def choose(basket_cents: Column) -> Column:
        candidates = []
        for code, _pct, months, min_cents, kind in PROMO_CATALOG:
            eligible = F.exists(lines, _discountable(scopes[code], kind)) & (
                basket_cents >= F.lit(min_cents)
            )
            if months is not None:
                eligible = eligible & month.isin(*months)
            weight = SEASONAL_CODE_WEIGHT if months is not None else 1.0
            # Weighted random choice: the largest u ** (1 / weight) among eligible codes.
            score = F.when(eligible, F.pow(draws.u([key, F.lit(code)], "pcode"), 1.0 / weight))
            candidates.append(F.struct(score.alias("score"), F.lit(code).alias("code")))
        eligible_codes = F.filter(F.array(*candidates), lambda entry: entry["score"].isNotNull())
        return F.when(offer, F.array_max(eligible_codes)["code"])

    def priced(line: Column, code: Column) -> Column:
        line_cents = line["unit_cents"] * line["quantity"]
        discount: Column = F.lit(0).cast("long")
        for name, pct, _months, _min_cents, kind in PROMO_CATALOG:
            applies = (code == F.lit(name)) & _in_scope(line["department"], scopes[name])
            if kind == "BOGO":
                amount = F.floor(
                    F.floor(line["quantity"] / 2) * line["unit_cents"] * pct / 100.0 + 0.5
                )
            else:
                amount = F.floor(line_cents * pct / 100.0 + 0.5)
            discount = F.when(applies, amount.cast("long")).otherwise(discount)
        discount = F.coalesce(discount, F.lit(0).cast("long"))
        return line.withField("promo_code", F.when(discount > 0, code)).withField(
            "discount_cents", discount
        )

    # The subtotal and the chosen code are evaluated once per basket and bound
    # to lambda variables, which keeps the expression (and planning) small.
    code = bind(gross, choose)
    return bind(code, lambda chosen: F.transform(lines, lambda line: priced(line, chosen)))


def merge_lines(lines: Column) -> Column:
    """Collapse repeated SKUs into one line (summed quantity), renumbered 1..n."""
    products = F.array_distinct(F.transform(lines, lambda line: line["product_id"]))

    def merged(product_id: Column, index: Column) -> Column:
        same = F.filter(lines, lambda line: line["product_id"] == product_id)
        quantity = F.aggregate(same, F.lit(0), lambda acc, line: acc + line["quantity"].cast("int"))
        first = F.element_at(same, 1)
        return first.withField("line_num", (index + 1).cast("int")).withField(
            "quantity", quantity.cast("int")
        )

    return F.transform(products, merged)


def with_complements(draws: seeded_draws, key: Column, lines: Column) -> Column:
    """Lines followed, for ``COMPLEMENT_RATE`` of them, by one unit of their partner."""

    def expand(line: Column) -> Column:
        brings = line["partner_ok"] & (
            draws.u([key, line["line_num"]], "complement") < F.lit(COMPLEMENT_RATE)
        )
        partner = (
            line.withField("product_id", line["partner_id"])
            .withField("quantity", F.lit(1))
            .withField("unit_cents", line["partner_cents"])
            .withField("taxability", line["partner_taxability"])
        )
        return F.when(brings, F.array(line, partner)).otherwise(F.array(line))

    return F.flatten(F.transform(lines, expand))


def complement_partners(products: DataFrame, seed: int) -> DataFrame:
    """``product_id, partner_id``: a fixed complementary product in the same department.

    Products are ranked within their department by a seeded hash; a product's
    partner sits a seeded, non-zero offset further round the ranking, so each
    pair is stable for the whole history and the live stream. Departments
    with one product have no partner.
    """
    draws = seeded_draws(seed)
    ranked = products.select(
        F.col("product_id").cast("long").alias("product_id"), "department"
    ).withColumn(
        "_rank",
        F.row_number().over(
            Window.partitionBy("department").orderBy(
                draws.h64([F.col("product_id")], "complement-rank"), "product_id"
            )
        ),
    )
    ranked = ranked.withColumn("_size", F.count("*").over(Window.partitionBy("department")))
    offset = draws.h64([F.col("product_id")], "complement-offset") % (F.col("_size") - 1) + 1
    wanted = ranked.where(F.col("_size") > 1).withColumn(
        "_partner_rank", F.pmod(F.col("_rank") - 1 + offset, F.col("_size")) + 1
    )
    partners = ranked.select(
        "department",
        F.col("_rank").alias("_partner_rank"),
        F.col("product_id").alias("partner_id"),
    )
    return wanted.join(partners, ["department", "_partner_rank"]).select("product_id", "partner_id")


def basket_products(dim_products: DataFrame, seed: int) -> DataFrame:
    """Catalog input of ``behavior.retail_catalog``: SKUs with their complementary partner.

    ``product_id, SalePrice, taxability, department, launch_date, inflation,
    elasticity`` plus the partner's ``partner_id, partner_price, partner_taxability, partner_launch``
    (NULL for a department of one product).
    """
    products = dim_products.select(
        F.col("ID").cast("long").alias("product_id"),
        "SalePrice",
        "taxability",
        F.col("Department").alias("department"),
        F.to_date("LaunchDate").alias("launch_date"),
        # Department price trend and elasticity (pricing.DEPARTMENT_PRICING),
        # resolved once per SKU here instead of in every price expression.
        inflation_column(F.col("Department")).alias("inflation"),
        elasticity_column(F.col("Department")).alias("elasticity"),
    )
    partner = products.select(
        F.col("product_id").alias("partner_id"),
        F.col("SalePrice").alias("partner_price"),
        F.col("taxability").alias("partner_taxability"),
        F.col("launch_date").alias("partner_launch"),
    )
    return products.join(complement_partners(products, seed), "product_id", "left").join(
        partner, "partner_id", "left"
    )


# Product taxability classes: TAXABLE pays the full combined rate everywhere;
# NON_TAXABLE (grocery staples) and REDUCED_RATE (clothing) pay the share of
# the rate their state's rule sets (``_shared/tax_rules.json``).
GROCERY_TAXABILITY = "NON_TAXABLE"
CLOTHING_TAXABILITY = "REDUCED_RATE"
# (grocery %, clothing %, clothing exempt under cents) of a state without a rule.
DEFAULT_TAX_RULE: tuple[int, int, int | None] = (0, 100, None)
TaxRules = dict[str, tuple[int, int, int | None]]


def tax_rules_by_state(entries: Iterable[TaxRuleEntry]) -> TaxRules:
    """``{state: (grocery %, clothing %, clothing exempt under cents)}`` of rule entries."""
    return {
        entry.StateCode: (
            entry.GroceryTaxPct,
            entry.ClothingTaxPct,
            None if entry.ClothingExemptUnder is None else int(entry.ClothingExemptUnder * 100),
        )
        for entry in entries
    }


def tax_percent(
    taxability: str, unit_cents: int, rule: tuple[int, int, int | None] = DEFAULT_TAX_RULE
) -> int:
    """Share (percent) of the combined rate a line pays under a state rule."""
    grocery, clothing, exempt_under = rule
    if taxability == GROCERY_TAXABILITY:
        return grocery
    if taxability == CLOTHING_TAXABILITY:
        return 0 if exempt_under is not None and unit_cents < exempt_under else clothing
    return 100 if taxability == "TAXABLE" else 0


def _rule_struct(rule: tuple[int, int, int | None]) -> Column:
    grocery, clothing, exempt_under = rule
    return F.struct(
        F.lit(grocery).cast("int").alias("grocery_pct"),
        F.lit(clothing).cast("int").alias("clothing_pct"),
        F.lit(exempt_under).cast("long").alias("clothing_exempt_under_cents"),
    )


def tax_rule_column(rules: TaxRules, state: Column) -> Column:
    """The state rule of a state-code column as a struct (the default when none)."""
    default = _rule_struct(DEFAULT_TAX_RULE)
    if not rules:
        return default
    return case_when(
        (state == F.lit(code), _rule_struct(rule)) for code, rule in sorted(rules.items())
    ).otherwise(default)


def line_tax_cents(
    net_cents: Column,
    taxability: Column,
    bps: Column,
    rule: Column | None = None,
    unit_cents: Column | None = None,
) -> Column:
    """Integer basis-point tax of a line's net cents (datagen ``_tax_cents``).

    ``rule`` is the selling (or destination) state's :func:`tax_rule_column`;
    without it groceries are exempt and clothing pays the full rate. Clothing
    priced (``unit_cents``) below the rule's threshold is exempt.
    """
    rule = _rule_struct(DEFAULT_TAX_RULE) if rule is None else rule
    exempt = rule["clothing_exempt_under_cents"]
    clothing = rule["clothing_pct"]
    if unit_cents is not None:
        clothing = F.when(exempt.isNotNull() & (unit_cents < exempt), F.lit(0)).otherwise(clothing)
    multiplier = (
        F.when(taxability == "TAXABLE", F.lit(100))
        .when(taxability == GROCERY_TAXABILITY, rule["grocery_pct"])
        .when(taxability == CLOTHING_TAXABILITY, clothing)
        .otherwise(F.lit(0))
    ).cast("long")
    return F.floor((net_cents * bps * multiplier + F.lit(500_000)) / F.lit(1_000_000)).cast("long")


def store_tax_rules(
    dim_stores: DataFrame, dim_geographies: DataFrame | None, rules: TaxRules
) -> DataFrame:
    """``store_id, tax_rule``: each store's state rule (the default without geographies)."""
    stores = dim_stores.select(F.col("ID").cast("long").alias("store_id"), "GeographyID")
    if dim_geographies is None:
        return stores.select("store_id", _rule_struct(DEFAULT_TAX_RULE).alias("tax_rule"))
    states = dim_geographies.select(F.col("ID").alias("GeographyID"), "State")
    return stores.join(states, "GeographyID", "left").select(
        "store_id", tax_rule_column(rules, F.col("State")).alias("tax_rule")
    )


def price_basket(
    draws: seeded_draws,
    key: Column,
    lines: Column,
    month: Column,
    offer: Column,
    bps: Column,
    profile: StoreTypeProfile,
    rule: Column | None = None,
) -> Column:
    """Lines with the basket promotion, ``ext_cents`` (net) and ``tax_cents``.

    ``rule`` is the store's state taxability rule (:func:`tax_rule_column`).
    """
    promoted = basket_promotion(draws, key, lines, month, offer, profile)

    def finish(line: Column) -> Column:
        gross = line["unit_cents"] * line["quantity"]
        net = F.greatest(F.lit(0).cast("long"), gross - line["discount_cents"])
        return line.withField("ext_cents", net).withField(
            "tax_cents", line_tax_cents(net, line["taxability"], bps, rule, line["unit_cents"])
        )

    return F.transform(promoted, finish)


def sum_lines(lines: Column, field: str) -> Column:
    """Sum of a long field over a basket's lines."""
    return F.aggregate(lines, F.lit(0).cast("long"), lambda acc, line: acc + line[field])
