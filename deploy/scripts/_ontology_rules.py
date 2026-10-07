"""Natural-language ontology business rules derived from enforcing code.

Each constant mirrors the source that enforces it; ``tests/deploy/
test_ontology_rules.py`` fails when a value drifts. Rules are guidance only:
the ontology does not execute them. Format reference:
https://learn.microsoft.com/fabric/iq/ontology/how-to-use-rules
"""

from __future__ import annotations

from dataclasses import dataclass

# fabric/lakehouse/14-ml-dynamic-pricing.ipynb
MIN_MARGIN_PCT = 0.15
MAX_PRICE_FACTOR = 1.0
MAX_CHANGE_PCT_WEEKLY = 0.10
MIN_DURATION_DAYS = 7
AGE_THRESHOLD_MODERATE = 30
AGE_THRESHOLD_AGGRESSIVE = 60
AGE_MARKDOWN_MODERATE = 0.10
AGE_MARKDOWN_AGGRESSIVE = 0.25
INVENTORY_HIGH_THRESHOLD = 100
# utility/src/retail_setup/generation/returns.py
RETURN_WINDOW_DAYS = 14
# fabric/lakehouse/12-ml-stockout-prediction.ipynb
STOCKOUT_THRESHOLD = 0
FORECAST_HORIZON_DAYS = 3
STOCKOUT_HIGH_PROBABILITY = 0.7
STOCKOUT_MEDIUM_PROBABILITY = 0.4
# utility/src/retail_setup/generation/inventory_policy.py
CASE_PACK = 12
REORDER_POINT_DAYS = 4
ORDER_UP_TO_DAYS = 8
NEAR_TERM_RETURN_DAYS = 4
HIGH_PRIORITY_DEMAND_DAYS = 2
# fabric/lakehouse/09-ml-churn-prediction.ipynb
CHURN_WINDOW_DAYS = 90


@dataclass(frozen=True)
class RuleReference:
    entity: str
    properties: tuple[str, ...] = ()


@dataclass(frozen=True)
class Rule:
    name: str
    statement: str
    entities: tuple[RuleReference, ...]
    relationships: tuple[str, ...] = ()


def _pct(value: float) -> str:
    return f"{value * 100:g}%"


RULES: tuple[Rule, ...] = (
    Rule(
        "PriceMinimumMargin",
        f"A recommended product price must be at least the product cost plus a "
        f"{_pct(MIN_MARGIN_PCT)} margin.",
        (RuleReference("Product", ("cost", "sale_price")),),
    ),
    Rule(
        "PriceCeilingMsrp",
        f"A recommended product price must not exceed {MAX_PRICE_FACTOR:g} times the "
        "product MSRP.",
        (RuleReference("Product", ("msrp", "sale_price")),),
    ),
    Rule(
        "PriceChangeCadence",
        f"A product price should not change by more than {_pct(MAX_CHANGE_PCT_WEEKLY)} "
        f"in a week, and price changes should be at least {MIN_DURATION_DAYS} days apart.",
        (RuleReference("Product", ("sale_price",)),),
    ),
    Rule(
        "AgedInventoryMarkdown",
        f"A product with no receipt for {AGE_THRESHOLD_MODERATE} days should receive a "
        f"{_pct(AGE_MARKDOWN_MODERATE)} markdown, and one with no receipt for "
        f"{AGE_THRESHOLD_AGGRESSIVE} days should receive a {_pct(AGE_MARKDOWN_AGGRESSIVE)} "
        f"markdown; {INVENTORY_HIGH_THRESHOLD} or more units on hand counts as overstocked.",
        (RuleReference("Product"), RuleReference("Store")),
    ),
    Rule(
        "PricingRequiresApproval",
        "A dynamic pricing recommendation is a proposal; it must be approved or "
        "rejected by a reviewer, and only approved recommendations should be treated "
        "as accepted price changes.",
        (RuleReference("Product"),),
    ),
    Rule(
        "ReturnWindow",
        f"A return must post between 1 and {RETURN_WINDOW_DAYS} days after the sale "
        "receipt it returns.",
        (RuleReference("Receipt", ("receipt_type", "event_ts")),),
    ),
    Rule(
        "StockoutDefinition",
        f"A store product is stocked out when its on-hand balance is at or below "
        f"{STOCKOUT_THRESHOLD:g} units; stockout risk forecasts the next "
        f"{FORECAST_HORIZON_DAYS} days and is HIGH at a probability of "
        f"{STOCKOUT_HIGH_PROBABILITY:g} or more, MEDIUM at "
        f"{STOCKOUT_MEDIUM_PROBABILITY:g} or more, and LOW otherwise.",
        (RuleReference("Store"), RuleReference("Product")),
        ("StoreHasStockoutRiskForProduct",),
    ),
    Rule(
        "ReplenishmentPolicy",
        f"A store should reorder a product when its inventory position (on hand, "
        f"inbound, and returns due within {NEAR_TERM_RETURN_DAYS} days) is at or below "
        f"the reorder point of {REORDER_POINT_DAYS} days of smoothed demand, never less "
        f"than one {CASE_PACK}-unit case; orders round up to whole {CASE_PACK}-unit "
        f"cases toward {ORDER_UP_TO_DAYS} days of demand and are URGENT when on hand is "
        f"zero and HIGH when on hand covers less than {HIGH_PRIORITY_DEMAND_DAYS} days.",
        (RuleReference("Store"), RuleReference("Product"),
         RuleReference("DistributionCenter")),
    ),
    Rule(
        "ChurnDefinition",
        f"A customer is churned when they make no purchase in the {CHURN_WINDOW_DAYS} "
        "days after a snapshot date; churn probabilities estimate that outcome and are "
        "not eligibility decisions.",
        (RuleReference("Customer"),
         RuleReference("ChurnPrediction", ("churn_probability", "risk_category",
                                           "churn_window_days"))),
        ("CustomerHasChurnPrediction",),
    ),
)


def _quote(name: str) -> str:
    return name if name.replace("_", "").isalnum() else "'" + name.replace("'", "''") + "'"


def render_rule(rule: Rule, lineage_tag: str) -> str:
    if not rule.statement or len(rule.statement) > 4000 or "\n" in rule.statement:
        raise ValueError(f"Rule {rule.name} needs a single-line statement under 4,000 characters.")
    if len(rule.entities) > 50 or len(rule.relationships) > 50:
        raise ValueError(f"Rule {rule.name} has too many references.")
    lines = [
        f"rule {rule.name}",
        f"\tstatement: {rule.statement}",
        f"\tlineageTag: {lineage_tag}",
        "",
    ]
    for reference in rule.entities:
        if len(reference.properties) > 10:
            raise ValueError(f"Rule {rule.name} references more than 10 properties.")
        lines.append(f"\truleReferencedEntity {_quote(reference.entity)}")
        if reference.properties:
            lines += ["\t\tpropertyScope: specific", ""]
            for prop in reference.properties:
                lines += [f"\t\truleReferencedProperty {_quote(prop)}", ""]
        else:
            lines.append("")
    for relationship in rule.relationships:
        lines += [f"\truleReferencedRelationship {_quote(relationship)}", ""]
    return "\n".join(lines)
