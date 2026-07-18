"""Contract tests for the IMP-007 marketing-attribution semantic-model slice.

Covers the two new Direct Lake tables (``fact_marketing_attribution`` and
``campaign_performance_daily``), their ``model.tmdl`` refs, their
relationships to ``dim_date``/``dim_customers``/``dim_stores``, and the
cents-authoritative DAX measures defined on each table.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SEMANTIC_DEFINITION = (
    REPO_ROOT / "fabric" / "powerbi" / "retail_model.SemanticModel" / "definition"
)
TABLES_DIR = SEMANTIC_DEFINITION / "tables"

# Authoritative Lakehouse schemas (utility/src/retail_setup/generation/schemas.py).
FACT_MARKETING_ATTRIBUTION_SOURCE_COLUMNS = [
    "attribution_id",
    "attribution_journey_id",
    "attribution_status",
    "attribution_model",
    "attribution_window_days",
    "impression_id_ext",
    "campaign_id",
    "creative_id",
    "channel",
    "customer_ad_id",
    "customer_id",
    "touch_ts",
    "purchase_ts",
    "lag_seconds",
    "purchase_type",
    "receipt_id_ext",
    "order_id_ext",
    "store_id",
    "gross_subtotal_cents",
    "discount_cents",
    "net_subtotal_cents",
    "tax_cents",
    "total_cents",
    "payment_cents",
    "attributed_revenue_cents",
    "event_date",
]

CAMPAIGN_PERFORMANCE_DAILY_SOURCE_COLUMNS = [
    "campaign_id",
    "channel",
    "day",
    "impressions",
    "spend_cents",
    "conversions",
    "attributed_revenue_cents",
    "discount_cents",
    "tax_cents",
    "payment_cents",
    "conversion_rate",
    "roas",
]


def _read_table(table_name: str) -> str:
    path = TABLES_DIR / f"{table_name}.tmdl"
    assert path.exists(), f"Missing semantic model table file: {path}"
    return path.read_text(encoding="utf-8")


def _model_refs() -> set[str]:
    model = (SEMANTIC_DEFINITION / "model.tmdl").read_text(encoding="utf-8")
    return {
        match.group(1).strip("'")
        for match in re.finditer(r"^ref table (.+)$", model, re.MULTILINE)
    }


def _relationships() -> str:
    return (SEMANTIC_DEFINITION / "relationships.tmdl").read_text(encoding="utf-8")


def _measure_expression(content: str, measure_name: str) -> str:
    match = re.search(
        rf"^\tmeasure '{re.escape(measure_name)}' = (.+)$", content, re.MULTILINE
    )
    assert match, f"Missing measure {measure_name!r}"
    return match.group(1)


def test_fact_marketing_attribution_table_and_columns_are_bound() -> None:
    content = _read_table("fact_marketing_attribution")

    assert re.search(r"^table fact_marketing_attribution$", content, re.MULTILINE)
    assert "sourceLineageTag: [ag].[fact_marketing_attribution]" in content

    for source_column in FACT_MARKETING_ATTRIBUTION_SOURCE_COLUMNS:
        assert re.search(
            rf"^\t\tsourceColumn: {re.escape(source_column)}$", content, re.MULTILINE
        ), f"fact_marketing_attribution is missing sourceColumn {source_column!r}"

    assert re.search(r"^\t\t\tschemaName: ag$", content, re.MULTILINE)
    assert re.search(
        r"^\t\t\tentityName: fact_marketing_attribution$", content, re.MULTILINE
    )
    assert re.search(r"^\t\tmode: directLake$", content, re.MULTILINE)


def test_campaign_performance_daily_table_and_columns_are_bound() -> None:
    content = _read_table("campaign_performance_daily")

    assert re.search(r"^table campaign_performance_daily$", content, re.MULTILINE)
    assert "sourceLineageTag: [au].[campaign_performance_daily]" in content

    for source_column in CAMPAIGN_PERFORMANCE_DAILY_SOURCE_COLUMNS:
        assert re.search(
            rf"^\t\tsourceColumn: {re.escape(source_column)}$", content, re.MULTILINE
        ), f"campaign_performance_daily is missing sourceColumn {source_column!r}"

    assert re.search(r"^\t\t\tschemaName: au$", content, re.MULTILINE)
    assert re.search(
        r"^\t\t\tentityName: campaign_performance_daily$", content, re.MULTILINE
    )
    assert re.search(r"^\t\tmode: directLake$", content, re.MULTILINE)

    # Stored daily rate/ratio columns are kept but must not silently sum.
    for stored_column in ("Conversion Rate (Stored)", "ROAS (Stored)"):
        column_block = re.search(
            rf"column '{re.escape(stored_column)}'\n(?P<body>(?:\t\t.+\n)+)",
            content,
        )
        assert column_block, f"Missing stored column {stored_column!r}"
        assert "summarizeBy: none" in column_block.group("body")


def test_new_tables_are_referenced_in_model() -> None:
    refs = _model_refs()
    assert "fact_marketing_attribution" in refs
    assert "campaign_performance_daily" in refs


def test_new_relationships_are_declared_correctly() -> None:
    relationships = _relationships()

    expected = {
        "Marketing Attribution to Customers": {
            "fromColumn": "fact_marketing_attribution.'Customer ID'",
            "toColumn": "dim_customers.ID",
            "inactive": False,
        },
        "Marketing Attribution to Stores": {
            "fromColumn": "fact_marketing_attribution.'Store ID'",
            "toColumn": "dim_stores.ID",
            "inactive": True,
        },
        "Marketing Attribution to Date": {
            "fromColumn": "fact_marketing_attribution.'Event Date'",
            "toColumn": "dim_date.Date",
            "inactive": False,
        },
        "Campaign Performance Daily to Date": {
            "fromColumn": "campaign_performance_daily.Day",
            "toColumn": "dim_date.Date",
            "inactive": False,
        },
    }

    for name, expectation in expected.items():
        block_match = re.search(
            rf"relationship '{re.escape(name)}'\n(?P<body>(?:\t.+\n)+)", relationships
        )
        assert block_match, f"Missing relationship {name!r}"
        body = block_match.group("body")
        assert f"\tfromColumn: {expectation['fromColumn']}\n" in body
        assert f"\ttoColumn: {expectation['toColumn']}\n" in body
        has_inactive = "\tisActive: false\n" in body
        assert has_inactive == expectation["inactive"], (
            f"{name} isActive mismatch: expected inactive={expectation['inactive']}"
        )

    # No fact-to-fact relationship was introduced for either new table.
    for block in re.split(r"(?=^relationship )", relationships, flags=re.MULTILINE):
        if (
            "fact_marketing_attribution" not in block
            and "campaign_performance_daily" not in block
        ):
            continue
        if not block.startswith("relationship "):
            continue
        from_match = re.search(r"^\tfromColumn: (.+)$", block, re.MULTILINE)
        to_match = re.search(r"^\ttoColumn: (.+)$", block, re.MULTILINE)
        assert from_match and to_match
        to_table = to_match.group(1).split(".", 1)[0].strip("'")
        assert to_table in {"dim_date", "dim_customers", "dim_stores"}, (
            f"Unexpected fact-to-fact relationship target {to_table!r} in: {block.splitlines()[0]}"
        )


def test_fact_marketing_attribution_measures_use_cents_authority() -> None:
    content = _read_table("fact_marketing_attribution")

    revenue = _measure_expression(content, "Attributed Revenue")
    assert "SUM('fact_marketing_attribution'[Attributed Revenue Cents])" in revenue
    assert "DIVIDE(" in revenue and ", 100, 0)" in revenue

    attributed_purchases = _measure_expression(content, "Attributed Purchases")
    assert "COUNTROWS('fact_marketing_attribution')" in attributed_purchases
    assert '"ATTRIBUTED"' in attributed_purchases

    unattributed_purchases = _measure_expression(content, "Unattributed Purchases")
    assert "COUNTROWS('fact_marketing_attribution')" in unattributed_purchases
    assert '<> "ATTRIBUTED"' in unattributed_purchases

    attribution_rate = _measure_expression(content, "Attribution Rate %")
    assert "[Attributed Purchases]" in attribution_rate
    assert "[Unattributed Purchases]" in attribution_rate

    discount = _measure_expression(content, "Attribution Discount Amount")
    assert "SUM('fact_marketing_attribution'[Discount Cents])" in discount

    tax = _measure_expression(content, "Attributed Tax")
    assert "SUM('fact_marketing_attribution'[Tax Cents])" in tax
    assert '"ATTRIBUTED"' in tax

    payments = _measure_expression(content, "Attributed Payments")
    assert "SUM('fact_marketing_attribution'[Payment Cents])" in payments
    assert '"ATTRIBUTED"' in payments

    variance = _measure_expression(content, "Payment Reconciliation Variance")
    assert "SUM('fact_marketing_attribution'[Payment Cents])" in variance
    assert "SUM('fact_marketing_attribution'[Total Cents])" in variance

    lag = _measure_expression(content, "Average Last-Touch Lag Hours")
    assert "AVERAGE('fact_marketing_attribution'[Lag Seconds])" in lag
    assert "3600" in lag


def test_campaign_performance_daily_measures_use_cents_authority() -> None:
    content = _read_table("campaign_performance_daily")

    spend = _measure_expression(content, "Campaign Spend")
    assert "SUM('campaign_performance_daily'[Spend Cents])" in spend

    impressions = _measure_expression(content, "Campaign Impressions")
    assert "SUM('campaign_performance_daily'[Impressions])" in impressions

    conversions = _measure_expression(content, "Campaign Conversions")
    assert "SUM('campaign_performance_daily'[Conversions])" in conversions

    attributed_revenue = _measure_expression(content, "Campaign Attributed Revenue")
    assert (
        "SUM('campaign_performance_daily'[Attributed Revenue Cents])"
        in attributed_revenue
    )

    conversion_rate = _measure_expression(content, "Campaign Conversion Rate")
    assert "[Campaign Conversions]" in conversion_rate
    assert "[Campaign Impressions]" in conversion_rate
    assert "Conversion Rate (Stored)" not in conversion_rate

    roas = _measure_expression(content, "Campaign ROAS")
    assert "[Campaign Attributed Revenue]" in roas
    assert "[Campaign Spend]" in roas
    assert "ROAS (Stored)" not in roas
