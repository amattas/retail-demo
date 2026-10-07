"""Semantic-model guards for filter propagation and Direct Lake DAX patterns."""

from __future__ import annotations

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
POWERBI_DIR = REPO_ROOT / "fabric" / "powerbi"
SEMANTIC_DEFINITION = (
    POWERBI_DIR / "retail_model.SemanticModel" / "definition"
)
TABLES_DIR = SEMANTIC_DEFINITION / "tables"


def _read_table(table_name: str) -> str:
    return (TABLES_DIR / f"{table_name}.tmdl").read_text(encoding="utf-8")


def _measure_block(table_name: str, measure_name: str) -> str:
    content = _read_table(table_name)
    match = re.search(
        rf"^\tmeasure '{re.escape(measure_name)}' = "
        r"(?P<body>.*?)(?=^\t(?:measure|column|partition) |\Z)",
        content,
        re.MULTILINE | re.DOTALL,
    )
    assert match, f"Missing measure {table_name}[{measure_name}]"
    return match.group("body")


def _relationship_block(relationship_name: str) -> str:
    content = (SEMANTIC_DEFINITION / "relationships.tmdl").read_text(
        encoding="utf-8"
    )
    match = re.search(
        rf"^relationship '{re.escape(relationship_name)}'\n"
        r"(?P<body>(?:\t.+\n)+)",
        content,
        re.MULTILINE,
    )
    assert match, f"Missing relationship {relationship_name!r}"
    return match.group("body")


def test_payment_relationship_roles_are_explicit() -> None:
    active_relationships = {
        "Payments to Date": (
            "fact_payments.'Event Date'",
            "dim_date.Date",
        ),
        "Payments to Stores": (
            "fact_payments.'Store ID'",
            "dim_stores.ID",
        ),
        "Payments to Customers": (
            "fact_payments.'Customer ID'",
            "dim_customers.ID",
        ),
    }
    inactive_relationships = {
        "Payments to Receipts": (
            "fact_payments.'Receipt ID'",
            "fact_receipts.'Receipt ID'",
        ),
        "Payments to Online Orders": (
            "fact_payments.'Order ID'",
            "fact_online_order_headers.'Order ID'",
        ),
    }

    for name, (from_column, to_column) in active_relationships.items():
        block = _relationship_block(name)
        assert "\tisActive: false\n" not in block
        assert f"\tfromColumn: {from_column}\n" in block
        assert f"\ttoColumn: {to_column}\n" in block

    for name, (from_column, to_column) in inactive_relationships.items():
        block = _relationship_block(name)
        assert "\tisActive: false\n" in block
        assert f"\tfromColumn: {from_column}\n" in block
        assert f"\ttoColumn: {to_column}\n" in block


def test_payment_metrics_combine_explicit_store_and_online_roles() -> None:
    total_amount = _measure_block("fact_payments", "Total Amounts")
    payment_count = _measure_block("fact_payments", "Payment Count")
    average_processing = _measure_block(
        "fact_payments", "Avg Payment Processing MS"
    )

    assert "[Store Payment Amount]" in total_amount
    assert "[Online Payment Amount]" in total_amount
    assert "[Store Payment Count]" in payment_count
    assert "[Online Payment Count]" in payment_count
    assert "[_Store Payment Processing Total MS]" in average_processing
    assert "[_Online Payment Processing Total MS]" in average_processing
    assert "[Payment Count]" in average_processing

    for measure_name in (
        "Store Payment Amount",
        "Store Payment Count",
        "_Store Payment Processing Total MS",
    ):
        block = _measure_block("fact_payments", measure_name)
        assert "'fact_payments'[Receipt ID] <> BLANK()" in block
        assert (
            "TREATAS(_ReceiptIds, 'fact_payments'[Receipt ID])" in block
        )
        assert "ISFILTERED('fact_online_order_headers'[Order ID])" in block

    for measure_name in (
        "Online Payment Amount",
        "Online Payment Count",
        "_Online Payment Processing Total MS",
    ):
        block = _measure_block("fact_payments", measure_name)
        assert "'fact_payments'[Order ID] <> BLANK()" in block
        assert "TREATAS(_OrderIds, 'fact_payments'[Order ID])" in block
        assert "ISFILTERED('fact_receipts'[Receipt ID])" in block
        assert (
            "CROSSFILTER("
            "'fact_payments'[Store ID], 'dim_stores'[ID], NONE"
            ")" in block
        )


def test_store_operations_visual_uses_store_payment_role() -> None:
    visual_path = (
        POWERBI_DIR
        / "retail_model.Report"
        / "definition"
        / "pages"
        / "opsmarketing0000001"
        / "visuals"
        / "ops_table"
        / "visual.json"
    )
    visual = json.loads(visual_path.read_text(encoding="utf-8"))
    serialized = json.dumps(visual)

    assert '"Property": "Store Payment Decline Rate %"' in serialized
    assert "fact_payments.Store Payment Decline Rate %" in serialized


def test_product_filters_reach_receipt_and_order_denominators() -> None:
    receipt_count = _measure_block("fact_receipts", "Receipt Count")
    order_count = _measure_block("fact_online_order_headers", "Online Order Count")
    average_basket = _measure_block("fact_receipts", "Average Basket Size")
    average_online_order = _measure_block(
        "fact_online_order_headers", "Average Online Order Value"
    )

    assert "DISTINCTCOUNT('fact_receipt_lines'[Receipt ID])" in receipt_count
    assert "COUNTROWS('fact_receipts')" not in receipt_count
    assert "DISTINCTCOUNT('fact_online_order_lines'[Order ID])" in order_count
    assert "COUNTROWS('fact_online_order_headers')" not in order_count
    assert "[Net Sales]" in average_basket
    assert "[Receipt Count]" in average_basket
    assert "[Online Line Revenue]" in average_online_order
    assert "[Online Order Count]" in average_online_order


def test_sales_per_customer_counts_filtered_purchasers() -> None:
    purchasing_customers = _measure_block(
        "fact_receipts", "Purchasing Customers"
    )
    sales_per_customer = _measure_block(
        "fact_receipts", "Sales per Customer"
    )

    assert "VALUES('fact_receipt_lines'[Receipt ID])" in purchasing_customers
    assert (
        "DISTINCTCOUNT('fact_receipts'[Customer ID])" in purchasing_customers
    )
    assert (
        "TREATAS(_ReceiptIds, 'fact_receipts'[Receipt ID])"
        in purchasing_customers
    )
    assert "[Purchasing Customers]" in sales_per_customer
    assert "[# of Customers]" not in sales_per_customer


def test_product_cost_measures_use_grouped_relationship_aggregation() -> None:
    expected = {
        ("fact_receipt_lines", "Line Cost"): (
            "CALCULATE(SUM('fact_receipt_lines'[Quantity]))",
            "CALCULATE(MAX('dim_products'[Cost]))",
        ),
        ("inventory_position_current", "Total Inventory Value"): (
            "CALCULATE(SUM('inventory_position_current'[On Hand]))",
            "CALCULATE(MAX('dim_products'[Cost]))",
        ),
        ("inventory_position_current", "Total Inventory Retail Value"): (
            "CALCULATE(SUM('inventory_position_current'[On Hand]))",
            "CALCULATE(MAX('dim_products'[Sale Price]))",
        ),
    }

    for (table_name, measure_name), fragments in expected.items():
        block = _measure_block(table_name, measure_name)
        assert "LOOKUPVALUE(" not in block
        assert "VALUES('dim_products'[ID])" in block
        for fragment in fragments:
            assert fragment in block
