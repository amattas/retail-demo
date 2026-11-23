"""Setup-11 (gold.py) and notebook 04 publish identical Gold tables.

Notebook 04 embeds ``gold_rules`` (``build_notebooks.py --check`` keeps the
copy exact); this test runs 04's own Gold cells on a small Silver fixture and
compares every table, row for row, with ``generate_gold``.
"""

import ast
import json
from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from retail_setup.generation.gold import GOLD_TABLES, generate_gold
from retail_setup.generation.gold_rules import gold_cutoff

pytestmark = pytest.mark.spark_no_filesystem

NOTEBOOK = (
    Path(__file__).resolve().parents[3] / "fabric" / "lakehouse" / "04-streaming-to-gold.ipynb"
)
T = datetime(2025, 10, 6, 12, 0)
LATE = datetime(2025, 10, 10, 8, 0)  # after the history cutoff


def _code_cells() -> list[str]:
    notebook = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    return ["".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"]


def _silver(spark) -> dict:
    def frame(rows, schema):
        return spark.createDataFrame(rows, schema)

    return {
        "fact_receipts": frame(
            [
                ("R1", "SALE", 1, T, "10.00", "CARD"),
                ("R2", "RETURN", 1, T, "-4.00", "CARD"),
                ("R3", "SALE", 2, T, "7.00", "CASH"),
                ("R4", "SALE", 1, LATE, "5.00", "CARD"),
            ],
            "receipt_id_ext string, receipt_type string, store_id long, event_ts timestamp, "
            "total_amount string, payment_method string",
        ),
        "fact_receipt_lines": frame(
            [
                ("R1", 11, T, "6.00", 2),
                ("R1", 12, T, "4.00", 1),
                ("R2", 11, T, "-4.00", -1),
                ("R3", 12, T, "7.00", 1),
                ("R4", 11, LATE, "5.00", 1),
            ],
            "receipt_id_ext string, product_id long, event_ts timestamp, ext_price string, "
            "quantity long",
        ),
        "fact_payments": frame(
            [
                ("R1", None, "APPROVED"),
                ("R2", None, "APPROVED"),
                ("R3", None, "DECLINED"),
                ("R4", None, "APPROVED"),
                (None, "O1", "APPROVED"),
                (None, "O2", "DECLINED"),
            ],
            "receipt_id_ext string, order_id_ext string, status string",
        ),
        "fact_online_order_headers": frame(
            [("O1", T, "9.00", "1.00", "10.00"), ("O2", T, "4.00", "0.50", "4.50")],
            "order_id_ext string, event_ts timestamp, subtotal_amount string, "
            "tax_amount string, total_amount string",
        ),
        "fact_marketing": frame(
            [("C1", "EMAIL", T, "I1", 10, "0.10"), ("C1", "EMAIL", LATE, "I2", 20, "0.20")],
            "campaign_id string, channel string, event_ts timestamp, impression_id_ext string, "
            "cost_cents long, cost string",
        ),
        "fact_marketing_attribution": frame(
            [("A1", "ATTRIBUTED", "C1", "EMAIL", T, T, 900, 0, 100, 1000)],
            "attribution_id string, attribution_status string, campaign_id string, "
            "channel string, touch_ts timestamp, purchase_ts timestamp, "
            "attributed_revenue_cents long, discount_cents long, tax_cents long, "
            "payment_cents long",
        ),
        "fact_foot_traffic": frame(
            [(1, "ENTRANCE", T, 30.0, 2), (1, "ENTRANCE", LATE, 60.0, 1)],
            "store_id long, zone string, event_ts timestamp, dwell_seconds double, count long",
        ),
        "fact_truck_moves": frame(
            [
                (7, 1, 5, "S1", "ARRIVED", T, datetime(2025, 10, 6, 12, 45), None),
                (
                    7,
                    1,
                    5,
                    "S1",
                    "COMPLETED",
                    T,
                    datetime(2025, 10, 6, 12, 45),
                    datetime(2025, 10, 6, 12, 45),
                ),
                (
                    8,
                    1,
                    5,
                    "S2",
                    "COMPLETED",
                    LATE,
                    datetime(2025, 10, 10, 9),
                    datetime(2025, 10, 10, 9),
                ),
            ],
            "truck_id long, dc_id long, store_id long, shipment_id string, status string, "
            "eta timestamp, etd timestamp, departure_time timestamp",
        ),
        "fact_store_inventory_txn": frame(
            [(1, 11, 5, T, "a"), (1, 11, 3, LATE, "b")],
            "store_id long, product_id long, balance long, event_ts timestamp, trace_id string",
        ),
        "fact_dc_inventory_txn": frame(
            [(1, 11, 50, T, "a"), (1, 11, 40, LATE, "b")],
            "dc_id long, product_id long, balance long, event_ts timestamp, trace_id string",
        ),
    }


def _notebook_gold(tables: dict, cutoff: datetime) -> dict:
    cells = _code_cells()
    shared = next(cell for cell in cells if cell.startswith("# SHARED SOURCE"))
    saved: dict = {}
    namespace = {
        "F": F,
        "GOLD_CUTOFF": cutoff,
        "silver_exists": tables.__contains__,
        "read_silver": tables.__getitem__,
        "save_gold": lambda frame, name: saved.__setitem__(name, frame),
    }
    exec(compile(shared, str(NOTEBOOK), "exec"), namespace)
    for cell in cells:
        if "save_gold(df," in cell and "def save_gold" not in cell:
            exec(compile(cell, str(NOTEBOOK), "exec"), namespace)
    return saved


def _rows(frame) -> list[tuple]:
    return sorted((tuple(row) for row in frame.collect()), key=repr)


def test_notebook_04_lists_the_shared_gold_tables():
    tree = ast.parse("\n".join(_code_cells()))
    expected = next(
        node.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(getattr(target, "id", None) == "EXPECTED_GOLD_TABLES" for target in node.targets)
    )
    assert ast.literal_eval(expected) == GOLD_TABLES


def test_setup_11_and_notebook_04_build_identical_gold(spark):
    tables = _silver(spark)
    cutoff = gold_cutoff(date(2025, 10, 8), False, datetime(2026, 1, 1))
    expected = generate_gold(spark, tables, cutoff)
    actual = _notebook_gold(tables, cutoff)

    assert set(actual) == set(GOLD_TABLES)
    for name in GOLD_TABLES:
        assert actual[name].columns == expected[name].columns, name
        assert actual[name].schema == expected[name].schema, name
        assert _rows(actual[name]) == _rows(expected[name]), name

    sales = expected["sales_minute_store"].collect()
    assert [(row.store_id, row.receipts, row.total_sales) for row in sales] == [(1, 1, 10.0)]
    assert [(row.trucks, row.avg_dwell_min) for row in expected["truck_dwell_daily"].collect()] == [
        (1, 45.0)
    ]
    assert expected["online_sales_daily"].first().orders == 1
    assert {row.on_hand for row in expected["inventory_position_current"].collect()} == {5}
    top = {row.product_id: row.units for row in expected["top_products_15m"].collect()}
    assert top == {11: 2, 12: 1}
