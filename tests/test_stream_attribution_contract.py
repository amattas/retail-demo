"""IMP-007 live-generator contract: mirrors the batch attribution semantics in
``utility/notebooks/templates/driver-05-stream.py`` (deterministic 2-touch
journeys, shared correlation_id, promo-before-tax money) — see
``utility/src/retail_setup/generation/attribution.py`` for the batch contract
this file must stay consistent with.

All checks here are static (AST/source inspection) or pure-Python arithmetic;
none require a running Spark session or a Kusto database.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
STREAM_TEMPLATE = ROOT / "utility" / "notebooks" / "templates" / "driver-05-stream.py"
STREAM_NOTEBOOK = ROOT / "utility" / "notebooks" / "stream-events.ipynb"


def _source() -> str:
    return STREAM_TEMPLATE.read_text(encoding="utf-8")


def _tree() -> ast.Module:
    return ast.parse(_source())


def _notebook_source(path: Path) -> str:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    return "\n".join(
        "".join(cell["source"])
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    )


def _function(tree: ast.Module, name: str) -> ast.FunctionDef:
    return next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )


def _assignment_node(tree: ast.Module, name: str) -> ast.Assign:
    return next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        )
    )


def _assignment_value(tree: ast.Module, source: str, name: str) -> Any:
    """Evaluate a module-level constant's RHS (arithmetic of int literals)."""
    node = _assignment_node(tree, name)
    segment = ast.get_source_segment(source, node.value)
    assert segment is not None
    return eval(segment, {"__builtins__": {}})  # noqa: S307 - trusted, numeric-only


# ---------------------------------------------------------------------------
# slot(): optional correlation argument (attribution_journey_id carrier)
# ---------------------------------------------------------------------------


def test_slot_gains_optional_correlation_argument_defaulting_to_null() -> None:
    tree = _tree()
    slot_fn = _function(tree, "slot")
    args = [a.arg for a in slot_fn.args.args]
    defaults_names = args[-len(slot_fn.args.defaults) :]
    assert "correlation" in args
    assert "correlation" in defaults_names

    slot_source = ast.get_source_segment(_source(), slot_fn)
    assert slot_source is not None
    # correlation_id is now carried through _str(), not hardcoded to NULL.
    assert '_str(correlation).alias("correlation_id")' in slot_source
    assert 'F.lit(None).cast("string").alias("correlation_id")' not in slot_source
    # session/parent stay optional and unaffected.
    assert '_str(session).alias("session_id")' in slot_source
    assert '_str(parent).alias("parent_event_id")' in slot_source


# ---------------------------------------------------------------------------
# Deterministic attributed STORE/ONLINE scenarios, additive to background
# ---------------------------------------------------------------------------


def test_scenario_adds_attributed_buckets_without_removing_background_ones() -> None:
    source = _source()
    tree = _tree()
    scenario_node = _assignment_node(tree, "scenario")
    scenario_source = ast.get_source_segment(source, scenario_node.value)
    assert scenario_source is not None

    for expected in (
        '"shopping"',
        '"store_attributed"',
        '"inventory"',
        '"online"',
        '"online_attributed"',
        '"logistics"',
        '"marketing"',
        '"store_ops"',
    ):
        assert expected in scenario_source

    # Still exactly 8 scenario branches (7 `.when` + 1 `.otherwise`): the two
    # attributed scenarios are additive, none of the original 6 were dropped.
    assert scenario_source.count(".when(") == 7
    assert scenario_source.count(".otherwise(") == 1

    assert 'shop = F.col("scenario").isin("shopping", "store_attributed")' in source
    assert 'onl = F.col("scenario").isin("online", "online_attributed")' in source
    assert 'store_attr = F.col("scenario") == "store_attributed"' in source
    assert 'online_attr = F.col("scenario") == "online_attributed"' in source
    assert "attr = store_attr | online_attr" in source


# ---------------------------------------------------------------------------
# Exactly one journey + exactly two ad_impression touches per attributed row
# ---------------------------------------------------------------------------


def test_exactly_two_attributed_touches_are_emitted_per_journey() -> None:
    source = _source()
    assert source.count('_attr_touch("older"') == 1
    assert source.count('_attr_touch("newer"') == 1
    # both wired into the same events_arr, gated on the shared `attr` flag.
    touch_fn_source = ast.get_source_segment(source, _function(_tree(), "_attr_touch"))
    assert touch_fn_source is not None
    assert 'slot(attr, "ad_impression"' in touch_fn_source
    # no 19th event type: touches use the pre-existing "ad_impression" type.
    payloads = _assignment_value_dict(source)
    assert set(payloads) == {
        "receipt_created",
        "receipt_line_added",
        "payment_processed",
        "inventory_updated",
        "stockout_detected",
        "reorder_triggered",
        "customer_entered",
        "customer_zone_changed",
        "ble_ping_detected",
        "truck_arrived",
        "truck_departed",
        "store_opened",
        "store_closed",
        "ad_impression",
        "promotion_applied",
        "online_order_created",
        "online_order_picked",
        "online_order_shipped",
    }
    assert len(payloads) == 18


def _assignment_value_dict(source: str) -> dict:
    tree = ast.parse(source)
    node = _assignment_node(tree, "EVENT_PAYLOADS")
    return ast.literal_eval(node.value)


def test_two_touches_share_journey_customer_campaign_session_and_differ() -> None:
    source = _source()
    tree = _tree()
    touch_fn = _function(tree, "_attr_touch")
    touch_source = ast.get_source_segment(source, touch_fn)
    assert touch_source is not None

    # shared: same journey (correlation), campaign, customer_ad_id/customer_id, session
    assert "correlation=journey_id" in touch_source
    assert 'attr_campaign_id.alias("campaign_id")' in touch_source
    assert 'attr_customer_ad_id.alias("customer_ad_id")' in touch_source
    assert 'F.col("customer_id")' in touch_source
    assert "session=attr_session" in touch_source

    # differ: impression_id/creative_id/timestamp are all parameterized by
    # the call site (touch_ts, impression_id) and the `which` suffix.
    assert 'impression_id.alias("impression_id")' in touch_source
    assert 'F.lit(f"-{which.upper()}")' in touch_source
    assert "def _attr_touch(which, touch_ts, impression_id):" in touch_source

    older_call = re.search(r'_attr_touch\("older", (\w+), (\w+)\)', source)
    newer_call = re.search(r'_attr_touch\("newer", (\w+), (\w+)\)', source)
    assert older_call is not None and newer_call is not None
    assert older_call.groups() == ("attr_touch_ts_older", "attr_impression_id_older")
    assert newer_call.groups() == ("attr_touch_ts_newer", "attr_impression_id_newer")
    # distinct expressions -> distinct values at runtime
    assert older_call.group(1) != newer_call.group(1)
    assert older_call.group(2) != newer_call.group(2)


def test_touch_offsets_stay_within_the_7_day_window_and_never_overlap() -> None:
    source = _source()
    tree = _tree()
    older_min = _assignment_value(tree, source, "ATTR_OLDER_OFFSET_MIN_S")
    older_max = _assignment_value(tree, source, "ATTR_OLDER_OFFSET_MAX_S")
    newer_min = _assignment_value(tree, source, "ATTR_NEWER_OFFSET_MIN_S")
    newer_max = _assignment_value(tree, source, "ATTR_NEWER_OFFSET_MAX_S")

    window_s = 7 * 86400
    assert 0 < newer_min <= newer_max < older_min <= older_max == window_s
    assert older_max <= window_s
    assert newer_max <= window_s

    # both touches are subtracted from `ts` (purchase_ts) -> always in the past
    assert (
        'attr_touch_ts_older = F.timestamp_seconds(F.unix_timestamp(F.col("ts")) - attr_older_offset)'
        in source
    )
    assert (
        'attr_touch_ts_newer = F.timestamp_seconds(F.unix_timestamp(F.col("ts")) - attr_newer_offset)'
        in source
    )
    # the purchase itself uses `ts` (the rate source's current timestamp) —
    # never a future timestamp.
    assert 'slot(shop, "receipt_created"' in source
    assert 'slot(onl, "online_order_created"' in source


def test_newer_touch_impression_is_the_one_carried_on_the_purchase() -> None:
    source = _source()
    assert "attr_purchase_impression_id = attr_impression_id_newer" in source
    assert (
        "attr_receipt_impression_id = F.when(\n    store_attr, attr_purchase_impression_id)"
        in source
    )
    assert (
        "attr_order_impression_id = F.when(\n    online_attr, attr_purchase_impression_id)"
        in source
    )


# ---------------------------------------------------------------------------
# Correlation id (attribution_journey_id) only on linked events
# ---------------------------------------------------------------------------


def test_correlation_id_is_null_unless_the_row_is_attributed() -> None:
    source = _source()
    assert (
        'attr_correlation_store = F.when(store_attr, journey_id).otherwise(F.lit(None).cast("string"))'
        in source
    )
    assert (
        'attr_correlation_online = F.when(online_attr, journey_id).otherwise(F.lit(None).cast("string"))'
        in source
    )

    # every linked event (receipt/payment/promotion for store, order for
    # online) passes one of these through slot()'s correlation kwarg.
    assert (
        source.count("correlation=attr_correlation_store") == 3
    )  # receipt, payment, promotion
    assert source.count("correlation=attr_correlation_online") == 4
    # online_order_created, payment_processed, picked, shipped
    assert source.count("correlation=journey_id") == 1  # shared _attr_touch body


# ---------------------------------------------------------------------------
# receipt_created / online_order_created payload field contract
# ---------------------------------------------------------------------------


def test_receipt_created_carries_the_new_imp007_fields() -> None:
    source = _source()
    receipt_block = source[
        source.index('slot(shop, "receipt_created"') : source.index(
            "_line(1), _line(2)"
        )
    ]
    for expected in (
        'attr_receipt_campaign_id.alias("campaign_id")',
        'attr_receipt_impression_id.alias("impression_id")',
        'gross_subtotal_cents.alias("gross_subtotal_cents")',
        'store_discount_cents.alias("discount_cents")',
        'store_subtotal_cents.alias("subtotal_cents")',
        'store_tax_cents.alias("tax_cents")',
        'store_total_cents.alias("total_cents")',
        # existing real-valued fields kept for compatibility
        'store_subtotal.alias("subtotal")',
        'store_tax.alias("tax")',
        'store_total.alias("total")',
    ):
        assert expected in receipt_block, expected


def test_online_order_created_carries_the_new_imp007_fields() -> None:
    source = _source()
    order_block = source[
        source.index('slot(onl, "online_order_created"') : source.index(
            'slot(onl, "online_order_picked"'
        )
    ]
    for expected in (
        'attr_order_campaign_id.alias("campaign_id")',
        'attr_order_impression_id.alias("impression_id")',
        'online_gross_subtotal_cents.alias("gross_subtotal_cents")',
        'online_discount_cents.alias("discount_cents")',
        'online_subtotal_cents.alias("subtotal_cents")',
        'online_tax_cents.alias("tax_cents")',
        'online_total_cents.alias("total_cents")',
        # existing real-valued fields kept for compatibility, unchanged names
        'F.col("subtotal")',
        'F.col("tax")',
        'F.col("total")',
    ):
        assert expected in order_block, expected


def test_online_orders_never_discount_gross_equals_net() -> None:
    source = _source()
    assert 'online_discount_cents = F.lit(0).cast("long")' in source
    assert "online_subtotal_cents = online_gross_subtotal_cents" in source


def test_event_payloads_project_new_fields_for_kql() -> None:
    payloads = _assignment_value_dict(_source())
    receipt_fields = {col for col, _field, _dtype in payloads["receipt_created"]}
    order_fields = {col for col, _field, _dtype in payloads["online_order_created"]}
    ad_fields = {col for col, _field, _dtype in payloads["ad_impression"]}

    new_money_fields = {
        "campaign_id",
        "impression_id",
        "gross_subtotal_cents",
        "discount_cents",
        "subtotal_cents",
        "tax_cents",
        "total_cents",
    }
    assert new_money_fields <= receipt_fields
    assert new_money_fields <= order_fields
    assert "customer_id" in ad_fields
    assert "customer_ad_id" in ad_fields  # pre-existing field kept


# ---------------------------------------------------------------------------
# Promotion reconciliation: discount before tax
# ---------------------------------------------------------------------------


def test_store_discount_is_applied_before_tax_in_expression_order() -> None:
    source = _source()
    idx_discount = source.index("store_discount_cents = F.when(")
    idx_l1_net = source.index("l1_net_cents = l1_ext_cents - store_discount_cents")
    idx_subtotal = source.index(
        "store_subtotal_cents = gross_subtotal_cents - store_discount_cents"
    )
    idx_tax = source.index("store_tax_cents = F.round(")
    idx_total = source.index(
        "store_total_cents = store_subtotal_cents + store_tax_cents"
    )

    # discount computed first, then applied to line 1 net, then subtotal,
    # THEN tax (computed from the already-net amounts), then total.
    assert idx_discount < idx_l1_net < idx_subtotal < idx_tax < idx_total

    tax_block = source[idx_tax:idx_total]
    assert 'l1_net_cents.cast("double")' in tax_block
    assert 'l1_ext_cents.cast("double")' not in tax_block  # tax uses NET, not gross


def test_promoted_line_ext_price_is_net_and_carries_promo_code() -> None:
    line_fn_source = ast.get_source_segment(_source(), _function(_tree(), "_line"))
    assert line_fn_source is not None
    assert (
        'ext_price = l1_net_ext if idx == 1 else F.col(f"l{idx}_ext")' in line_fn_source
    )
    assert (
        'line_promo_code = F.when(has_promo, F.col("promo_code")) if idx == 1 else F.lit(None).cast("string")'
        in line_fn_source
    )


def test_non_promoted_rows_have_zero_discount() -> None:
    source = _source()
    assert "store_discount_cents = F.when(\n    has_promo," in source
    assert ').otherwise(F.lit(0).cast("long"))' in source


def test_promotion_applied_discount_cents_equals_receipt_discount_cents() -> None:
    source = _source()
    promo_block = source[
        source.index('slot(shop & has_promo, "promotion_applied"') : source.index(
            "# --- inventory ---"
        )
    ]
    # both the receipt payload and the promotion payload alias the SAME
    # underlying column, so they are guaranteed numerically identical.
    assert 'store_discount_cents.alias("discount_cents")' in promo_block
    receipt_block = source[
        source.index('slot(shop, "receipt_created"') : source.index(
            "_line(1), _line(2)"
        )
    ]
    assert 'store_discount_cents.alias("discount_cents")' in receipt_block


def test_promotion_net_subtotal_algebra_is_self_consistent() -> None:
    """Pure-python replay of the cents algebra (no Spark needed): for any
    gross line amounts and a promo pct on line 1, gross - discount == net,
    and total == net + tax computed from net amounts.
    """

    def compute(
        l1_gross_cents,
        l2_gross_cents,
        pct,
        has_promo,
        rate_bps,
        taxable1=True,
        taxable2=True,
    ):
        gross_subtotal = l1_gross_cents + l2_gross_cents
        discount = round(l1_gross_cents * pct / 100.0) if has_promo else 0
        l1_net = l1_gross_cents - discount
        subtotal = gross_subtotal - discount
        assert subtotal == l1_net + l2_gross_cents
        tax = round(
            (
                l1_net * (1.0 if taxable1 else 0.0)
                + l2_gross_cents * (1.0 if taxable2 else 0.0)
            )
            * rate_bps
            / 10000.0
        )
        total = subtotal + tax
        return gross_subtotal, discount, subtotal, tax, total

    # promoted case: discount > 0, net < gross, total reflects the discount
    gross, discount, subtotal, tax, total = compute(1999, 599, 20, True, 800)
    assert discount == round(1999 * 0.20)
    assert subtotal == gross - discount
    assert total == subtotal + tax
    assert subtotal < gross

    # non-promoted case: discount == 0, net == gross
    gross, discount, subtotal, tax, total = compute(1999, 599, 20, False, 800)
    assert discount == 0
    assert subtotal == gross


# ---------------------------------------------------------------------------
# payment_processed: amount == total for approved payments, shares correlation
# ---------------------------------------------------------------------------


def test_payment_amount_equals_store_total_and_shares_correlation() -> None:
    source = _source()
    payment_block = source[
        source.index('slot(shop, "payment_processed"') : source.index(
            'slot(shop & has_promo, "promotion_applied"'
        )
    ]
    assert 'store_total.alias("amount")' in payment_block
    assert 'store_total_cents.alias("amount_cents")' in payment_block
    assert 'F.lit("APPROVED").alias("status")' in payment_block
    assert "correlation=attr_correlation_store" in payment_block


def test_online_payment_equals_order_total_and_shares_correlation() -> None:
    source = _source()
    order_start = source.index('slot(onl, "online_order_created"')
    payment_start = source.index('slot(onl, "payment_processed"', order_start)
    picked_start = source.index('slot(onl, "online_order_picked"', payment_start)
    payment_block = source[payment_start:picked_start]

    assert 'F.col("order_id")' in payment_block
    assert 'F.col("total").alias("amount")' in payment_block
    assert 'online_total_cents.alias("amount_cents")' in payment_block
    assert 'F.lit("APPROVED").alias("status")' in payment_block
    assert "correlation=attr_correlation_online" in payment_block


# ---------------------------------------------------------------------------
# Generated notebook stays in sync with the template
# ---------------------------------------------------------------------------


def test_generated_stream_notebook_contains_the_imp007_contract() -> None:
    notebook_source = _notebook_source(STREAM_NOTEBOOK)
    for expected in (
        '"store_attributed"',
        '"online_attributed"',
        "def _attr_touch(which, touch_ts, impression_id):",
        "attr_correlation_store = F.when(store_attr, journey_id)",
        "store_subtotal_cents = gross_subtotal_cents - store_discount_cents",
        'gross_subtotal_cents.alias("gross_subtotal_cents")',
    ):
        assert expected in notebook_source, expected
