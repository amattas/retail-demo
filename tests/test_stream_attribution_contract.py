"""IMP-007 live-generator contract.

The live stream (``utility/src/retail_setup/generation/stream_events.py``,
inlined into ``stream-events``) mirrors the batch attribution semantics of
``attribution.py``: the same ~5% journey selection, two linked touches per
journey, a shared correlation ID, and promotion-before-tax money. Touches
carry their touch time in ``impression_time`` and arrive with the purchase.

All checks here are static (AST/source inspection) or pure-Python arithmetic;
the money and arrival-time behavior is exercised with Spark in
``utility/tests/generation/test_stream_realism.py``.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GENERATION = ROOT / "utility" / "src" / "retail_setup" / "generation"
STREAM_EVENTS = GENERATION / "stream_events.py"
ATTRIBUTION = GENERATION / "attribution.py"
STREAM_TEMPLATE = ROOT / "utility" / "notebooks" / "templates" / "driver-05-stream.py"
STREAM_NOTEBOOK = ROOT / "utility" / "notebooks" / "stream-events.ipynb"


def _source(path: Path = STREAM_EVENTS) -> str:
    return path.read_text(encoding="utf-8")


def _function_source(name: str, path: Path = STREAM_EVENTS) -> str:
    source = _source(path)
    node = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )
    segment = ast.get_source_segment(source, node)
    assert segment is not None
    return segment


def _constant(path: Path, name: str) -> int:
    source = _source(path)
    node = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == name for t in node.targets)
    )
    segment = ast.get_source_segment(source, node.value)
    assert segment is not None
    names = {"ATTRIBUTION_WINDOW_DAYS": 7}
    return eval(segment, {"__builtins__": {}}, names)  # noqa: S307 - numeric only


def _payloads() -> dict:
    tree = ast.parse(_source(STREAM_TEMPLATE))
    node = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "EVENT_PAYLOADS" for t in node.targets)
    )
    return ast.literal_eval(node.value)


def _notebook_source(path: Path) -> str:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    return "\n".join(
        "".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"
    )


def test_envelope_carries_optional_correlation_and_arrival_time() -> None:
    envelope = _function_source("envelope")
    assert "correlation: Column | None = None" in envelope
    assert "arrival: Column | None = None" in envelope
    assert '_str(correlation).alias("correlation_id")' in envelope
    assert '_str(session).alias("session_id")' in envelope
    assert '_str(parent).alias("parent_event_id")' in envelope
    # ingest_timestamp derives from the rate-source time: replays are identical.
    assert 'iso(ts if arrival is None else arrival).alias("ingest_timestamp")' in envelope


def test_journeys_reuse_the_batch_selection_and_plan() -> None:
    journey = _function_source("_journey")
    assert "with_journey_plan(" in journey
    assert '(F.col("status") == "APPROVED")' in journey
    assert 'F.col("customer_id").isNotNull()' in journey
    assert '"attr_select") < F.lit(SELECTION_RATE)' in journey
    assert "from retail_setup.generation.attribution import SELECTION_RATE, with_journey_plan" in (
        _source()
    )


def test_each_journey_has_two_touches_that_arrive_with_the_purchase() -> None:
    touches = _function_source("journey_touch_events")
    assert '("older", "touch_ts_older"), ("newer", "touch_ts_newer")' in touches
    assert '"ad_impression"' in touches
    assert 'iso(F.col(ts_col)).alias("impression_time")' in touches
    assert 'arrival=F.col("purchase_ts")' in touches
    assert 'correlation=F.col("journey_id")' in touches
    assert 'due & F.col("_attributed")' in touches
    older_min = _constant(ATTRIBUTION, "OLDER_OFFSET_MIN_S")
    older_max = _constant(ATTRIBUTION, "OLDER_OFFSET_MAX_S")
    newer_min = _constant(ATTRIBUTION, "NEWER_OFFSET_MIN_S")
    newer_max = _constant(ATTRIBUTION, "NEWER_OFFSET_MAX_S")
    assert 0 < newer_min <= newer_max < older_min <= older_max == 7 * 86400


def test_purchase_carries_the_newer_touch_and_journey_on_linked_events() -> None:
    purchases = _function_source("purchase_events")
    online = _function_source("online_events")
    assert 'newer_impression = F.concat(F.lit("IMPJRN"), F.col("_jhash"), F.lit("N"))' in purchases
    assert 'F.when(F.col("_attributed"), newer_impression).alias("impression_id")' in purchases
    # receipt, payment and each promotion of an attributed purchase
    assert purchases.count("correlation=journey") == 3
    # order created, payment, and the picked/shipped/delivered stages (one stage helper)
    assert online.count("correlation=journey") == 3
    assert 'stage("online_order_delivered", "delivered_ms", "delivered_time")' in online
    assert 'journey = F.col("journey_id")' in purchases


def test_receipt_and_order_payloads_carry_the_imp007_fields() -> None:
    purchases = _function_source("purchase_events")
    receipt = purchases[purchases.index('"receipt_created"') : purchases.index('"payment_processed"')]
    for expected in (
        '.alias("campaign_id")',
        '.alias("impression_id")',
        'F.col("gross_cents").alias("gross_subtotal_cents")',
        'F.col("discount_cents").alias("discount_cents")',
        'F.col("subtotal_cents").alias("subtotal_cents")',
        'F.col("tax_cents").alias("tax_cents")',
        'F.col("total_cents").alias("total_cents")',
        '.alias("subtotal")',
        '.alias("tax")',
        'total.alias("total")',
    ):
        assert expected in receipt, expected
    online = _function_source("online_events")
    order = online[online.index('"online_order_created"') : online.index('"payment_processed"')]
    # Online lines carry history's promo model: gross - discount = net subtotal.
    assert 'F.col("gross_cents").alias("gross_subtotal_cents")' in order
    assert 'F.col("discount_cents")' in order
    assert 'F.col("subtotal_cents")' in order
    assert '.withColumn("gross_cents", (subtotal + discount).cast("long"))' in online


def test_discount_is_applied_per_line_before_tax() -> None:
    pricing = _function_source("price_line")
    assert "line_promotion(" in pricing
    assert 'net = F.greatest(F.lit(0).cast("long"), line["gross_cents"] - discount)' in pricing
    assert 'net * _tax_share(line["taxability"])' in pricing
    purchases = _function_source("purchase_events")
    assert '.withColumn("subtotal_cents", gross - discount)' in purchases
    assert '.withColumn("total_cents", gross - discount + tax)' in purchases
    assert '(line["net_cents"] / 100.0).alias("extended_price")' in purchases
    assert 'F.when(line["discount_cents"] > 0, line["promo_code"]).alias("promo_code")' in purchases
    # one promotion_applied per (receipt, promo code), like fact_promotions
    assert 'F.transform(codes, promotion_event)' in purchases


def test_payment_amount_is_the_purchase_total() -> None:
    purchases = _function_source("purchase_events")
    payment = purchases[purchases.index('"payment_processed"') : purchases.index("def line_event")]
    assert 'total.alias("amount")' in payment
    assert 'F.col("total_cents").alias("amount_cents")' in payment
    assert 'F.col("status")' in payment
    assert "decline_reason_column(" in payment


def test_event_payloads_project_imp007_and_touch_time_fields() -> None:
    payloads = _payloads()
    fields = {event: {column for column, _f, _t in spec} for event, spec in payloads.items()}
    money = {
        "campaign_id",
        "impression_id",
        "gross_subtotal_cents",
        "discount_cents",
        "subtotal_cents",
        "tax_cents",
        "total_cents",
    }
    assert money <= fields["receipt_created"]
    assert money <= fields["online_order_created"]
    assert {"customer_id", "customer_ad_id", "impression_time"} <= fields["ad_impression"]
    assert ("impression_time", "impression_time", "datetime") in payloads["ad_impression"]


def test_promotion_net_subtotal_algebra_is_self_consistent() -> None:
    """gross - discount == net, and total == net + tax computed on net amounts."""

    def compute(lines, rate_bps):
        gross = sum(line_gross for line_gross, _pct, _taxable in lines)
        discounts = [round(line_gross * pct / 100.0) for line_gross, pct, _taxable in lines]
        nets = [line[0] - d for line, d in zip(lines, discounts)]
        tax = sum(
            round(net * (1.0 if taxable else 0.0) * rate_bps / 10000.0)
            for net, (_g, _p, taxable) in zip(nets, lines)
        )
        return gross, sum(discounts), sum(nets), tax, sum(nets) + tax

    gross, discount, subtotal, tax, total = compute([(1999, 20, True), (599, 0, True)], 800)
    assert discount == round(1999 * 0.20)
    assert subtotal == gross - discount
    assert total == subtotal + tax
    gross, discount, subtotal, _tax, _total = compute([(1999, 0, True), (599, 0, False)], 800)
    assert discount == 0 and subtotal == gross


def test_generated_stream_notebook_contains_the_live_contract() -> None:
    notebook = _notebook_source(STREAM_NOTEBOOK)
    for expected in (
        "def with_journey_plan(",
        "def journey_touch_events(",
        'iso(F.col(ts_col)).alias("impression_time")',
        "def generate_stream_batch(",
        "generate_stream_batch(STREAM_CONTEXT, rows, batch_id, STREAM_STATE)",
        "commit_stream_batch(STREAM_STATE, batch_id, batch)",
    ):
        assert expected in notebook, expected
