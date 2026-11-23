"""Semantic-model measure contracts: sale-only counts, approved payments,
like-for-like comparisons, stated bases, forecast intervals, date roles and the
twin observation cutoff. Static TMDL checks plus small Python oracles of the
intended arithmetic; no DAX engine is executed."""

from __future__ import annotations

import json
import math
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / "fabric" / "powerbi" / "retail_model.SemanticModel" / "definition"
TABLES = MODEL / "tables"
REPORTS = sorted((ROOT / "fabric" / "powerbi").glob("*.Report"))
SALE_ONLY = "KEEPFILTERS('fact_receipts'[Receipt Type] = \"SALE\")"


def _table(name: str) -> str:
    return (TABLES / f"{name}.tmdl").read_text(encoding="utf-8").replace("\r\n", "\n")


def _measure(table: str, name: str) -> tuple[str, str]:
    """``(description, body)`` of a measure."""
    match = re.search(
        rf"(?:^\t/// (?P<desc>[^\n]*)\n)?^\tmeasure '{re.escape(name)}' = "
        r"(?P<body>.*?)(?=^\t\t(?:formatString|displayFolder|lineageTag|isHidden)\b)",
        _table(table),
        re.MULTILINE | re.DOTALL,
    )
    assert match, f"missing measure {table}[{name}]"
    return match.group("desc") or "", match.group("body")


def _model_measures() -> dict[str, set[str]]:
    return {
        path.stem: set(
            re.findall(r"^\tmeasure '?([^'=\n]+?)'? = ", path.read_text(encoding="utf-8"), re.M)
        )
        for path in TABLES.glob("*.tmdl")
    }


def test_receipt_counts_and_return_rate_cover_sale_receipts_only() -> None:
    # L-7: returns are refunds, not purchases.
    _, count = _measure("fact_receipts", "Receipt Count")
    assert "DISTINCTCOUNT('fact_receipt_lines'[Receipt ID])" in count and SALE_ONLY in count
    _, gross = _measure("fact_receipts", "Gross Sales")
    assert "[Line Revenue]" in gross and SALE_ONLY in gross
    _, basket = _measure("fact_receipts", "Average Basket Size")
    assert "DIVIDE([Gross Sales], [Receipt Count], 0)" in basket
    _, units = _measure("fact_receipt_lines", "Units per Receipt")
    assert f"CALCULATE([Total Units Sold], {SALE_ONLY})" in units and "[Receipt Count]" in units
    _, rate = _measure("fact_receipts", "Return Rate %")
    assert f"CALCULATE([Total Sales], {SALE_ONLY})" in rate
    assert "DIVIDE(ABS([Total Returns]), [Total Sales]" not in rate


def test_payment_amounts_are_approved_and_counts_are_attempts() -> None:
    # L-8: a declined attempt collects nothing.
    approved = "KEEPFILTERS('fact_payments'[Status] = \"APPROVED\")"
    for name in ("Store Payment Amount", "Online Payment Amount"):
        desc, body = _measure("fact_payments", name)
        assert body.count(approved) == body.count("SUM('fact_payments'[Amount Cents])") == 2
        assert desc.startswith("Approved")
    assert _measure("fact_payments", "Total Amounts")[0].startswith("Approved")
    for name in ("Payment Count", "Store Payment Count", "Online Payment Count"):
        desc, body = _measure("fact_payments", name)
        assert "attempts" in desc and "APPROVED" not in body


def test_prior_year_measures_compare_the_same_days() -> None:
    # L-18: a partial (or gapped) current period meets the same days a year earlier.
    for table, name, base in (
        ("fact_receipts", "Store Sales PY", "[Net Sales]"),
        ("fact_receipts", "Omnichannel Revenue PY", "[Omnichannel Revenue]"),
        ("fact_receipts", "Net Sales (Prior Year)", "[Net Sales]"),
        ("fact_receipts", "Omnichannel Revenue (Prior Year)", "[Omnichannel Revenue]"),
    ):
        desc, body = _measure(table, name)
        assert "SUMMARIZE('fact_receipts', 'dim_date'[Date])" in body, name
        assert "EDATE('dim_date'[Date], -12)" in body, name
        assert f"CALCULATE({base}, REMOVEFILTERS('dim_date'), TREATAS(_PriorDays" in body
        assert "SAMEPERIODLASTYEAR" not in body
        assert "same calendar days" in desc.lower(), name
    _, latest_year = _measure("fact_receipts", "_Latest Sales Year")
    assert "[_Latest Sales Date]" in latest_year
    _, latest = _measure("fact_receipts", "_Latest Sales Date")
    assert "MAX('fact_receipts'[Event Date])" in latest


def test_sales_measures_state_their_tax_and_returns_basis() -> None:
    # L-20: measures that look alike say what they include.
    bases = {
        ("fact_receipts", "Total Sales"): ("including tax", "net of returns"),
        ("fact_receipts", "Total Subtotal"): ("excluding tax", "net of returns"),
        ("fact_receipts", "Net Sales"): ("excluding tax", "net of returns"),
        ("fact_receipts", "Gross Sales"): ("excluding tax", "before returns"),
        ("fact_receipts", "Average Basket Size"): ("excluding tax", "before returns"),
        ("fact_receipts", "Omnichannel Revenue"): ("excluding tax", "net of returns"),
        ("fact_receipt_lines", "Line Revenue"): ("excluding tax", "net of returns"),
        ("online_sales_daily", "Online Revenue"): ("including tax",),
        ("sales_minute_store", "Total Store Sales"): ("including tax", "before returns"),
        ("sales_minute_store", "Avg Store Basket"): ("including tax",),
    }
    for (table, name), phrases in bases.items():
        desc, _ = _measure(table, name)
        for phrase in phrases:
            assert phrase in desc, (name, phrase)
    proxy, _ = _measure("fact_foot_traffic", "Traffic Conversion Proxy %")
    assert proxy.startswith("Proxy, not a measured conversion rate")
    assert "live stream" in proxy


def _bounds(rows: list[tuple[float, float, float]]) -> tuple[float, float]:
    """Oracle of Forecast Lower/Upper Bound over (predicted, lower, upper) rows."""
    predicted = sum(p for p, _, _ in rows)
    low = math.sqrt(sum((p - lo) ** 2 for p, lo, _ in rows))
    high = math.sqrt(sum((hi - p) ** 2 for p, _, hi in rows))
    return max(0.0, predicted - low), predicted + high


def test_forecast_bounds_are_the_row_bound_and_combine_in_quadrature() -> None:
    # L-23: summed per-row quantiles are not an interval for the total.
    for side, gap in (
        ("Lower", "'demand_forecast'[Predicted Units] - 'demand_forecast'[Lower Bound]"),
        ("Upper", "'demand_forecast'[Upper Bound] - 'demand_forecast'[Predicted Units]"),
    ):
        desc, body = _measure("demand_forecast", f"Forecast {side} Bound")
        assert f"SQRT(SUMX('demand_forecast', ({gap}) ^ 2))" in body
        assert "SUM('demand_forecast'[Predicted Units])" in body
        assert f"SUM('demand_forecast'[{side} Bound])" not in body
        assert "quadrature" in desc
        _, window = _measure("demand_forecast", f"Forecast {side} Bound Next 7 Days")
        assert f"[Forecast {side} Bound]" in window

    row = [(10.0, 6.0, 15.0)]
    assert _bounds(row) == (6.0, 15.0)
    week = [(10.0, 6.0, 15.0)] * 7
    low, high = _bounds(week)
    assert 42.0 < low < 70.0 < high < 105.0  # inside the summed bounds (42, 105)


def test_attributed_revenue_measures_document_their_date_role() -> None:
    # L-27: purchase day vs touch day.
    attributed, _ = _measure("fact_marketing_attribution", "Attributed Revenue")
    campaign, _ = _measure("campaign_performance_daily", "Campaign Attributed Revenue")
    assert "dated by the purchase" in attributed and "Campaign Attributed Revenue" in attributed
    assert "day of the marketing touch" in campaign and "by purchase" in campaign


def test_twin_measures_observe_up_to_the_source_as_of() -> None:
    # C-16: pre-generated future truck stages are not "observed" as the clock moves.
    cutoff = (
        "VAR _ObservationCutoff =\n\t\t\t\tMIN(\n\t\t\t\t\tUTCNOW(),\n"
        "\t\t\t\t\tCALCULATE(MAX('fact_receipts'[Event Timestamp]), REMOVEFILTERS())\n"
    )
    counts = {
        table: _table(table).count("VAR _ObservationCutoff =")
        for table in ("dim_trucks", "dim_stores", "dim_distribution_centers")
    }
    assert counts == {"dim_trucks": 3, "dim_stores": 1, "dim_distribution_centers": 1}
    for table in counts:
        assert _table(table).count(cutoff) == counts[table]
        assert "UTC current time" not in _table(table)


def test_report_visuals_reference_existing_measures() -> None:
    measures = _model_measures()
    referenced = 0
    for report in REPORTS:
        for visual in report.rglob("visual.json"):
            text = json.dumps(json.loads(visual.read_text(encoding="utf-8")))
            for entity, name in re.findall(
                r'"Measure": \{"Expression": \{"SourceRef": \{"Entity": "([^"]+)"\}\}, '
                r'"Property": "([^"]+)"\}',
                text,
            ):
                referenced += 1
                assert name in measures.get(entity, set()), (visual, entity, name)
    assert referenced > 0
