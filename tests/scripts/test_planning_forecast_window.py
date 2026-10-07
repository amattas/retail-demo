"""Static contracts for the native seven-day forecast window measures."""

from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TABLE_PATH = (
    REPO_ROOT
    / "fabric"
    / "powerbi"
    / "retail_model.SemanticModel"
    / "definition"
    / "tables"
    / "demand_forecast.tmdl"
)
TABLE_TEXT = TABLE_PATH.read_text(encoding="utf-8")

EXPECTED_MEASURES = {
    "Forecasted Units Next 7 Days": "[Forecasted Units]",
    "Forecast Lower Bound Next 7 Days": "[Forecast Lower Bound]",
    "Forecast Upper Bound Next 7 Days": "[Forecast Upper Bound]",
}


def _measure_body(measure_name: str) -> str:
    text = TABLE_TEXT
    match = re.search(
        rf"^\tmeasure '{re.escape(measure_name)}' = ```\r?\n(?P<body>.*?)^\t+```\r?\n",
        text,
        re.MULTILINE | re.DOTALL,
    )
    assert match, f"Missing measure {measure_name!r} in {TABLE_PATH.name}"
    return match.group("body")


def _compact(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _next_seven_day_window(source_as_of: date | None) -> list[date] | None:
    if source_as_of is None:
        return None
    return [source_as_of + timedelta(days=offset) for offset in range(1, 8)]


@pytest.mark.parametrize("measure_name, base_measure", EXPECTED_MEASURES.items())
def test_forecast_window_measures_are_source_anchored(measure_name: str, base_measure: str) -> None:
    body = _compact(_measure_body(measure_name))

    assert f"measure '{measure_name}'" in TABLE_TEXT
    assert "MAX('demand_forecast'[Source As Of])" in body
    assert "ISBLANK(_SourceAsOfDateTime)" in body
    assert "BLANK()" in body
    assert "INT(_SourceAsOfDateTime)" in body
    assert "_WindowStart = _SourceAsOfDate + 1" in body
    assert "_WindowEndExclusive = _SourceAsOfDate + 8" in body
    assert base_measure in body
    assert "REMOVEFILTERS(" not in body
    assert "ALL(" not in body
    assert "KEEPFILTERS('demand_forecast'[Forecast Date] >= _WindowStart)" in body
    assert "KEEPFILTERS('demand_forecast'[Forecast Date] < _WindowEndExclusive)" in body
    assert "TODAY(" not in body
    assert "UTCNOW(" not in body
    assert "FILTER(" not in body
    assert "Forecast Horizon Days] = 7" not in body
    assert "Generated At" not in body


def test_next_seven_day_window_oracle_excludes_anchor_day_and_preserves_blank() -> None:
    assert _next_seven_day_window(None) is None
    assert _next_seven_day_window(date(2026, 1, 1)) == [
        date(2026, 1, 2),
        date(2026, 1, 3),
        date(2026, 1, 4),
        date(2026, 1, 5),
        date(2026, 1, 6),
        date(2026, 1, 7),
        date(2026, 1, 8),
    ]
    leap_window = _next_seven_day_window(date(2024, 2, 28))
    assert leap_window == [
        date(2024, 2, 29),
        date(2024, 3, 1),
        date(2024, 3, 2),
        date(2024, 3, 3),
        date(2024, 3, 4),
        date(2024, 3, 5),
        date(2024, 3, 6),
    ]
