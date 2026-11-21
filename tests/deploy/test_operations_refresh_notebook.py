"""The scheduled availability refresh must select and build signals like the deploy."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from deploy.scripts import _operations_scenario as scenario

ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK = ROOT / "fabric" / "lakehouse" / "17-refresh-availability-signals.ipynb"
PIPELINE = ROOT / "fabric" / "pipelines" / "daily-maintenance.DataPipeline" / "pipeline-content.json"
NOW = datetime(2026, 9, 7, 12, tzinfo=UTC)


def _logic():
    cells = json.loads(NOTEBOOK.read_text(encoding="utf-8"))["cells"]
    source = next(
        "".join(cell["source"])
        for cell in cells
        if cell["cell_type"] == "code" and "def build_signals(" in "".join(cell["source"])
    )
    namespace = {"LAKEHOUSE_NAME": "retail_lakehouse", "SILVER_DB": "ag", "GOLD_DB": "au"}
    exec(compile("import hashlib, json, math, uuid\n"
                 "from datetime import datetime, timedelta, timezone\n" + source,
                 str(NOTEBOOK), "exec"), namespace)
    return namespace


def _row(**changes):
    return {
        "store_id": 1, "product_id": 2, "product_name": "Milk",
        "source_as_of": NOW - timedelta(days=1), "forecast_as_of": NOW - timedelta(days=1),
        "forecast_days": 7, "forecast_rows": 7, "forecast_versions": 1,
        "current_inventory": 4, "forecast_units_7d": 10.1, "stockout_probability": 0.8,
        "risk_horizon_days": 3, "days_of_inventory": 2.0, **changes,
    }


@pytest.mark.parametrize("plan_cohort", [False, True])
def test_refresh_query_is_the_deploy_query_in_spark_sql(plan_cohort):
    logic = _logic()
    expected = scenario.scenario_sql(
        plan_cohort=plan_cohort, dialect="spark", catalog="retail_lakehouse"
    )
    assert " ".join(logic["scenario_query"](plan_cohort).split()) == " ".join(expected.split())


@pytest.mark.parametrize("plan_cohort", [False, True])
def test_refresh_builds_the_same_signals_as_the_deploy(plan_cohort):
    logic = _logic()
    rows = [
        _row(),
        # A slow mover's last ledger change must not become its source time.
        _row(product_id=3, source_as_of=NOW - timedelta(days=11), stockout_probability=0.2,
             days_of_inventory=9.0),
    ]
    expected = scenario.scenario_signals(rows, NOW, plan_cohort=plan_cohort)
    assert logic["build_signals"](rows, NOW, plan_cohort) == expected
    assert [logic["meets_alert_rule"](signal) for signal in expected] == [
        scenario.meets_alert_rule(signal) for signal in expected
    ]
    assert [name for name, _kind in logic["FIELDS"]] == [name for name, _ in scenario.FIELDS]


@pytest.mark.parametrize(
    "changes",
    [
        {"store_id": 6},
        {"forecast_rows": 6},
        {"stockout_probability": 1.5},
        {"source_as_of": NOW - timedelta(days=9)},
    ],
)
def test_refresh_rejects_what_the_deploy_rejects(changes):
    logic = _logic()
    with pytest.raises(ValueError):
        scenario.scenario_signals([_row(**changes)], NOW, plan_cohort=True)
    with pytest.raises(ValueError):
        logic["build_signals"]([_row(**changes)], NOW, True)


def test_stale_snapshot_is_a_distinct_skip_not_a_contract_failure():
    logic = _logic()
    with pytest.raises(logic["StaleSnapshotError"]):
        logic["build_signals"]([_row(source_as_of=NOW - timedelta(days=9))], NOW, False)
    with pytest.raises(ValueError) as error:
        logic["build_signals"]([_row(store_id=0)], NOW, False)
    assert not isinstance(error.value, logic["StaleSnapshotError"])


def test_daily_maintenance_refreshes_signals_after_their_ml_inputs():
    activities = {
        activity["name"]: {
            dependency["activity"] for dependency in activity.get("dependsOn", [])
        }
        for activity in json.loads(PIPELINE.read_text(encoding="utf-8"))["properties"][
            "activities"
        ]
    }
    assert activities["17-refresh-availability-signals"] == {
        "06-ml-demand-forecast",
        "12-ml-stockout-prediction",
    }
