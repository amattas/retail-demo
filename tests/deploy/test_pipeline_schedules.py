"""Every committed pipeline schedule is a valid Fabric schedule the deploy can enable."""

import json
import re
from datetime import datetime
from pathlib import Path

import pytest

from deploy.scripts.deploy_config import SchedulesConfig
from deploy.scripts.fabric_runtime import configured_schedule_document

ROOT = Path(__file__).resolve().parents[2]
SCHEDULE_FILES = sorted((ROOT / "fabric" / "pipelines").glob("*.DataPipeline/.schedules"))
SCHEMA = (
    "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/schedules/1.0.0/"
    "schema.json"
)
# Fabric Job Scheduler ScheduleConfig types and their type-specific fields.
TYPE_FIELDS = {
    "Cron": {"interval"},
    "Daily": {"times"},
    "Weekly": {"weekdays", "times"},
}
COMMON_FIELDS = {"type", "startDateTime", "endDateTime", "localTimeZoneId"}
WEEKDAYS = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}
# Windows time-zone IDs (Fabric's localTimeZoneId) used or expected in this repo.
TIME_ZONES = {
    "UTC", "Eastern Standard Time", "Central Standard Time", "Mountain Standard Time",
    "Pacific Standard Time", "GMT Standard Time", "W. Europe Standard Time",
}


def test_every_pipeline_schedule_file_is_found():
    assert {path.parent.stem for path in SCHEDULE_FILES} == {
        "daily-maintenance", "streaming-data-load",
    }


@pytest.mark.parametrize("path", SCHEDULE_FILES, ids=lambda path: path.parent.stem)
def test_schedule_matches_the_fabric_schedule_schema(path):
    document = json.loads(path.read_text(encoding="utf-8"))
    assert set(document) == {"$schema", "schedules"}
    assert document["$schema"] == SCHEMA
    assert document["schedules"]
    for schedule in document["schedules"]:
        assert set(schedule) == {"enabled", "jobType", "configuration"}
        assert isinstance(schedule["enabled"], bool)
        assert schedule["jobType"] == "Execute"
        configuration = schedule["configuration"]
        kind = configuration.get("type")
        assert kind in TYPE_FIELDS, kind
        assert set(configuration) == COMMON_FIELDS | TYPE_FIELDS[kind]
        start, end = (
            datetime.strptime(configuration[field], "%Y-%m-%dT%H:%M:%S")
            for field in ("startDateTime", "endDateTime")
        )
        assert end > start
        assert configuration["localTimeZoneId"] in TIME_ZONES
        if kind == "Cron":
            interval = configuration["interval"]
            assert isinstance(interval, int) and not isinstance(interval, bool)
            assert 1 <= interval <= 5_270_400
        if "times" in configuration:
            assert 1 <= len(configuration["times"]) <= 100
            assert all(re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", value)
                       for value in configuration["times"])
        if "weekdays" in configuration:
            assert set(configuration["weekdays"]) <= WEEKDAYS


@pytest.mark.parametrize("path", SCHEDULE_FILES, ids=lambda path: path.parent.stem)
def test_deploy_configuration_can_enable_every_committed_schedule(path):
    every = SchedulesConfig(daily_maintenance=True, streaming_data_load=True)
    assert path.parent.stem in every.enabled_pipelines()
    document = json.loads(path.read_text(encoding="utf-8"))
    enabled = configured_schedule_document(document, enabled=True)
    assert all(schedule["enabled"] for schedule in enabled["schedules"])


def test_streaming_cadence_is_the_one_the_notebooks_document():
    document = json.loads(
        (ROOT / "fabric" / "pipelines" / "streaming-data-load.DataPipeline" / ".schedules")
        .read_text(encoding="utf-8")
    )
    interval = document["schedules"][0]["configuration"]["interval"]
    for name in ("03-streaming-to-silver.ipynb", "04-streaming-to-gold.ipynb"):
        notebook = json.loads((ROOT / "fabric" / "lakehouse" / name).read_text(encoding="utf-8"))
        header = "".join(notebook["cells"][0]["source"])
        assert f"**every {interval} minutes**" in header
