"""Console severity must distinguish optional readiness from deployment failure."""

import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from deploy.scripts.verify_readiness import (
    CheckResult,
    ReadinessRunner,
    _print_summary,
    aggregate_status,
    write_report_atomic,
)

ABSENT_STREAM = {
    "freshness.watermarks": "ag._watermarks contains no source evidence",
    "freshness.eventhouse_ingestion": "Eventhouse returned no ingestion evidence",
    "freshness.checkpoint": "Kusto ingestion tags contain no checkpoint stream identity",
}


def _report(checks):
    return {
        "status": aggregate_status(checks),
        "counts": {
            name.lower(): sum(check.status == name for check in checks)
            for name in ("PASS", "FAIL", "UNKNOWN", "SKIPPED")
        },
        "checks": [check.to_dict() for check in checks],
    }


@pytest.mark.parametrize("structured_reason", [False, True])
def test_optional_stream_not_started_is_informational_not_an_error(
    capsys, structured_reason
):
    checks = [CheckResult("items.required", "inventory", True, True, "PASS", "Ready")]
    checks += [
        CheckResult(
            name,
            "freshness",
            True,
            False,
            "UNKNOWN",
            summary,
            evidence={"reason": "not_observed"} if structured_reason else {},
        )
        for name, summary in ABSENT_STREAM.items()
    ]
    report = _report(checks)
    original = deepcopy(report)

    _print_summary(report, Path("readiness-report.json"))

    captured = capsys.readouterr()
    assert "Required deployment READY" in captured.out
    assert "expected before" in captured.out
    assert "DEGRADED" in captured.out
    assert captured.err == ""
    assert report == original
    assert report["status"] == "DEGRADED"
    assert report["counts"]["unknown"] == 3


@pytest.mark.parametrize("status", ["FAIL", "UNKNOWN"])
def test_optional_problems_are_warnings_not_deployment_errors(capsys, status):
    report = _report(
        [
            CheckResult("required", "inventory", True, True, "PASS", "Ready"),
            CheckResult(
                "freshness.models.optional",
                "freshness",
                True,
                False,
                status,
                "Needs attention",
            ),
        ]
    )
    _print_summary(report, Path("report.json"))
    output = capsys.readouterr()
    assert "Optional freshness.models.optional" in output.err
    assert "ERROR:" not in output.err
    assert report["status"] == "DEGRADED"


def test_stream_network_error_is_not_presented_as_expected_inactivity(capsys):
    report = _report(
        [
            CheckResult("required", "inventory", True, True, "PASS", "Ready"),
            CheckResult(
                "freshness.checkpoint",
                "freshness",
                True,
                False,
                "UNKNOWN",
                "Live evidence unavailable (KustoNetworkError).",
                evidence={"error_type": "KustoNetworkError"},
            ),
        ]
    )
    _print_summary(report, Path("report.json"))
    output = capsys.readouterr()
    assert "Optional freshness.checkpoint" in output.err
    assert "expected before" not in output.out


@pytest.mark.parametrize("status", ["FAIL", "UNKNOWN"])
def test_required_problems_remain_errors_and_block_readiness(capsys, status):
    report = _report(
        [CheckResult("kql.tables", "kql", True, True, status, "Unavailable")]
    )
    _print_summary(report, Path("report.json"))
    output = capsys.readouterr()
    assert "ERROR: kql.tables" in output.err
    assert "Required deployment READY" not in output.out
    assert report["status"] == "FAILED"


def test_partial_stream_evidence_remains_a_warning(capsys):
    checks = [CheckResult("required", "inventory", True, True, "PASS", "Ready")]
    checks += [
        CheckResult(name, "freshness", True, False, "PASS", "Observed")
        for name in ("freshness.watermarks", "freshness.eventhouse_ingestion")
    ]
    checks.append(
        CheckResult(
            "freshness.checkpoint",
            "freshness",
            True,
            False,
            "UNKNOWN",
            ABSENT_STREAM["freshness.checkpoint"],
            evidence={"reason": "not_observed"},
        )
    )
    _print_summary(_report(checks), Path("report.json"))
    output = capsys.readouterr()
    assert "Optional freshness.checkpoint" in output.err
    assert "expected before" not in output.out


def test_missing_watermark_records_absence_without_changing_unknown_status():
    runner = object.__new__(ReadinessRunner)
    runner.adapter = SimpleNamespace(watermark_signal=lambda: None)
    runner.checks = []
    runner._check(
        "freshness.watermarks",
        "freshness",
        selected=True,
        required=False,
        function=runner._watermark_freshness,
    )
    assert runner.checks[0].status == "UNKNOWN"
    assert runner.checks[0].evidence == {"reason": "not_observed"}


def test_metadata_connection_failure_is_not_classified_as_absent_stream():
    def unavailable():
        raise ConnectionError("network unavailable")

    runner = object.__new__(ReadinessRunner)
    runner.adapter = SimpleNamespace(watermark_signal=unavailable)
    runner.checks = []
    runner._check(
        "freshness.watermarks",
        "freshness",
        selected=True,
        required=False,
        function=runner._watermark_freshness,
    )
    assert runner.checks[0].status == "UNKNOWN"
    assert runner.checks[0].evidence == {"error_type": "ConnectionError"}


def test_successful_data_publication_still_warns_about_cleanup(capsys):
    check = CheckResult(
        "freshness.setup_run_log",
        "freshness",
        True,
        True,
        "PASS",
        "Staging cleanup requires attention.",
        evidence={"cleanup_required": True},
    )
    report = _report([check])
    _print_summary(report, Path("report.json"))
    output = capsys.readouterr()
    assert "Non-blocking cleanup" in output.err
    assert "ERROR:" not in output.err
    assert report["status"] == "DEGRADED"


def test_persisted_report_keeps_all_control_records_while_bounding_evidence(tmp_path, capsys):
    checks = [
        CheckResult(
            f"check.{index}", "inventory", True, True, "PASS", "Ready",
            evidence={"token": "synthetic-secret", "samples": list(range(40))},
        )
        for index in range(30)
    ]
    report = _report(checks)
    report["freshness_lineage"] = [{"check_id": check.check_id} for check in checks]
    path = tmp_path / "report.json"

    write_report_atomic(path, report)

    saved = json.loads(path.read_text(encoding="utf-8"))
    assert [check["id"] for check in saved["checks"]] == [check.check_id for check in checks]
    assert len(saved["freshness_lineage"]) == 30
    assert all(isinstance(item, dict) for item in saved["freshness_lineage"])
    assert saved["checks"][0]["evidence"]["token"] == "[REDACTED]"
    assert len(saved["checks"][0]["evidence"]["samples"]) == 26
    assert "synthetic-secret" not in path.read_text(encoding="utf-8")
    _print_summary(saved, path)
    assert "Readiness SUCCEEDED" in capsys.readouterr().out


def test_report_writer_rejects_malformed_control_records(tmp_path):
    path = tmp_path / "report.json"
    with pytest.raises(ValueError, match="checks"):
        write_report_atomic(path, {"checks": ["[TRUNCATED]"]})
    assert not path.exists()
