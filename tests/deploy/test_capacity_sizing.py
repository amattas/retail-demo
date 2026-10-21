"""Capacity-aware custom Spark pool sizing tests."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

from deploy.scripts.capacity_sizing import (
    CapacitySizingError,
    derive_capacity_sizing,
    load_capacity_sizing_evidence,
    write_capacity_sizing_evidence,
)
from deploy.scripts.deploy_config import load_environment

REPO_ROOT = Path(__file__).resolve().parents[2]


def _capacity(sku: str) -> dict[str, str]:
    return {
        "id": "capacity-id",
        "displayName": "demo-capacity",
        "sku": sku,
    }


def _config(tmp_path: Path, profile: str = "full-demo"):
    environments = tmp_path / "environments"
    environments.mkdir(parents=True)
    (environments / "dev.yml").write_text(
        yaml.safe_dump(
            {
                "tenant_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "workspace": {
                    "name": "retail-demo-dev",
                    "capacity_id": "capacity-id",
                    "capacity_name": "demo-capacity",
                },
                "deployment": {"profile": profile},
            }
        ),
        encoding="utf-8",
    )
    return load_environment("dev", environments_root=environments)


@pytest.mark.parametrize(
    ("sku", "expected_max"),
    [
        ("F64", 2),
        ("FTL64", 2),
        ("F128", 4),
        ("F256", 8),
        ("F512", 16),
        ("F1024", 16),
    ],
)
def test_capacity_sizing_uses_base_vcores_and_caps_nodes(
    sku: str,
    expected_max: int,
) -> None:
    evidence = derive_capacity_sizing(
        environment="dev",
        capacity=_capacity(sku),
        repo_root=REPO_ROOT,
    )

    assert evidence.producer_concurrency == 6
    assert evidence.node_size == "Medium"
    assert evidence.node_vcores == 8
    assert evidence.min_node_count == 2
    assert evidence.max_node_count == expected_max
    assert evidence.aggregate_max_vcores == 6 * expected_max * 8
    assert evidence.usable_vcores == evidence.capacity_units * 2 * 3 // 4


def test_capacity_sizing_rejects_unsupported_and_undersized_skus() -> None:
    with pytest.raises(CapacitySizingError, match="unsupported"):
        derive_capacity_sizing(
            environment="dev",
            capacity=_capacity("P1"),
            repo_root=REPO_ROOT,
        )
    with pytest.raises(CapacitySizingError, match="driver and one Medium executor"):
        derive_capacity_sizing(
            environment="dev",
            capacity=_capacity("F32"),
            repo_root=REPO_ROOT,
        )


def test_evidence_rejects_malformed_identity_and_stale_values(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    path = tmp_path / "capacity-sizing.json"
    path.write_text("{", encoding="utf-8")
    with pytest.raises(CapacitySizingError, match="valid JSON"):
        load_capacity_sizing_evidence(
            path,
            config=config,
            repo_root=REPO_ROOT,
        )

    evidence = derive_capacity_sizing(
        environment="dev",
        capacity=_capacity("F64"),
        repo_root=REPO_ROOT,
        observed_at=datetime.now(UTC) - timedelta(hours=2),
    )
    write_capacity_sizing_evidence(path, evidence)
    with pytest.raises(CapacitySizingError, match="stale"):
        load_capacity_sizing_evidence(
            path,
            config=config,
            repo_root=REPO_ROOT,
        )

    payload = evidence.to_dict()
    payload["observed_at"] = datetime.now(UTC).isoformat()
    payload["capacity_id"] = "other-capacity"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(CapacitySizingError, match="capacity ID"):
        load_capacity_sizing_evidence(
            path,
            config=config,
            repo_root=REPO_ROOT,
        )


def test_evidence_rejects_required_ml_producer_count_drift(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    path = tmp_path / "capacity-sizing.json"
    evidence = derive_capacity_sizing(
        environment="dev",
        capacity=_capacity("F64"),
        repo_root=REPO_ROOT,
    )
    write_capacity_sizing_evidence(path, evidence)
    pipeline = tmp_path / "fabric" / "pipelines" / "ml-required.DataPipeline"
    pipeline.mkdir(parents=True)
    (pipeline / "pipeline-content.json").write_text(
        json.dumps(
            {
                "properties": {
                    "activities": [
                        {
                            "name": f"producer-{index}",
                            "type": "TridentNotebook",
                            "dependsOn": [],
                        }
                        for index in range(5)
                    ]
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(CapacitySizingError, match="producer count"):
        load_capacity_sizing_evidence(
            path,
            config=config,
            repo_root=tmp_path,
        )


def test_starter_profile_rejects_sizing_evidence(tmp_path: Path) -> None:
    starter = _config(tmp_path, profile="standard")
    evidence = derive_capacity_sizing(
        environment="dev",
        capacity=_capacity("F64"),
        repo_root=REPO_ROOT,
    )
    path = tmp_path / "capacity-sizing.json"
    write_capacity_sizing_evidence(path, evidence)

    with pytest.raises(CapacitySizingError, match="Starter-pool"):
        load_capacity_sizing_evidence(
            path,
            config=starter,
            repo_root=REPO_ROOT,
        )


def test_setup_pipeline_never_outgrows_the_sized_session_concurrency() -> None:
    """Pool sizing reserves room for the required-ML producers running at once.

    The setup pipeline runs independent notebooks concurrently, each in its own
    Spark session; its widest set of mutually independent notebooks must fit
    the same session budget.
    """
    from itertools import combinations

    from deploy.scripts.capacity_sizing import required_ml_producer_count

    content = json.loads(
        (
            REPO_ROOT / "fabric" / "pipelines" / "setup-pipeline.DataPipeline"
            / "pipeline-content.json"
        ).read_text(encoding="utf-8")
    )
    parents = {
        activity["name"]: {dep["activity"] for dep in activity["dependsOn"]}
        for activity in content["properties"]["activities"]
    }
    ancestors: dict[str, set[str]] = {}

    def upstream(name: str) -> set[str]:
        if name not in ancestors:
            ancestors[name] = set().union(
                *({parent} | upstream(parent) for parent in parents[name])
            ) if parents[name] else set()
        return ancestors[name]

    def independent(group: tuple[str, ...]) -> bool:
        return all(a not in upstream(b) and b not in upstream(a) for a, b in combinations(group, 2))

    width = max(
        size for size in range(1, len(parents) + 1)
        if any(independent(group) for group in combinations(parents, size))
    )
    # setup-02 validation can overlap 04-08, so up to six sessions run at once.
    assert width == 6
    assert width <= required_ml_producer_count(REPO_ROOT)
