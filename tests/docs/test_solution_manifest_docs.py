"""Standard-library contracts for manifest-owned documentation claims."""

from __future__ import annotations

import ast
import importlib.util
import json
import re
import sys
import tomllib
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = ROOT / "contracts" / "retail-demo.json"
GUIDE_PATH = ROOT / "docs" / "guides" / "workspace-inventory.md"


def _manifest() -> dict[str, Any]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _literal_assignment(path: Path, name: str) -> Any:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        ):
            return ast.literal_eval(node.value)
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == name
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in {path}")


def _dict_assignment_keys(path: Path, name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        target: ast.expr | None = None
        value: ast.expr | None = None
        if isinstance(node, ast.Assign):
            if any(
                isinstance(candidate, ast.Name) and candidate.id == name
                for candidate in node.targets
            ):
                target, value = node.targets[0], node.value
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == name
        ):
            target, value = node.target, node.value
        if target is not None:
            assert isinstance(value, ast.Dict)
            return {ast.literal_eval(key) for key in value.keys}
    raise AssertionError(f"{name} not found in {path}")


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _catalog_records(document: dict[str, Any]) -> list[dict[str, Any]]:
    sections = (
        "prerequisites",
        "commands",
        "assets",
        "profiles",
        "readiness_expectations",
        "data_contracts",
        "event_paths",
        "exceptions",
        "ml_contracts",
        "source_owners",
    )
    return [document["metadata"], *(
        record for section in sections for record in document[section]
    )]


def test_manifest_ids_references_descriptions_and_sources_are_owned() -> None:
    document = _manifest()
    records = _catalog_records(document)
    nested_records = [
        record
        for entry in (*document["assets"], *document["data_contracts"])
        for field in ("inventories", "inventory_rules")
        for record in entry.get(field, ())
    ]
    for profile in document["profiles"]:
        nested_records.extend(profile.get("required_acknowledgements", ()))
        nested_records.extend(profile.get("blockers", ()))
    for contract in document["data_contracts"]:
        nested_records.extend(contract.get("source_agreements", ()))
        nested_records.extend(contract.get("events", ()))
        if contract.get("envelope"):
            nested_records.append(contract["envelope"])
    ids = [record["id"] for record in (*records, *nested_records)]

    assert len(ids) == len(set(ids))
    assert all(record.get("description", "").strip() for record in records)
    assert all(
        record["support_status"]
        in {"core", "experimental", "manual", "optional", "preview"}
        for record in records
        if "support_status" in record
    )

    known_ids = set(ids)
    owner_ids = {record["id"] for record in document["source_owners"]}
    asset_ids = {record["id"] for record in document["assets"]}
    profile_ids = {record["id"] for record in document["profiles"]}
    contract_ids = {record["id"] for record in document["data_contracts"]}
    event_ids = {
        event["id"]
        for contract in document["data_contracts"]
        for event in contract.get("events", ())
    }
    assert all(record["owner"] in owner_ids for record in records if "owner" in record)
    for asset in document["assets"]:
        assert set(asset.get("depends_on", ())) <= asset_ids
    for profile in document["profiles"]:
        assert set(profile["asset_refs"]) <= asset_ids
    for expectation in document["readiness_expectations"]:
        assert set(expectation["profile_refs"]) <= profile_ids
    for path in document["event_paths"]:
        assert set(path["contract_ids"]) <= contract_ids
        assert set(path.get("event_ids", ())) <= event_ids
    for contract in document["ml_contracts"]:
        assert set(contract["asset_ids"]) <= asset_ids
        assert set(contract["data_contract_ids"]) <= contract_ids

    def source_paths(value: Any) -> set[str]:
        if isinstance(value, list):
            return set().union(*(source_paths(item) for item in value))
        if isinstance(value, dict):
            found = (
                {value["path"]}
                if isinstance(value.get("path"), str)
                else set()
            )
            return found | set().union(
                *(source_paths(item) for item in value.values())
            )
        return set()

    source_paths = source_paths(document)
    assert source_paths
    assert all((ROOT / path).exists() for path in source_paths)
    assert known_ids


def test_prerequisites_and_setup_projection_match_authoritative_constraints() -> None:
    document = _manifest()
    prerequisites = {item["id"]: item for item in document["prerequisites"]}
    pyproject = tomllib.loads(
        (ROOT / "utility" / "pyproject.toml").read_text(encoding="utf-8")
    )
    setup_path = ROOT / "scripts" / "setup.py"
    setup_minimum = _literal_assignment(setup_path, "MIN_PYTHON")
    providers = (ROOT / "deploy" / "terraform" / "providers.tf").read_text(
        encoding="utf-8"
    )

    python_requirement = prerequisites["prerequisite.python"]["requirement"]
    assert python_requirement == pyproject["project"]["requires-python"]
    assert python_requirement == f">={setup_minimum[0]}.{setup_minimum[1]}"
    terraform_requirement = prerequisites["prerequisite.terraform"]["requirement"]
    match = re.search(r'required_version\s*=\s*"([^"]+)"', providers)
    assert match is not None
    assert terraform_requirement == match.group(1).replace(" ", "")

    setup = _load_module("_manifest_docs_setup", setup_path)
    expected_commands = {
        item["check_command"][0]
        for item in document["prerequisites"]
        if item["bootstrap_required"]
        and item["check_command"]
        and item["id"] != "prerequisite.python"
    }
    assert set(setup.prerequisites()) == expected_commands
    assert setup.PROFILE_CHOICES == tuple(
        profile["deployment_name"] for profile in document["profiles"]
    )
    assert setup.DEFAULT_PROFILE == next(
        profile["deployment_name"]
        for profile in document["profiles"]
        if profile["default"]
    )
    assert setup.BOOTSTRAP_MANIFEST.version == document["version"]


def test_canonical_command_examples_use_real_cli_flags() -> None:
    document = _manifest()
    cli_path = ROOT / "utility" / "src" / "retail_setup" / "cli" / "main.py"
    cli_source = cli_path.read_text(encoding="utf-8")
    tree = ast.parse(cli_source, filename=str(cli_path))
    functions = {
        node.name: ast.get_source_segment(cli_source, node) or ""
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    for command in document["commands"]:
        assert command["examples"]
        for example in command["examples"]:
            assert example[: len(command["argv"])] == command["argv"]
            if command["id"] == "command.bootstrap":
                assert (ROOT / example[1]).is_file()
                continue
            function_name = command["argv"][1].replace("-", "_")
            function_source = functions[function_name]
            for token in example[len(command["argv"]) :]:
                if token.startswith("--"):
                    assert f'"{token}"' in function_source

    guide = GUIDE_PATH.read_text(encoding="utf-8")
    for command in document["commands"]:
        for example in command["examples"]:
            assert " ".join(example) in guide


def test_source_derived_data_event_model_and_ml_counts_match_guide() -> None:
    document = _manifest()
    tables = _literal_assignment(
        ROOT / "utility" / "src" / "retail_setup" / "generation" / "schemas.py",
        "TABLES",
    )
    gold_tables = _literal_assignment(
        ROOT / "utility" / "src" / "retail_setup" / "generation" / "gold.py",
        "GOLD_TABLES",
    )
    event_names = _dict_assignment_keys(
        ROOT / "utility" / "notebooks" / "templates" / "driver-05-stream.py",
        "EVENT_PAYLOADS",
    )
    kql = (ROOT / "fabric" / "kql_database" / "01-create-tables.kql").read_text(
        encoding="utf-8"
    )
    kql_tables = set(
        re.findall(
            r"(?im)^\s*\.create-merge\s+table\s+([A-Za-z_][A-Za-z0-9_]*)",
            kql,
        )
    )
    model = (
        ROOT
        / "fabric"
        / "powerbi"
        / "retail_model.SemanticModel"
        / "definition"
        / "model.tmdl"
    ).read_text(encoding="utf-8")
    model_tables = set(re.findall(r"(?m)^ref table ([^\s]+)$", model))
    required_ml = {
        contract["output"]["table"]
        for contract in document["ml_contracts"]
        if contract["tier"] == "required"
    }
    tier_counts = {
        tier: sum(
            contract["tier"] == tier for contract in document["ml_contracts"]
        )
        for tier in ("required", "optional", "experimental")
    }

    dimensions = {name for name in tables if name.startswith("dim_")}
    facts = {name for name in tables if name.startswith("fact_")}
    assert (len(dimensions), len(facts), len(gold_tables)) == (7, 19, 10)
    assert len(event_names) == 18
    assert kql_tables == event_names | {"unknown_event"}
    assert len(model_tables) == 40
    assert model_tables == set(tables) | set(gold_tables) | required_ml
    assert tier_counts == {"required": 4, "optional": 6, "experimental": 4}

    guide = GUIDE_PATH.read_text(encoding="utf-8")
    for claim in (
        "**7 dimensions**",
        "**19 facts**",
        "**10 Gold aggregates**",
        "**18 emitted business event types**",
        "**19 KQL event tables**",
        "**40 tables**",
        "| required | 4 |",
        "| optional | 6 |",
        "| experimental | 4 |",
    ):
        assert claim in guide


def test_profile_inventory_safety_and_guide_rows_match_manifest() -> None:
    document = _manifest()
    asset_status = {
        asset["id"]: asset["support_status"] for asset in document["assets"]
    }
    notebook_groups = _literal_assignment(
        ROOT / "deploy" / "scripts" / "build_artifacts.py",
        "NOTEBOOK_GROUPS",
    )
    guide = GUIDE_PATH.read_text(encoding="utf-8")
    support_labels = {
        "core": "core/default",
        "standard": "supported opt-in",
        "full-demo": "preview/acknowledged",
    }
    configured_kql_scripts = tuple(
        path.name
        for path in sorted((ROOT / "fabric" / "kql_database").glob("*.kql"))
    )

    for profile in document["profiles"]:
        assert "reset" not in profile["group_refs"]
        assert set(profile["group_refs"]) <= set(notebook_groups)
        assert all(
            (ROOT / "fabric" / "pipelines" / reference).is_dir()
            for reference in profile["pipeline_refs"]
        )
        kql_scripts = (
            configured_kql_scripts
            if "asset.eventhouse" in profile["asset_refs"]
            else ()
        )
        publication = profile["publication"]
        total = (
            publication["infrastructure_item_count"]
            + publication["reporting_item_count"]
        )
        row = (
            f"| `{profile['deployment_name']}` | "
            f"{support_labels[profile['deployment_name']]} | "
            f"{len(profile['asset_refs'])} | {len(profile['group_refs'])} | "
            f"{len(profile['pipeline_refs'])} | {len(kql_scripts)} | "
            f"{publication['infrastructure_item_count']} | "
            f"{publication['reporting_item_count']} | {total} |"
        )
        assert row in guide
        if profile["deployment_name"] == "standard":
            assert all(
                asset_status[asset_id] != "preview"
                for asset_id in profile["asset_refs"]
            )


def test_platform_and_taskflow_descriptions_match_manifest_ownership() -> None:
    document = _manifest()
    descriptions = {
        asset["id"]: asset["description"] for asset in document["assets"]
    }
    roots = {
        ROOT / "fabric" / "pipelines": "asset.data-pipelines",
        ROOT / "fabric" / "data-agents": "asset.data-agents",
        ROOT / "fabric" / "powerbi" / "retail_model.Report": "asset.report",
        (
            ROOT / "fabric" / "powerbi" / "retail_model.SemanticModel"
        ): "asset.semantic-model",
    }
    platform_paths = list((ROOT / "fabric").rglob(".platform"))
    assert platform_paths
    for platform_path in platform_paths:
        matches = [
            asset_id
            for root, asset_id in roots.items()
            if platform_path.is_relative_to(root)
        ]
        assert len(matches) == 1
        metadata = json.loads(platform_path.read_text(encoding="utf-8"))["metadata"]
        assert metadata["description"] == descriptions[matches[0]]

    taskflow = json.loads(
        (ROOT / "fabric" / "taskflow" / "taskflow.json").read_text(
            encoding="utf-8"
        )
    )
    assert taskflow["description"] == descriptions["asset.task-flow"]
    assert all(task.get("description", "").strip() for task in taskflow["tasks"])
    serialized = json.dumps(taskflow)
    assert "Package your items for distribution" not in serialized
    assert "created by the setup pipeline" not in serialized


def test_manifest_pages_and_workflow_paths_are_navigated() -> None:
    config = tomllib.loads((ROOT / "zensical.toml").read_text(encoding="utf-8"))

    def nav_paths(value: Any) -> set[str]:
        if isinstance(value, str):
            return {value}
        if isinstance(value, list):
            return set().union(*(nav_paths(item) for item in value))
        if isinstance(value, dict):
            return set().union(*(nav_paths(item) for item in value.values()))
        return set()

    paths = nav_paths(config["project"]["nav"])
    assert "guides/workspace-inventory.md" in paths
    assert "design/specifications/core/solution-manifest.md" in paths

    for path in (
        ROOT / "README.md",
        ROOT / "deploy" / "README.md",
        ROOT / "utility" / "README.md",
        ROOT / "fabric" / "README.md",
        ROOT / "docs" / "guides" / "README.md",
    ):
        assert "workspace-inventory.md" in path.read_text(encoding="utf-8")

    workflow = (ROOT / ".github" / "workflows" / "docs.yml").read_text(
        encoding="utf-8"
    )
    for watched_path in (
        "contracts/retail-demo.json",
        "scripts/solution_manifest.py",
        "utility/src/retail_setup/**",
        "tests/docs/**",
    ):
        assert watched_path in workflow
    assert "python -m pytest tests/docs -q" in workflow
