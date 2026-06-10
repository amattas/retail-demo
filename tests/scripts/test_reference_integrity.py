"""Reference integrity checks for PBIP and project-local imports."""

from __future__ import annotations

import ast
import json
import re
from collections import deque
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SEMANTIC_DEFINITION = (
    REPO_ROOT / "fabric" / "powerbi" / "retail_model.SemanticModel" / "definition"
)
REPORT_DEFINITION = (
    REPO_ROOT / "fabric" / "powerbi" / "retail_model.Report" / "definition"
)
DATAGEN_SRC = REPO_ROOT / "datagen" / "src"


def _strip_identifier(value: str) -> str:
    value = value.strip()
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    return value


def _parse_column_reference(value: str) -> tuple[str, str]:
    value = value.strip()
    if value.startswith("'"):
        end = value.find("'", 1)
        table = value[1:end]
        column = _strip_identifier(value[end + 2 :])
        return table, column

    table, column = value.split(".", 1)
    return _strip_identifier(table), _strip_identifier(column)


def _load_active_filter_graph(relationships: str) -> dict[str, set[str]]:
    """Return active single-direction filter edges from TMDL relationships."""
    graph: dict[str, set[str]] = {}

    for block in re.split(r"(?=^relationship )", relationships, flags=re.MULTILINE):
        if not block.startswith("relationship "):
            continue
        if re.search(r"^\tisActive: false$", block, re.MULTILINE):
            continue

        from_match = re.search(r"^\tfromColumn: (.+)$", block, re.MULTILINE)
        to_match = re.search(r"^\ttoColumn: (.+)$", block, re.MULTILINE)
        if not from_match or not to_match:
            continue

        from_table, _ = _parse_column_reference(from_match.group(1))
        to_table, _ = _parse_column_reference(to_match.group(1))

        graph.setdefault(to_table, set()).add(from_table)
        graph.setdefault(from_table, set())
        if re.search(
            r"^\tcrossFilteringBehavior: bothDirections$", block, re.MULTILINE
        ):
            graph[from_table].add(to_table)

    return graph


def _can_filter(
    graph: dict[str, set[str]], source_table: str, target_table: str
) -> bool:
    if source_table == target_table:
        return True

    queue: deque[str] = deque([source_table])
    seen = {source_table}
    while queue:
        table = queue.popleft()
        for next_table in graph.get(table, set()):
            if next_table == target_table:
                return True
            if next_table not in seen:
                seen.add(next_table)
                queue.append(next_table)
    return False


def _load_semantic_tables() -> dict[str, dict[str, set[str]]]:
    tables: dict[str, dict[str, set[str]]] = {}
    for path in sorted((SEMANTIC_DEFINITION / "tables").glob("*.tmdl")):
        content = path.read_text(encoding="utf-8")
        table_match = re.search(r"^table (.+)$", content, re.MULTILINE)
        assert table_match, f"{path.relative_to(REPO_ROOT)} has no table declaration"

        table_name = _strip_identifier(table_match.group(1))
        columns = {
            _strip_identifier(match.group(1).strip().rstrip("=").strip())
            for match in re.finditer(
                r"^\tcolumn (.+?)(?: =)?$", content, re.MULTILINE
            )
        }
        measures = {
            _strip_identifier(match.group(1))
            for match in re.finditer(r"^\tmeasure (.+?) =", content, re.MULTILINE)
        }
        hierarchies: set[str] = set()
        for hier_match in re.finditer(
            r"^\thierarchy (.+?)\n(?P<body>.*?)(?=^\t(?:column|measure|hierarchy|partition) |\Z)",
            content,
            re.MULTILINE | re.DOTALL,
        ):
            hierarchy_name = _strip_identifier(hier_match.group(1))
            for level_match in re.finditer(
                r"^\t\tlevel (.+?)$", hier_match.group("body"), re.MULTILINE
            ):
                hierarchies.add(
                    f"{hierarchy_name}.{_strip_identifier(level_match.group(1))}"
                )
        tables[table_name] = {
            "columns": columns,
            "measures": measures,
            "hierarchies": hierarchies,
        }

    return tables


def _load_column_metadata() -> dict[str, dict[str, str]]:
    metadata: dict[str, dict[str, str]] = {}
    for path in sorted((SEMANTIC_DEFINITION / "tables").glob("*.tmdl")):
        content = path.read_text(encoding="utf-8")
        table_match = re.search(r"^table (.+)$", content, re.MULTILINE)
        assert table_match, f"{path.relative_to(REPO_ROOT)} has no table declaration"

        table_name = _strip_identifier(table_match.group(1))
        metadata[table_name] = {}
        for match in re.finditer(
            r"^\tcolumn (.+?)\n(?P<body>.*?)(?=^\t(?:column|measure|hierarchy|partition) |\Z)",
            content,
            re.MULTILINE | re.DOTALL,
        ):
            column_name = _strip_identifier(match.group(1).strip().rstrip("=").strip())
            metadata[table_name][column_name] = match.group("body")

    return metadata


_AGGREGATION_FUNCTIONS = (
    r"(?:SUMX?|AVERAGEX?|MINX?|MAXX?|COUNTX?|COUNTAX?|DISTINCTCOUNT|COUNTROWS|"
    r"PRODUCTX?|MEDIANX?|GEOMEANX?)"
)


def _load_measure_expressions() -> dict[str, tuple[str, str]]:
    """Map measure name -> (home table, DAX expression)."""
    expressions: dict[str, tuple[str, str]] = {}
    for path in sorted((SEMANTIC_DEFINITION / "tables").glob("*.tmdl")):
        content = path.read_text(encoding="utf-8")
        table_match = re.search(r"^table (.+)$", content, re.MULTILINE)
        assert table_match, f"{path.relative_to(REPO_ROOT)} has no table declaration"

        table_name = _strip_identifier(table_match.group(1))
        for match in re.finditer(
            r"^\tmeasure (.+?) = (.*)$", content, re.MULTILINE
        ):
            measure_name = _strip_identifier(match.group(1))
            expressions[measure_name] = (table_name, match.group(2))

    return expressions


def _load_measure_base_tables() -> dict[str, set[str]]:
    """Map each measure to the tables its DAX (transitively) aggregates over.

    A visual column filters a measure through whichever tables the measure
    scans, not through the measure's home table, so relationship-path checks
    must use these base tables. Falls back to the home table for measures
    without a recognizable aggregation.
    """
    expressions = _load_measure_expressions()
    resolved: dict[str, set[str]] = {}

    def resolve(name: str, visiting: set[str]) -> set[str]:
        if name in resolved:
            return resolved[name]
        if name in visiting:
            return set()
        visiting.add(name)

        home_table, expression = expressions[name]
        tables: set[str] = set()
        for match in re.finditer(
            _AGGREGATION_FUNCTIONS + r"\s*\(\s*(?:'([^']+)'|([A-Za-z_]\w*))\s*[\[,)]",
            expression,
        ):
            tables.add(match.group(1) or match.group(2))

        for match in re.finditer(r"(?:^|[^'\w\]])\[([^\[\]]+)\]", expression):
            referenced = match.group(1)
            if referenced in expressions:
                tables |= resolve(referenced, visiting)

        visiting.discard(name)
        resolved[name] = tables or {home_table}
        return resolved[name]

    for measure_name in expressions:
        resolve(measure_name, set())

    return resolved


def _assert_report_field(
    tables: dict[str, dict[str, set[str]]],
    entity: str,
    property_name: str,
    field_type: str,
    path: Path,
) -> None:
    assert entity in tables, (
        f"{path.relative_to(REPO_ROOT)} references missing table {entity!r}"
    )
    valid_fields = tables[entity]["measures" if field_type == "Measure" else "columns"]
    assert property_name in valid_fields, (
        f"{path.relative_to(REPO_ROOT)} references missing {field_type.lower()} "
        f"{entity}[{property_name}]"
    )


def _walk_report_fields(
    obj: object,
    tables: dict[str, dict[str, set[str]]],
    path: Path,
    query_refs: set[str],
    visual_fields: list[tuple[str, str, str]],
) -> None:
    if isinstance(obj, dict):
        for field_type in ("Column", "Measure"):
            field = obj.get(field_type)
            if isinstance(field, dict):
                expression = field.get("Expression")
                source_ref = (
                    expression.get("SourceRef")
                    if isinstance(expression, dict)
                    else None
                )
                entity = (
                    source_ref.get("Entity") if isinstance(source_ref, dict) else None
                )
                property_name = field.get("Property")
                if isinstance(entity, str) and isinstance(property_name, str):
                    _assert_report_field(
                        tables, entity, property_name, field_type, path
                    )
                    visual_fields.append((field_type, entity, property_name))

        for key in ("queryRef", "metadata"):
            value = obj.get(key)
            if isinstance(value, str):
                query_refs.add(value)

        for value in obj.values():
            _walk_report_fields(value, tables, path, query_refs, visual_fields)
    elif isinstance(obj, list):
        for value in obj:
            _walk_report_fields(value, tables, path, query_refs, visual_fields)


def _assert_visual_relationship_paths(
    visual_fields: list[tuple[str, str, str]],
    active_filter_graph: dict[str, set[str]],
    measure_base_tables: dict[str, set[str]],
    path: Path,
) -> None:
    """Ensure visual columns can filter each measure through active relationships.

    Measures are checked against the tables their DAX scans (base tables);
    the column must reach at least one of them through active relationships.
    """
    unique_fields = list(dict.fromkeys(visual_fields))
    columns = [field for field in unique_fields if field[0] == "Column"]
    measures = [field for field in unique_fields if field[0] == "Measure"]

    issues = []
    for _, measure_table, measure_name in measures:
        base_tables = measure_base_tables.get(measure_name, {measure_table})
        for _, column_table, column_name in columns:
            if not any(
                _can_filter(active_filter_graph, column_table, base_table)
                for base_table in base_tables
            ):
                issues.append(
                    f"{column_table}[{column_name}] cannot filter "
                    f"{measure_table}[{measure_name}] "
                    f"(scans {sorted(base_tables)})"
                )

    assert not issues, (
        f"{path.relative_to(REPO_ROOT)} has disconnected visual fields:\n"
        + "\n".join(issues)
    )


def test_pbip_semantic_and_report_references_are_valid() -> None:
    """Validate semantic model, report visual, page, and layout references."""
    tables = _load_semantic_tables()
    column_metadata = _load_column_metadata()
    assert tables, "No semantic model tables were parsed"

    expressions = (SEMANTIC_DEFINITION / "expressions.tmdl").read_text(
        encoding="utf-8"
    )
    assert "PBI_RemovedChildren" not in expressions, (
        "Do not keep stale Direct Lake removed-child metadata in expressions.tmdl; "
        "it can preserve references to tables that are no longer in the active model."
    )

    model = (SEMANTIC_DEFINITION / "model.tmdl").read_text(encoding="utf-8")
    model_refs = {
        _strip_identifier(match.group(1))
        for match in re.finditer(r"^ref table (.+)$", model, re.MULTILINE)
    }
    assert model_refs == set(tables), (
        f"model.tmdl refs differ from table files: "
        f"missing={sorted(set(tables) - model_refs)}, "
        f"extra={sorted(model_refs - set(tables))}"
    )

    for table_name, columns in column_metadata.items():
        key_columns = [
            column_name
            for column_name, body in columns.items()
            if re.search(r"^\t\tisKey$", body, re.MULTILINE)
        ]
        assert len(key_columns) <= 1, (
            f"{table_name} has multiple isKey columns: {key_columns}. "
            "Power BI permits only one key column per table."
        )

    relationships = (SEMANTIC_DEFINITION / "relationships.tmdl").read_text(
        encoding="utf-8"
    )
    active_filter_graph = _load_active_filter_graph(relationships)
    for block in re.split(r"(?=^relationship )", relationships, flags=re.MULTILINE):
        if not block.startswith("relationship "):
            continue

        relationship_name = block.splitlines()[0]
        is_inactive = re.search(r"^\tisActive: false$", block, re.MULTILINE)
        for property_name in ("fromColumn", "toColumn"):
            assert re.search(
                rf"^\t{property_name}: ", block, re.MULTILINE
            ), f"{relationship_name} is missing {property_name}"

        # Power BI Desktop omits properties that match TMDL defaults
        # (many-to-one, single-direction filtering); validate them only
        # when explicitly declared.
        from_cardinality = re.search(
            r"^\tfromCardinality: (.+)$", block, re.MULTILINE
        )
        to_cardinality = re.search(r"^\ttoCardinality: (.+)$", block, re.MULTILINE)
        cross_filter = re.search(
            r"^\tcrossFilteringBehavior: (.+)$", block, re.MULTILINE
        )
        security_filter = re.search(
            r"^\tsecurityFilteringBehavior: (.+)$", block, re.MULTILINE
        )
        assert from_cardinality is None or from_cardinality.group(1) == "many", (
            f"{relationship_name} must use fromCardinality: many"
        )
        assert to_cardinality is None or to_cardinality.group(1) == "one", (
            f"{relationship_name} must use toCardinality: one"
        )
        assert cross_filter is None or cross_filter.group(1) in {
            "oneDirection",
            "bothDirections",
        }, f"{relationship_name} has invalid crossFilteringBehavior"
        assert security_filter is None or security_filter.group(1) in {
            "oneDirection",
            "bothDirections",
        }, f"{relationship_name} has invalid securityFilteringBehavior"

        to_match = re.search(r"^\ttoColumn: (.+)$", block, re.MULTILINE)
        assert to_match, f"{relationship_name} is missing toColumn"
        to_table, to_column = _parse_column_reference(to_match.group(1))
        to_column_body = column_metadata[to_table][to_column]
        if not is_inactive:
            assert re.search(r"^\t\tisKey$", to_column_body, re.MULTILINE), (
                f"{relationship_name} to-side column {to_table}[{to_column}] "
                "must be marked isKey"
            )
        assert re.search(
            r"^\t\tsummarizeBy: none$", to_column_body, re.MULTILINE
        ), (
            f"{relationship_name} to-side column {to_table}[{to_column}] "
            "must use summarizeBy: none"
        )

    for line_number, line in enumerate(relationships.splitlines(), start=1):
        if "\tfromColumn:" not in line and "\ttoColumn:" not in line:
            continue
        table, column = _parse_column_reference(line.split(":", 1)[1])
        assert table in tables, f"relationships.tmdl:{line_number} missing {table}"
        assert column in tables[table]["columns"], (
            f"relationships.tmdl:{line_number} missing {table}[{column}]"
        )

    for table_path in sorted((SEMANTIC_DEFINITION / "tables").glob("*.tmdl")):
        content = table_path.read_text(encoding="utf-8")
        assert "RELATED(" not in content and "RELATEDTABLE(" not in content, (
            f"{table_path.relative_to(REPO_ROOT)} uses relationship-dependent DAX "
            "row navigation; prefer explicit LOOKUPVALUE/TREATAS patterns in this "
            "Direct Lake model."
        )
        table_name = _strip_identifier(
            re.search(r"^table (.+)$", content, re.MULTILINE).group(1)  # type: ignore[union-attr]
        )
        for line_number, line in enumerate(content.splitlines(), start=1):
            if not re.match(r"^\t\t\tcolumn: ", line):
                continue
            column = _strip_identifier(line.split(":", 1)[1])
            assert column in tables[table_name]["columns"], (
                f"{table_path.relative_to(REPO_ROOT)}:{line_number} hierarchy "
                f"references missing {table_name}[{column}]"
            )

    measure_base_tables = _load_measure_base_tables()
    query_refs: set[str] = set()
    for report_path in sorted(REPORT_DEFINITION.rglob("*.json")):
        data = json.loads(report_path.read_text(encoding="utf-8"))
        visual_fields: list[tuple[str, str, str]] = []
        _walk_report_fields(data, tables, report_path, query_refs, visual_fields)
        _assert_visual_relationship_paths(
            visual_fields, active_filter_graph, measure_base_tables, report_path
        )

    for query_ref in query_refs:
        inner_ref = re.sub(
            r"^(?:Sum|Average|Min|Max|Count|CountNonNull)\((.*)\)$",
            r"\1",
            query_ref,
        )
        if "." not in inner_ref:
            continue
        table, field = map(_strip_identifier, inner_ref.rsplit(".", 1))
        if table not in tables and "." in table:
            # Hierarchy level refs are table.hierarchy.level
            head, hierarchy = map(_strip_identifier, table.split(".", 1))
            if head in tables and f"{hierarchy}.{field}" in tables[head]["hierarchies"]:
                continue
        assert table in tables, f"Report queryRef references missing table {table}"
        assert (
            field in tables[table]["columns"] or field in tables[table]["measures"]
        ), f"Report queryRef references missing field {table}[{field}]"

    pages_metadata = json.loads(
        (REPORT_DEFINITION / "pages" / "pages.json").read_text(encoding="utf-8")
    )
    for page_name in pages_metadata["pageOrder"]:
        assert (REPORT_DEFINITION / "pages" / page_name / "page.json").exists(), (
            f"pages.json references missing report page {page_name}"
        )

    diagram = json.loads(
        (
            REPO_ROOT
            / "fabric"
            / "powerbi"
            / "retail_model.SemanticModel"
            / "diagramLayout.json"
        ).read_text(encoding="utf-8")
    )
    diagram_nodes: set[str] = set()

    def collect_nodes(value: object) -> None:
        if isinstance(value, dict):
            node = value.get("nodeIndex")
            if isinstance(node, str):
                diagram_nodes.add(node)
            for child in value.values():
                collect_nodes(child)
        elif isinstance(value, list):
            for child in value:
                collect_nodes(child)

    collect_nodes(diagram)
    assert not (diagram_nodes - set(tables)), (
        f"diagramLayout references missing tables: {sorted(diagram_nodes - set(tables))}"
    )


def _module_path_exists(module: str) -> bool:
    parts = module.split(".")
    if not parts or parts[0] != "retail_datagen":
        return True

    base = DATAGEN_SRC.joinpath(*parts)
    return base.with_suffix(".py").exists() or base.is_dir()


def _resolve_import_from(path: Path, node: ast.ImportFrom) -> str:
    if node.level == 0:
        return node.module or ""

    relative = path.relative_to(DATAGEN_SRC).with_suffix("")
    package_parts = list(relative.parts[:-1])
    base_parts = package_parts[: len(package_parts) - node.level + 1]
    if node.module:
        base_parts.extend(node.module.split("."))
    return ".".join(base_parts)


def test_project_local_imports_reference_existing_modules() -> None:
    """Validate retail_datagen imports point at modules that exist in this repo."""
    missing: list[str] = []

    for path in sorted(DATAGEN_SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("retail_datagen") and not _module_path_exists(
                        alias.name
                    ):
                        missing.append(
                            f"{path.relative_to(REPO_ROOT)}:{node.lineno} {alias.name}"
                        )
            elif isinstance(node, ast.ImportFrom):
                module = _resolve_import_from(path, node)
                if module.startswith("retail_datagen") and not _module_path_exists(
                    module
                ):
                    missing.append(
                        f"{path.relative_to(REPO_ROOT)}:{node.lineno} {module}"
                    )

    assert not missing, "Missing project-local module references:\n" + "\n".join(missing)


def test_report_canvas_has_no_known_stale_binding_strings() -> None:
    """Prevent recurrence of known Desktop Missing References patterns."""
    stale_patterns = (
        "fact_Receipts",
        "fact_rorders",
        "Missing_References",
        "Missing references",
        "Missing References",
        "PBI_RemovedChildren",
    )
    stale_hits: list[str] = []

    semantic_root = REPO_ROOT / "fabric" / "powerbi"
    for path in sorted(semantic_root.rglob("*")):
        if path.suffix.lower() not in {".json", ".tmdl", ".pbir", ".pbip", ".platform"}:
            continue
        content = path.read_text(encoding="utf-8")
        for pattern in stale_patterns:
            if pattern in content:
                stale_hits.append(f"{path.relative_to(REPO_ROOT)} contains {pattern}")

    assert not stale_hits, "Known stale binding strings found:\n" + "\n".join(
        stale_hits
    )


# Published PBIR schema versions per definition part, from
# https://github.com/microsoft/json-schemas/tree/main/fabric/item/report/definition.
# Referencing an unpublished version (e.g. page/2.3.0) breaks Power BI Desktop.
_PUBLISHED_SCHEMA_VERSIONS = {
    "page": {"1.0.0", "1.1.0", "1.2.0", "1.3.0", "1.4.0", "2.0.0", "2.1.0"},
    "pagesMetadata": {"1.0.0", "1.1.0"},
    "report": {
        "1.0.0", "1.1.0", "1.2.0", "1.3.0",
        "2.0.0", "2.1.0", "3.0.0", "3.1.0", "3.2.0", "3.3.0",
    },
    "versionMetadata": {"1.0.0"},
    "bookmark": {"1.0.0", "1.1.0", "1.2.0", "1.3.0", "1.4.0", "2.0.0", "2.1.0"},
    "bookmarksMetadata": {"1.0.0"},
    "reportExtension": {"1.0.0"},
    "filterConfiguration": {"1.0.0", "1.1.0", "1.2.0", "1.3.0"},
    "visualContainer": {
        "1.0.0", "1.1.0", "1.2.0", "1.3.0", "1.4.0", "1.5.0", "1.6.0", "1.7.0",
        "1.8.0", "2.0.0", "2.1.0", "2.2.0", "2.3.0", "2.4.0", "2.5.0", "2.6.0",
        "2.7.0", "2.8.0", "2.9.0",
        # Written by current Power BI Desktop; the public json-schemas repo
        # lags behind Desktop releases.
        "2.10.0",
    },
}


def test_report_definition_uses_published_schema_versions() -> None:
    """Every report definition $schema must reference a published version."""
    invalid: list[str] = []

    for path in sorted(REPORT_DEFINITION.rglob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        schema_url = data.get("$schema", "")
        match = re.search(r"/report/definition/(\w+)/([\d.]+)/schema\.json$", schema_url)
        if not match:
            continue
        part, version = match.groups()
        published = _PUBLISHED_SCHEMA_VERSIONS.get(part)
        if published is not None and version not in published:
            invalid.append(
                f"{path.relative_to(REPO_ROOT)} references unpublished "
                f"{part} schema version {version}"
            )

    assert not invalid, "Unpublished schema versions found:\n" + "\n".join(invalid)
