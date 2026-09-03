"""Parsers for source selectors used by manifest validation."""

from __future__ import annotations

import ast
import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

KQL_TYPES = frozenset({"bool", "datetime", "dynamic", "int", "long", "real", "string"})
SPARK_TYPES = frozenset(
    {"boolean", "date", "double", "int", "long", "string", "timestamp"}
)
ML_SPARK_TYPES = SPARK_TYPES | frozenset({"array<long>", "array<string>"})
TMDL_TYPES = frozenset({"boolean", "dateTime", "decimal", "double", "int64", "string"})
_ML_GOLD_SCHEMA = "au"


@dataclass(frozen=True)
class PhysicalField:
    """One source-derived physical field."""

    name: str
    data_type: str
    nullable: bool | None
    source_name: str | None = None


@dataclass(frozen=True)
class MlValidationRule:
    """Runtime grain, temporal, lineage, probability, and horizon semantics."""

    grain: tuple[str, ...]
    as_of: str
    lineage: tuple[str, ...]
    probabilities: tuple[str, ...]
    horizon: str | int | None


@dataclass(frozen=True)
class DriverEventSchemas:
    """Envelope and event payload schemas parsed from the stream driver."""

    envelope: tuple[PhysicalField, ...]
    payloads: dict[str, tuple[PhysicalField, ...]]


@dataclass(frozen=True)
class DerivedSilverRoute:
    """A derived Silver route and its required Silver inputs."""

    name: str
    source_tables: tuple[str, ...]
    target_table: str


@dataclass(frozen=True)
class StreamingSilverContract:
    """Source-derived direct routes, derived routes, and dedupe keys."""

    routes: dict[str, str]
    dedupe_keys: dict[str, tuple[str, ...]]
    derived_routes: dict[str, DerivedSilverRoute]


@dataclass(frozen=True)
class GoldRoute:
    """One Gold output and its Silver inputs."""

    target_table: str
    source_tables: tuple[str, ...]


@dataclass(frozen=True)
class TmdlTableSchema:
    """One active TMDL table and its physical Direct Lake binding."""

    name: str
    source_schema: str
    source_table: str
    fields: tuple[PhysicalField, ...]


def notebook_ml_output_schemas(
    path: Path,
    *,
    require_runtime_validation: bool = True,
) -> dict[str, tuple[PhysicalField, ...]]:
    """Parse exact ML output declarations from a deployable notebook.

    Producer notebooks must call ``validate_ml_output`` once for every declared
    output. The helper checks the Spark frame immediately before the Delta write,
    making the notebook declaration executable rather than documentation only.
    """

    tree = ast.parse(notebook_python_source(path), filename=str(path))
    raw = _python_symbol_from_tree(tree, "ML_OUTPUT_CONTRACTS")
    if not isinstance(raw, dict) or not raw:
        raise TypeError("ML_OUTPUT_CONTRACTS must be a non-empty dictionary")

    schemas: dict[str, tuple[PhysicalField, ...]] = {}
    for table_name, records in raw.items():
        if not isinstance(table_name, str) or not table_name.isidentifier():
            raise ValueError("ML_OUTPUT_CONTRACTS keys must be identifiers")
        if not isinstance(records, (list, tuple)) or not records:
            raise TypeError(
                f"ML_OUTPUT_CONTRACTS[{table_name!r}] must be a sequence"
            )
        fields: list[PhysicalField] = []
        for record in records:
            if (
                not isinstance(record, (list, tuple))
                or len(record) != 3
                or not isinstance(record[0], str)
                or not record[0].isidentifier()
                or record[1] not in ML_SPARK_TYPES
                or not isinstance(record[2], bool)
            ):
                raise ValueError(
                    f"invalid ML output field in {table_name}: {record!r}"
                )
            fields.append(
                PhysicalField(
                    name=record[0],
                    data_type=record[1],
                    nullable=record[2],
                )
            )
        result = tuple(fields)
        _require_unique_fields(result, table_name, case_insensitive=True)
        schemas[table_name] = result

    if require_runtime_validation:
        helper = _function_node(tree, "validate_ml_output")
        helper_text = ast.unparse(helper)
        if (
            "schema.fields" not in helper_text
            or "ML_OUTPUT_CONTRACTS" not in helper_text
            or "isNull" not in helper_text
            or "nullable" not in helper_text
            or sum(isinstance(node, ast.Raise) for node in ast.walk(helper)) < 2
        ):
            raise ValueError(
                "validate_ml_output must reject schema drift and nulls in "
                "non-nullable fields"
            )
        _validate_ml_output_write_sites(tree, set(schemas))
    return schemas


def _validate_ml_output_write_sites(
    tree: ast.Module,
    declared_outputs: set[str],
) -> None:
    """Tie every declared schema to the frame and physical table actually written."""

    calls = [
        node
        for node in ast.walk(tree)
        if _named_call(node, "validate_ml_output")
    ]
    called_outputs = [
        _string_constant(call.args[1], "validate_ml_output table")
        for call in calls
        if len(call.args) >= 2
    ]
    if (
        set(called_outputs) != declared_outputs
        or len(called_outputs) != len(declared_outputs)
        or len(calls) != len(declared_outputs)
    ):
        raise ValueError("ML_OUTPUT_CONTRACTS and validate_ml_output calls differ")

    assignments = _top_level_assignments(tree)
    validated_call_ids: set[int] = set()

    def inspect_body(body: list[ast.stmt]) -> None:
        for index, statement in enumerate(body):
            validated = _validated_ml_assignment(statement)
            if validated is not None:
                call, frame_name, table_name = validated
                if index + 1 >= len(body):
                    raise ValueError(
                        f"validated ML output {table_name!r} is not immediately written"
                    )
                write_target = _ml_write_target(body[index + 1], frame_name)
                if write_target is None:
                    raise ValueError(
                        f"validated ML output {table_name!r} is not immediately written"
                    )
                target, uses_gold_helper = write_target
                physical_target = _static_string(target, assignments)
                if uses_gold_helper and "." not in physical_target:
                    try:
                        gold_schema = _static_string(
                            assignments["GOLD_DB"],
                            assignments,
                        )
                    except KeyError as exc:
                        raise ValueError(
                            "save_gold requires a statically resolvable GOLD_DB"
                        ) from exc
                    physical_target = f"{gold_schema}.{physical_target}"
                if physical_target.split(".")[-2:] != [
                    _ML_GOLD_SCHEMA,
                    table_name,
                ]:
                    raise ValueError(
                        f"validated ML output {table_name!r} writes "
                        f"{physical_target!r}"
                    )
                validated_call_ids.add(id(call))
            for nested_body in _nested_statement_bodies(statement):
                inspect_body(nested_body)

    inspect_body(tree.body)
    if validated_call_ids != {id(call) for call in calls}:
        raise ValueError(
            "validate_ml_output must assign the validated frame immediately "
            "before its Delta write"
        )


def _validated_ml_assignment(
    statement: ast.stmt,
) -> tuple[ast.Call, str, str] | None:
    if isinstance(statement, ast.Assign):
        if len(statement.targets) != 1 or not isinstance(statement.targets[0], ast.Name):
            return None
        target = statement.targets[0]
        value = statement.value
    elif isinstance(statement, ast.AnnAssign):
        if not isinstance(statement.target, ast.Name):
            return None
        target = statement.target
        value = statement.value
    else:
        return None
    if not _named_call(value, "validate_ml_output"):
        return None
    assert isinstance(value, ast.Call)
    if (
        len(value.args) < 2
        or not isinstance(value.args[0], ast.Name)
        or value.args[0].id != target.id
    ):
        return None
    return (
        value,
        target.id,
        _string_constant(value.args[1], "validate_ml_output table"),
    )


def _ml_write_target(
    statement: ast.stmt,
    frame_name: str,
) -> tuple[ast.AST, bool] | None:
    for node in ast.walk(statement):
        if (
            _named_call(node, "save_gold")
            and len(node.args) >= 2
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id == frame_name
        ):
            return node.args[1], True
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "saveAsTable"
            and node.args
            and _expression_root_name(node.func.value) == frame_name
        ):
            return node.args[0], False
    return None


def _expression_root_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return _expression_root_name(node.value)
    if isinstance(node, ast.Call):
        return _expression_root_name(node.func)
    return None


def _top_level_assignments(tree: ast.Module) -> dict[str, ast.AST]:
    assignments: dict[str, ast.AST] = {}
    for statement in tree.body:
        if isinstance(statement, ast.Assign) and len(statement.targets) == 1:
            target = statement.targets[0]
            if isinstance(target, ast.Name):
                assignments[target.id] = statement.value
        elif isinstance(statement, ast.AnnAssign) and isinstance(
            statement.target, ast.Name
        ):
            assignments[statement.target.id] = statement.value
    return assignments


def _static_string(
    node: ast.AST,
    assignments: dict[str, ast.AST],
    resolving: frozenset[str] = frozenset(),
) -> str:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        if node.id in resolving or node.id not in assignments:
            raise ValueError(f"ML write target {node.id!r} is not statically resolvable")
        return _static_string(
            assignments[node.id],
            assignments,
            resolving | {node.id},
        )
    if isinstance(node, ast.JoinedStr):
        return "".join(
            (
                _static_string(value.value, assignments, resolving)
                if isinstance(value, ast.FormattedValue)
                else _static_string(value, assignments, resolving)
            )
            for value in node.values
        )
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _static_string(
            node.left, assignments, resolving
        ) + _static_string(node.right, assignments, resolving)
    if _named_call(node, "get_env"):
        assert isinstance(node, ast.Call)
        default = next(
            (keyword.value for keyword in node.keywords if keyword.arg == "default"),
            None,
        )
        if default is not None:
            return _static_string(default, assignments, resolving)
    raise ValueError("ML write target must resolve to a static table name")


def _nested_statement_bodies(statement: ast.stmt) -> tuple[list[ast.stmt], ...]:
    bodies: list[list[ast.stmt]] = []
    for attribute in ("body", "orelse", "finalbody"):
        value = getattr(statement, attribute, None)
        if isinstance(value, list) and value and all(
            isinstance(child, ast.stmt) for child in value
        ):
            bodies.append(value)
    if isinstance(statement, ast.Try):
        bodies.extend(handler.body for handler in statement.handlers)
    if isinstance(statement, ast.Match):
        bodies.extend(case.body for case in statement.cases)
    return tuple(bodies)


def notebook_ml_source_tables(path: Path) -> tuple[str, ...]:
    """Parse the producer's declared physical input-table inventory."""

    tree = ast.parse(notebook_python_source(path), filename=str(path))
    return _string_sequence(
        _python_symbol_from_tree(tree, "ML_SOURCE_TABLES"),
        "ML_SOURCE_TABLES",
    )


def notebook_ml_validation_rules(path: Path) -> dict[str, MlValidationRule]:
    """Parse exact required-output semantics enforced by the runtime gate."""

    tree = ast.parse(notebook_python_source(path), filename=str(path))
    raw = _python_symbol_from_tree(tree, "REQUIRED_ML_RULES")
    if not isinstance(raw, dict) or not raw:
        raise TypeError("REQUIRED_ML_RULES must be a non-empty dictionary")

    expected_keys = {
        "grain",
        "as_of",
        "lineage",
        "probabilities",
        "horizon",
    }
    rules: dict[str, MlValidationRule] = {}
    for table_name, value in raw.items():
        if not isinstance(table_name, str) or not table_name.isidentifier():
            raise ValueError("REQUIRED_ML_RULES keys must be identifiers")
        if not isinstance(value, dict) or set(value) != expected_keys:
            raise ValueError(
                f"REQUIRED_ML_RULES[{table_name!r}] must define exactly "
                f"{sorted(expected_keys)}"
            )

        grain = _ml_rule_columns(value["grain"], table_name, "grain", require_items=True)
        lineage = _ml_rule_columns(
            value["lineage"],
            table_name,
            "lineage",
            require_items=True,
        )
        probabilities = _ml_rule_columns(
            value["probabilities"],
            table_name,
            "probabilities",
        )
        as_of = value["as_of"]
        if not isinstance(as_of, str) or not as_of.isidentifier():
            raise ValueError(
                f"REQUIRED_ML_RULES[{table_name!r}].as_of must be an identifier"
            )
        horizon = value["horizon"]
        if isinstance(horizon, bool) or (
            horizon is not None
            and not (
                isinstance(horizon, str)
                and horizon.isidentifier()
                or isinstance(horizon, int)
                and horizon > 0
            )
        ):
            raise ValueError(
                f"REQUIRED_ML_RULES[{table_name!r}].horizon must be a "
                "positive fixed day count, column identifier, or null"
            )
        rules[table_name] = MlValidationRule(
            grain=grain,
            as_of=as_of,
            lineage=lineage,
            probabilities=probabilities,
            horizon=horizon,
        )
    return rules


def _ml_rule_columns(
    value: Any,
    table_name: str,
    field_name: str,
    *,
    require_items: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise TypeError(
            f"REQUIRED_ML_RULES[{table_name!r}].{field_name} must be a sequence"
        )
    values = tuple(value)
    if require_items and not values:
        raise ValueError(
            f"REQUIRED_ML_RULES[{table_name!r}].{field_name} must not be empty"
        )
    if any(not isinstance(item, str) or not item.isidentifier() for item in values):
        raise ValueError(
            f"REQUIRED_ML_RULES[{table_name!r}].{field_name} must contain identifiers"
        )
    if len(values) != len(set(values)):
        raise ValueError(
            f"REQUIRED_ML_RULES[{table_name!r}].{field_name} must be unique"
        )
    return values


def python_symbol(path: Path, name: str) -> Any:
    """Return a literal top-level assignment or a matching definition node."""

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return _python_symbol_from_tree(tree, name)


def _python_symbol_from_tree(tree: ast.Module, name: str) -> Any:
    """Return a safe literal assignment or definition from an already parsed tree."""

    for node in tree.body:
        if isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
                return ast.literal_eval(node.value)
        if isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == name:
                return ast.literal_eval(node.value)
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name == name:
                return node
    raise KeyError(name)


def nested_value(document: Any, dotted_path: str) -> Any:
    """Resolve a dot-separated path through mapping values."""

    value = document
    for part in dotted_path.split("."):
        if not isinstance(value, dict) or part not in value:
            raise KeyError(dotted_path)
        value = value[part]
    return value


def glob_values(directory: Path, pattern: str) -> list[str]:
    """Return names matched by a non-empty directory glob."""

    if not directory.is_dir():
        raise TypeError(f"glob source is not a directory: {directory}")
    matches = sorted(directory.glob(pattern), key=lambda path: path.name)
    if not matches:
        raise ValueError(f"glob {pattern!r} matched no entries in {directory}")
    return [match.name for match in matches]


def flatten_sequences(values: Iterable[Any]) -> list[Any]:
    """Flatten dictionary values that are all list-like."""

    flattened: list[Any] = []
    for value in values:
        if not isinstance(value, (list, tuple)):
            raise TypeError("dictionary values are not sequences")
        flattened.extend(value)
    return flattened


def kql_table_schemas(path: Path) -> dict[str, dict[str, str]]:
    """Parse table and field/type mappings from KQL create-merge statements."""

    text = path.read_text(encoding="utf-8")
    pattern = re.compile(
        r"^\.create-merge\s+table\s+([A-Za-z_]\w*)\s*\((.*?)^\)",
        re.MULTILINE | re.DOTALL,
    )
    schemas: dict[str, dict[str, str]] = {}
    for table_name, body in pattern.findall(text):
        fields: dict[str, str] = {}
        for raw_line in body.splitlines():
            line = raw_line.split("//", 1)[0].strip().rstrip(",").strip()
            if not line:
                continue
            match = re.fullmatch(
                r"([A-Za-z_]\w*)\s*:\s*([A-Za-z_]\w*)",
                line,
            )
            if match is None:
                raise ValueError(f"unparsed KQL field in {table_name}: {line}")
            if match.group(2) not in KQL_TYPES:
                raise ValueError(
                    f"unsupported KQL type in {table_name}: {match.group(2)}"
                )
            _add_unique(fields, match.group(1), match.group(2), table_name)
        _add_unique(schemas, table_name, fields, "KQL tables")
    if not schemas:
        raise ValueError("no .create-merge table statements found")
    return schemas


def kql_mapping_schemas(path: Path) -> dict[str, dict[str, tuple[str, str]]]:
    """Parse table and column type/path mappings from KQL mapping scripts."""

    text = path.read_text(encoding="utf-8")
    pattern = re.compile(
        r"^\.create-or-alter\s+table\s+([A-Za-z_]\w*)\s+"
        r"ingestion\s+json\s+mapping\s+'[^']+'\s*\n```\s*\n"
        r"(\[.*?\])\s*\n```",
        re.MULTILINE | re.DOTALL,
    )
    schemas: dict[str, dict[str, tuple[str, str]]] = {}
    for table_name, mapping_json in pattern.findall(text):
        fields: dict[str, tuple[str, str]] = {}
        items = json.loads(mapping_json)
        if not isinstance(items, list):
            raise TypeError(f"KQL mapping for {table_name} is not an array")
        for item in items:
            if not isinstance(item, dict) or set(item) != {
                "column",
                "datatype",
                "path",
            }:
                raise ValueError(f"invalid KQL mapping item in {table_name}")
            column = item["column"]
            data_type = item["datatype"]
            json_path = item["path"]
            if (
                not isinstance(column, str)
                or not column.isidentifier()
                or data_type not in KQL_TYPES
                or not isinstance(json_path, str)
                or not json_path.startswith("$.")
            ):
                raise ValueError(f"invalid KQL mapping value in {table_name}")
            _add_unique(
                fields,
                column,
                (data_type, json_path),
                table_name,
            )
        _add_unique(schemas, table_name, fields, "KQL mappings")
    if not schemas:
        raise ValueError("no KQL ingestion mappings found")
    return schemas


def tmdl_tables(path: Path) -> list[str]:
    """Parse active table references from model.tmdl."""

    text = path.read_text(encoding="utf-8")
    matches = re.findall(r"^ref table\s+(?:'([^']+)'|(\S+))\s*$", text, re.MULTILINE)
    tables = [quoted or plain for quoted, plain in matches]
    if not tables:
        raise ValueError("no active TMDL table references found")
    return tables


def driver_event_schemas(path: Path) -> DriverEventSchemas:
    """Parse driver envelope/payload definitions with a strict tuple schema."""

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    envelope_value = _python_symbol_from_tree(tree, "ENVELOPE")
    payload_value = _python_symbol_from_tree(tree, "EVENT_PAYLOADS")
    _require_nullable_struct_fields(tree, "_from_json_schema")

    if not isinstance(envelope_value, (list, tuple)):
        raise TypeError("ENVELOPE must be a sequence")
    envelope = tuple(
        _physical_field(record, "ENVELOPE", with_source_name=False)
        for record in envelope_value
    )
    _require_unique_fields(envelope, "ENVELOPE")

    if not isinstance(payload_value, dict) or not payload_value:
        raise TypeError("EVENT_PAYLOADS must be a non-empty dictionary")
    payloads: dict[str, tuple[PhysicalField, ...]] = {}
    for event_type, records in payload_value.items():
        if not isinstance(event_type, str) or not event_type.isidentifier():
            raise ValueError("EVENT_PAYLOADS keys must be identifiers")
        if not isinstance(records, (list, tuple)) or not records:
            raise TypeError(f"EVENT_PAYLOADS[{event_type!r}] must be a sequence")
        fields = tuple(
            _physical_field(record, event_type, with_source_name=True)
            for record in records
        )
        _require_unique_fields(fields, event_type)
        source_names = [field.source_name for field in fields]
        if len(set(source_names)) != len(source_names):
            raise ValueError(f"duplicate JSON payload field in {event_type}")
        payloads[event_type] = fields
    return DriverEventSchemas(envelope=envelope, payloads=payloads)


def python_table_schemas(
    path: Path,
    symbol: str = "TABLES",
) -> dict[str, tuple[PhysicalField, ...]]:
    """Parse ``schemas.py`` table fields without importing or executing it."""

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    value = _python_symbol_from_tree(tree, symbol)
    _require_nullable_struct_fields(tree, "spark_schema", keyword=True)
    if not isinstance(value, dict) or not value:
        raise TypeError(f"{symbol} must be a non-empty dictionary")

    schemas: dict[str, tuple[PhysicalField, ...]] = {}
    for table_name, records in value.items():
        if not isinstance(table_name, str) or not table_name.isidentifier():
            raise ValueError(f"{symbol} keys must be identifiers")
        if not isinstance(records, (list, tuple)) or not records:
            raise TypeError(f"{symbol}[{table_name!r}] must be a sequence")
        fields: list[PhysicalField] = []
        for record in records:
            if (
                not isinstance(record, (list, tuple))
                or len(record) != 2
                or not isinstance(record[0], str)
                or not record[0].isidentifier()
                or record[1] not in SPARK_TYPES
            ):
                raise ValueError(f"invalid schema field in {table_name}: {record!r}")
            fields.append(
                PhysicalField(
                    name=record[0],
                    data_type=record[1],
                    nullable=True,
                )
            )
        result = tuple(fields)
        _require_unique_fields(result, table_name, case_insensitive=True)
        schemas[table_name] = result
    return schemas


def notebook_python_source(path: Path) -> str:
    """Return code-cell source from a structurally valid Jupyter notebook."""

    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or not isinstance(document.get("cells"), list):
        raise TypeError(f"invalid notebook structure: {path}")
    code: list[str] = []
    for index, cell in enumerate(document["cells"]):
        if not isinstance(cell, dict):
            raise TypeError(f"notebook cell {index} is not an object")
        cell_type = cell.get("cell_type")
        source = cell.get("source")
        if cell_type not in {"code", "markdown"}:
            raise ValueError(f"unsupported notebook cell type: {cell_type!r}")
        if isinstance(source, str):
            text = source
        elif isinstance(source, list) and all(isinstance(line, str) for line in source):
            text = "".join(source)
        else:
            raise TypeError(f"notebook cell {index} has invalid source")
        if cell_type == "code":
            code.append(text)
    if not code:
        raise ValueError(f"notebook has no code cells: {path}")
    return "\n".join(code)


def streaming_silver_contract(path: Path) -> StreamingSilverContract:
    """Parse declared streaming transformations and dedupe keys from the notebook."""

    tree = ast.parse(notebook_python_source(path), filename=str(path))
    routes: dict[str, str] = {}
    for node in ast.walk(tree):
        if not _named_call(node, "process_events"):
            continue
        if len(node.args) < 2:
            raise ValueError("process_events call is missing route arguments")
        event_type = _string_constant(node.args[0], "process_events source")
        target_table = _string_constant(node.args[1], "process_events target")
        _add_unique(routes, event_type, target_table, "streaming routes")

    raw_dedupe = _python_symbol_from_tree(tree, "STREAM_DEDUPE_KEYS")
    dedupe_keys = _string_sequence_mapping(raw_dedupe, "STREAM_DEDUPE_KEYS")

    lifecycle = _function_node(tree, "process_truck_lifecycles")
    lifecycle_sources = {
        _string_constant(call.args[0], "truck lifecycle source")
        for call in ast.walk(lifecycle)
        if _named_call(call, "streaming_table_exists") and call.args
    }
    lifecycle_target = _single_qualified_table(lifecycle, "fact_", "truck lifecycle")
    lifecycle_keys = _string_sequence(
        _python_symbol_from_tree(tree, "TRUCK_LIFECYCLE_KEYS"),
        "TRUCK_LIFECYCLE_KEYS",
    )
    for event_type in sorted(lifecycle_sources):
        _add_unique(routes, event_type, lifecycle_target, "streaming routes")
        _add_unique(dedupe_keys, event_type, lifecycle_keys, "streaming dedupe keys")

    derived_function = _function_node(tree, "process_marketing_attribution")
    required = _local_literal(derived_function, "required")
    source_tables = _string_sequence(required, "marketing attribution inputs")
    merge_calls = [
        call for call in ast.walk(derived_function) if _named_call(call, "merge_new_rows")
    ]
    if len(merge_calls) != 1 or len(merge_calls[0].args) < 3:
        raise ValueError("marketing attribution must have one merge_new_rows target")
    route_name = _string_constant(
        merge_calls[0].args[1],
        "marketing attribution route name",
    )
    target_table = _string_constant(
        merge_calls[0].args[2],
        "marketing attribution target",
    )
    derived = DerivedSilverRoute(
        name=route_name,
        source_tables=source_tables,
        target_table=target_table,
    )
    if route_name not in dedupe_keys:
        raise ValueError("derived route has no STREAM_DEDUPE_KEYS entry")
    return StreamingSilverContract(
        routes=routes,
        dedupe_keys=dedupe_keys,
        derived_routes={route_name: derived},
    )


def gold_output_contract(path: Path) -> dict[str, GoldRoute]:
    """Parse Gold outputs and their Silver inputs from known notebook calls."""

    tree = ast.parse(notebook_python_source(path), filename=str(path))
    expected = _string_sequence(
        _python_symbol_from_tree(tree, "EXPECTED_GOLD_TABLES"),
        "EXPECTED_GOLD_TABLES",
    )
    routes: dict[str, GoldRoute] = {}
    for statement in tree.body:
        save_calls = [call for call in ast.walk(statement) if _named_call(call, "save_gold")]
        if not save_calls:
            continue
        if len(save_calls) != 1 or len(save_calls[0].args) < 2:
            raise ValueError("one Gold block must write exactly one named target")
        target = _string_constant(save_calls[0].args[1], "Gold target")
        source_tables = tuple(
            dict.fromkeys(
                _string_constant(call.args[0], "Gold Silver source")
                for call in ast.walk(statement)
                if _named_call(call, "read_silver") and call.args
            )
        )
        if not source_tables:
            raise ValueError(f"Gold target {target!r} has no read_silver source")
        _add_unique(
            routes,
            target,
            GoldRoute(target_table=target, source_tables=source_tables),
            "Gold routes",
        )
    if set(routes) != set(expected):
        raise ValueError("EXPECTED_GOLD_TABLES and save_gold outputs differ")
    return {name: routes[name] for name in expected}


def tmdl_active_table_schemas(path: Path) -> dict[str, TmdlTableSchema]:
    """Parse active model tables, physical bindings, columns, and TMDL types."""

    tables: dict[str, TmdlTableSchema] = {}
    for table_name in tmdl_tables(path):
        table_path = path.parent / "tables" / f"{table_name}.tmdl"
        if not table_path.is_file():
            raise FileNotFoundError(f"active TMDL table source is missing: {table_path}")
        schema = _tmdl_table_schema(table_path)
        if schema.name != table_name:
            raise ValueError(
                f"TMDL ref/file table mismatch: {table_name!r} != {schema.name!r}"
            )
        _add_unique(tables, table_name, schema, "active TMDL tables")
    return tables


def _physical_field(
    record: object,
    context: str,
    *,
    with_source_name: bool,
) -> PhysicalField:
    expected_size = 3 if with_source_name else 2
    if not isinstance(record, (list, tuple)) or len(record) != expected_size:
        raise ValueError(f"invalid physical field in {context}: {record!r}")
    name = record[0]
    source_name = record[1] if with_source_name else None
    data_type = record[2] if with_source_name else record[1]
    if (
        not isinstance(name, str)
        or not name.isidentifier()
        or (
            with_source_name
            and (
                not isinstance(source_name, str)
                or not source_name.isidentifier()
            )
        )
        or data_type not in KQL_TYPES
    ):
        raise ValueError(f"invalid physical field in {context}: {record!r}")
    return PhysicalField(
        name=name,
        data_type=data_type,
        nullable=True,
        source_name=source_name,
    )


def _require_nullable_struct_fields(
    tree: ast.Module,
    function_name: str,
    *,
    keyword: bool = False,
) -> None:
    function = _function_node(tree, function_name)
    calls = [call for call in ast.walk(function) if _named_call(call, "StructField")]
    if not calls:
        raise ValueError(f"{function_name} does not define StructField nullability")
    for call in calls:
        if keyword:
            values = [
                item.value
                for item in call.keywords
                if item.arg == "nullable"
            ]
        else:
            values = [call.args[2]] if len(call.args) > 2 else []
        if len(values) != 1 or not (
            isinstance(values[0], ast.Constant) and values[0].value is True
        ):
            raise ValueError(f"{function_name} must explicitly use nullable=True")


def _require_unique_fields(
    fields: tuple[PhysicalField, ...],
    context: str,
    *,
    case_insensitive: bool = False,
) -> None:
    names = [
        field.name.casefold() if case_insensitive else field.name
        for field in fields
    ]
    if len(set(names)) != len(names):
        raise ValueError(f"duplicate physical field in {context}")


def _function_node(tree: ast.Module, name: str) -> ast.FunctionDef:
    matches = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    ]
    if len(matches) != 1:
        raise KeyError(name)
    return matches[0]


def _named_call(node: ast.AST, name: str) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == name
    )


def _string_constant(node: ast.AST, context: str) -> str:
    if (
        not isinstance(node, ast.Constant)
        or not isinstance(node.value, str)
        or not node.value.isidentifier()
    ):
        raise ValueError(f"{context} must be a literal identifier")
    return node.value


def _string_sequence(value: object, context: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value:
        raise TypeError(f"{context} must be a non-empty sequence")
    if not all(isinstance(item, str) and item.isidentifier() for item in value):
        raise ValueError(f"{context} must contain literal identifiers")
    if len(set(value)) != len(value):
        raise ValueError(f"{context} contains duplicates")
    return tuple(value)


def _string_sequence_mapping(
    value: object,
    context: str,
) -> dict[str, tuple[str, ...]]:
    if not isinstance(value, dict) or not value:
        raise TypeError(f"{context} must be a non-empty dictionary")
    result: dict[str, tuple[str, ...]] = {}
    for key, items in value.items():
        if not isinstance(key, str) or not key.isidentifier():
            raise ValueError(f"{context} keys must be identifiers")
        result[key] = _string_sequence(items, f"{context}[{key!r}]")
    return result


def _local_literal(function: ast.FunctionDef, name: str) -> object:
    matches: list[ast.AST] = []
    for node in function.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        ):
            matches.append(node.value)
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == name
        ):
            matches.append(node.value)
    if len(matches) != 1:
        raise KeyError(name)
    return ast.literal_eval(matches[0])


def _single_qualified_table(
    node: ast.AST,
    prefix: str,
    context: str,
) -> str:
    matches: set[str] = set()
    for value in ast.walk(node):
        if not isinstance(value, ast.JoinedStr):
            continue
        literal = "".join(
            part.value
            for part in value.values
            if isinstance(part, ast.Constant) and isinstance(part.value, str)
        )
        match = re.search(rf"\.({re.escape(prefix)}[A-Za-z0-9_]*)$", literal)
        if match:
            matches.add(match.group(1))
    if len(matches) != 1:
        raise ValueError(f"{context} must resolve one qualified table, got {matches}")
    return matches.pop()


def _tmdl_table_schema(path: Path) -> TmdlTableSchema:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    if not lines:
        raise ValueError(f"empty TMDL table: {path}")
    table_match = re.fullmatch(r"table\s+(.+)", lines[0])
    if table_match is None:
        raise ValueError(f"missing TMDL table declaration: {path}")
    name = _unquote_tmdl_identifier(table_match.group(1))
    lineage = re.search(
        r"^\tsourceLineageTag:\s+\[([^\]]+)\]\.\[([^\]]+)\]\s*$",
        text,
        re.MULTILINE,
    )
    if lineage is None:
        raise ValueError(f"missing physical TMDL lineage: {path}")
    source_schema, source_table = _tmdl_direct_lake_binding(text, path)
    lineage_binding = (
        _unquote_tmdl_identifier(lineage.group(1)),
        _unquote_tmdl_identifier(lineage.group(2)),
    )
    if (source_schema, source_table) != lineage_binding:
        raise ValueError(
            f"Direct Lake binding disagrees with sourceLineageTag: {path}"
        )

    fields: list[PhysicalField] = []
    index = 0
    while index < len(lines):
        match = re.fullmatch(r"\tcolumn\s+(.+)", lines[index])
        if match is None:
            index += 1
            continue
        display_name = _unquote_tmdl_identifier(match.group(1))
        block: list[str] = []
        index += 1
        while index < len(lines) and (
            not lines[index].startswith("\t")
            or lines[index].startswith("\t\t")
            or not lines[index].strip()
        ):
            block.append(lines[index])
            index += 1
        block_text = "\n".join(block)
        data_type_match = re.search(r"^\t\tdataType:\s+(\w+)\s*$", block_text, re.MULTILINE)
        if data_type_match is None:
            continue
        data_type = data_type_match.group(1)
        if data_type not in TMDL_TYPES:
            raise ValueError(f"unsupported TMDL type {data_type!r} in {path}")
        source_match = re.search(
            r"^\t\tsourceColumn:\s+(.+?)\s*$",
            block_text,
            re.MULTILINE,
        )
        if source_match is None:
            if re.search(r"^\t\texpression\s*=", block_text, re.MULTILINE):
                continue
            source_name = display_name
        else:
            source_name = _unquote_tmdl_identifier(source_match.group(1))
        if not source_name:
            raise ValueError(f"empty TMDL source column in {path}")
        fields.append(
            PhysicalField(
                name=source_name,
                data_type=data_type,
                nullable=None,
            )
        )
    result = tuple(fields)
    if not result:
        raise ValueError(f"no physical TMDL fields found: {path}")
    _require_unique_fields(result, name, case_insensitive=True)
    return TmdlTableSchema(
        name=name,
        source_schema=source_schema,
        source_table=source_table,
        fields=result,
    )


def _tmdl_direct_lake_binding(text: str, path: Path) -> tuple[str, str]:
    partitions = re.findall(
        r"^\tpartition\s+.+?\s*=\s*entity\s*$\n"
        r"(.*?)(?=^\tpartition\s+|\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    if len(partitions) != 1:
        raise ValueError(
            f"TMDL table must have one executable entity partition: {path}"
        )
    partition = partitions[0]
    modes = re.findall(
        r"^\t\tmode:\s+(\w+)\s*$",
        partition,
        re.MULTILINE,
    )
    if modes != ["directLake"]:
        raise ValueError(f"TMDL entity partition must use Direct Lake: {path}")
    if re.search(r"^\t\tsource\s*$", partition, re.MULTILINE) is None:
        raise ValueError(f"TMDL entity partition has no source block: {path}")

    values: list[str] = []
    for property_name in ("schemaName", "entityName"):
        matches = re.findall(
            rf"^\t\t\t{property_name}:\s+(.+?)\s*$",
            partition,
            re.MULTILINE,
        )
        if len(matches) != 1:
            raise ValueError(
                f"TMDL entity partition requires one {property_name}: {path}"
            )
        value = _unquote_tmdl_identifier(matches[0])
        if not value:
            raise ValueError(
                f"TMDL entity partition has empty {property_name}: {path}"
            )
        values.append(value)
    return values[0], values[1]


def _unquote_tmdl_identifier(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] == "'":
        return value[1:-1].replace("''", "'")
    return value


def _add_unique(mapping: dict, key: str, value: Any, context: str) -> None:
    if key in mapping:
        raise ValueError(f"duplicate {key!r} in {context}")
    mapping[key] = value
