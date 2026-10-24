"""Render the retail ontology as a new-experience (TMDL) Fabric definition.

Generate the definition from the authoritative physical contracts:

* Silver tables: ``retail_setup.generation.schemas.TABLES``;
* Gold ML outputs: ``ml_contracts`` in ``contracts/retail-demo.json``;
* Bronze event tables: ``fabric/kql_database/01-create-tables.kql``, exposed to
  the Lakehouse SQL endpoint through the ``cusn`` Eventhouse shortcuts.

This module makes no service calls. Callers supply the Lakehouse SQL endpoint,
its database ID, and the available Bronze tables. Format reference:
https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/ontology-definition
"""

from __future__ import annotations

import base64
import json
import re
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

from deploy.scripts._ontology_rules import RULES, render_rule

ROOT = Path(__file__).resolve().parents[2]
ONTOLOGY_NAME = "RetailOntology_AutoGen"
DESCRIPTION = (
    "Retail domain ontology generated from Lakehouse Silver/Gold tables and "
    "Eventhouse event tables exposed through Bronze shortcuts."
)
EXPRESSION_NAME = "DatabaseQuery"
COMPATIBILITY_LEVEL = 1000000
_LINEAGE_NAMESPACE = uuid.UUID("5b1f3c0e-9d0a-4a7e-8f61-2f6e0c1d7a42")
_EXCLUDED_COLUMNS = frozenset({"__index_level_0__"})
_TIMESTAMP_COLUMN = "ingest_timestamp"

_SPARK_TYPES = {
    "long": "int64",
    "int": "int64",
    "string": "string",
    "double": "double",
    "boolean": "boolean",
    "timestamp": "dateTime",
    "date": "dateTime",
}
_KQL_TYPES = {
    "long": "int64",
    "int": "int64",
    "real": "double",
    "double": "double",
    "decimal": "decimal",
    "string": "string",
    "bool": "boolean",
    "boolean": "boolean",
    "datetime": "dateTime",
}
_ABBREVIATIONS = {
    "dc": "DC", "gps": "GPS", "id": "ID", "ip": "IP", "sku": "SKU", "utc": "UTC",
}


class OntologyModelError(ValueError):
    """The ontology model disagrees with the authoritative source schemas."""


@dataclass(frozen=True)
class Entity:
    name: str
    table: str
    layer: str  # "silver" or "gold"
    key: str


@dataclass(frozen=True)
class Relationship:
    name: str
    source: str
    target: str
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    active: bool = True


@dataclass(frozen=True)
class JunctionRelationship:
    name: str
    source: str
    target: str
    table: str
    layer: str
    source_column: str
    target_column: str


@dataclass(frozen=True)
class EventBinding:
    entity: str
    table: str
    key: str


ENTITIES: tuple[Entity, ...] = (
    Entity("Geography", "dim_geographies", "silver", "ID"),
    Entity("Store", "dim_stores", "silver", "ID"),
    Entity("DistributionCenter", "dim_distribution_centers", "silver", "ID"),
    Entity("Truck", "dim_trucks", "silver", "ID"),
    Entity("Customer", "dim_customers", "silver", "ID"),
    Entity("Product", "dim_products", "silver", "ID"),
    Entity("Receipt", "fact_receipts", "silver", "receipt_id_ext"),
    Entity("OnlineOrder", "fact_online_order_headers", "silver", "order_id_ext"),
    Entity("Promotion", "fact_promotions", "silver", "receipt_id_ext"),
    Entity("Payment", "fact_payments", "silver", "transaction_id"),
    Entity("CustomerSegment", "customer_segments", "gold", "customer_id"),
    Entity("ChurnPrediction", "churn_predictions", "gold", "customer_id"),
    Entity("StockoutRisk", "stockout_risk", "gold", "ranking"),
)

# Inactive flags mirror retail_model relationships.tmdl so the TOM graph has
# no ambiguous active paths.
RELATIONSHIPS: tuple[Relationship, ...] = (
    Relationship("StoreLocatedInGeography", "Store", "Geography",
                 "dim_stores", "GeographyID", "dim_geographies", "ID"),
    Relationship("DistributionCenterLocatedInGeography", "DistributionCenter", "Geography",
                 "dim_distribution_centers", "GeographyID", "dim_geographies", "ID"),
    Relationship("CustomerLocatedInGeography", "Customer", "Geography",
                 "dim_customers", "GeographyID", "dim_geographies", "ID", active=False),
    Relationship("TruckAssignedToDistributionCenter", "Truck", "DistributionCenter",
                 "dim_trucks", "DCID", "dim_distribution_centers", "ID"),
    Relationship("ReceiptPlacedByCustomer", "Receipt", "Customer",
                 "fact_receipts", "customer_id", "dim_customers", "ID"),
    Relationship("ReceiptAtStore", "Receipt", "Store",
                 "fact_receipts", "store_id", "dim_stores", "ID"),
    Relationship("OnlineOrderPlacedByCustomer", "OnlineOrder", "Customer",
                 "fact_online_order_headers", "customer_id", "dim_customers", "ID"),
    Relationship("PromotionAppliedToReceipt", "Promotion", "Receipt",
                 "fact_promotions", "receipt_id_ext", "fact_receipts", "receipt_id_ext"),
    Relationship("PromotionAtStore", "Promotion", "Store",
                 "fact_promotions", "store_id", "dim_stores", "ID", active=False),
    Relationship("PromotionByCustomer", "Promotion", "Customer",
                 "fact_promotions", "customer_id", "dim_customers", "ID", active=False),
    Relationship("PaymentForReceipt", "Payment", "Receipt",
                 "fact_payments", "receipt_id_ext", "fact_receipts", "receipt_id_ext",
                 active=False),
    Relationship("PaymentForOnlineOrder", "Payment", "OnlineOrder",
                 "fact_payments", "order_id_ext", "fact_online_order_headers",
                 "order_id_ext", active=False),
    Relationship("CustomerHasSegment", "CustomerSegment", "Customer",
                 "customer_segments", "customer_id", "dim_customers", "ID"),
    Relationship("CustomerHasChurnPrediction", "ChurnPrediction", "Customer",
                 "churn_predictions", "customer_id", "dim_customers", "ID"),
    Relationship("StockoutRiskAtStore", "StockoutRisk", "Store",
                 "stockout_risk", "store_id", "dim_stores", "ID"),
    Relationship("StockoutRiskForProduct", "StockoutRisk", "Product",
                 "stockout_risk", "product_id", "dim_products", "ID"),
)

JUNCTIONS: tuple[JunctionRelationship, ...] = (
    JunctionRelationship("ReceiptContainsProduct", "Receipt", "Product",
                         "fact_receipt_lines", "silver", "receipt_id_ext", "product_id"),
    JunctionRelationship("OnlineOrderContainsProduct", "OnlineOrder", "Product",
                         "fact_online_order_lines", "silver", "order_id", "product_id"),
)

# The first binding of each event table is its only active TOM relationship.
EVENT_BINDINGS: tuple[EventBinding, ...] = (
    EventBinding("Receipt", "receipt_created", "receipt_id"),
    EventBinding("Receipt", "receipt_line_added", "receipt_id"),
    EventBinding("Payment", "payment_processed", "transaction_id"),
    EventBinding("Promotion", "promotion_applied", "receipt_id"),
    EventBinding("OnlineOrder", "online_order_created", "order_id"),
    EventBinding("OnlineOrder", "online_order_picked", "order_id"),
    EventBinding("OnlineOrder", "online_order_shipped", "order_id"),
    EventBinding("Store", "receipt_created", "store_id"),
    EventBinding("Store", "payment_processed", "store_id"),
    EventBinding("Store", "promotion_applied", "store_id"),
    EventBinding("Store", "inventory_updated", "store_id"),
    EventBinding("Store", "stockout_detected", "store_id"),
    EventBinding("Store", "reorder_triggered", "store_id"),
    EventBinding("Store", "customer_entered", "store_id"),
    EventBinding("Store", "customer_zone_changed", "store_id"),
    EventBinding("Store", "ble_ping_detected", "store_id"),
    EventBinding("Store", "truck_arrived", "store_id"),
    EventBinding("Store", "truck_departed", "store_id"),
    EventBinding("Store", "store_opened", "store_id"),
    EventBinding("Store", "store_closed", "store_id"),
    EventBinding("Customer", "receipt_created", "customer_id"),
    EventBinding("Customer", "payment_processed", "customer_id"),
    EventBinding("Customer", "promotion_applied", "customer_id"),
    EventBinding("Customer", "online_order_created", "customer_id"),
    EventBinding("Product", "receipt_line_added", "product_id"),
    EventBinding("Product", "inventory_updated", "product_id"),
    EventBinding("Product", "stockout_detected", "product_id"),
    EventBinding("Product", "reorder_triggered", "product_id"),
    EventBinding("DistributionCenter", "inventory_updated", "dc_id"),
    EventBinding("DistributionCenter", "stockout_detected", "dc_id"),
    EventBinding("DistributionCenter", "reorder_triggered", "dc_id"),
    EventBinding("DistributionCenter", "truck_arrived", "dc_id"),
    EventBinding("DistributionCenter", "truck_departed", "dc_id"),
)


@dataclass(frozen=True)
class Schemas:
    silver: Mapping[str, tuple[tuple[str, str], ...]]
    gold: Mapping[str, tuple[tuple[str, str], ...]]
    bronze: Mapping[str, tuple[tuple[str, str], ...]]


def snake_case(name: str) -> str:
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"[^0-9a-zA-Z]+", "_", value).strip("_")
    return re.sub(r"_+", "_", value).lower()


def _label(column: str) -> str:
    return " ".join(_ABBREVIATIONS.get(word, word) for word in snake_case(column).split("_"))


def parse_kql_tables(text: str) -> dict[str, tuple[tuple[str, str], ...]]:
    """Return ``{table: ((column, kql_type), ...)}`` from create-merge statements."""
    tables: dict[str, tuple[tuple[str, str], ...]] = {}
    for match in re.finditer(r"\.create-merge table (\w+) \((.*?)\n\)", text, re.DOTALL):
        columns = []
        for raw in match.group(2).splitlines():
            line = raw.split("//", 1)[0].strip().rstrip(",")
            if not line:
                continue
            name, _, kql_type = (part.strip() for part in line.partition(":"))
            columns.append((name, kql_type.lower()))
        tables[match.group(1)] = tuple(columns)
    return tables


def load_schemas(repo_root: Path = ROOT) -> Schemas:
    from retail_setup.generation.schemas import TABLES

    manifest = json.loads(
        (repo_root / "contracts" / "retail-demo.json").read_text(encoding="utf-8")
    )
    gold = {
        contract["output"]["table"]: tuple(
            (field_["name"], field_["data_type"]) for field_ in contract["output"]["fields"]
        )
        for contract in manifest["ml_contracts"]
    }
    bronze = parse_kql_tables(
        (repo_root / "fabric" / "kql_database" / "01-create-tables.kql").read_text(
            encoding="utf-8"
        )
    )
    return Schemas(
        silver={name: tuple(columns) for name, columns in TABLES.items()},
        gold=gold,
        bronze=bronze,
    )


@dataclass
class _Table:
    name: str
    schema: str
    columns: list[tuple[str, str]]  # (column, tmdl data type)


@dataclass
class _Property:
    name: str
    data_type: str
    description: str
    column: str
    table: str
    ordering_column: str | None = None


@dataclass
class _EntityPlan:
    entity: Entity
    properties: list[_Property] = field(default_factory=list)
    additional: list[tuple[str, str]] = field(default_factory=list)  # (table, relationship)


def lineage_tag(*parts: str) -> str:
    return str(uuid.uuid5(_LINEAGE_NAMESPACE, "/".join(parts)))


def _quote(name: str) -> str:
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        return name
    return "'" + name.replace("'", "''") + "'"


def _doc(text: str, indent: str = "") -> str:
    clean = " ".join(text.split())
    if len(clean) > 4000:
        raise OntologyModelError("Ontology descriptions are limited to 4,000 characters.")
    return f"{indent}/// {clean}\n"


def _mapped(columns: Iterable[tuple[str, str]], types: Mapping[str, str]) -> list[tuple[str, str]]:
    return [
        (name, types[source_type])
        for name, source_type in columns
        if name not in _EXCLUDED_COLUMNS and source_type in types
    ]


def _require(columns: Iterable[tuple[str, str]], column: str, label: str) -> None:
    if column not in {name for name, _ in columns}:
        raise OntologyModelError(f"{label}: column {column!r} is not in the source schema.")


def _tom_name(from_table: str, from_column: str, to_table: str) -> str:
    return f"rel_{from_table}_{snake_case(from_column)}_{to_table}"


@dataclass(frozen=True)
class Layers:
    silver: str = "ag"
    gold: str = "au"
    bronze: str = "cusn"


@dataclass(frozen=True)
class SourceItem:
    """The Lakehouse item that backs every table partition.

    Fabric identifies a partition's data source from ``ONT_*`` annotations, not
    from the ``Sql.Database`` expression. Without them the ontology cannot
    resolve its underlying source, so a Data Agent using the ontology as
    context has nothing to query and graph materialization marks every entity
    ineligible.
    """

    workspace_id: str
    workspace_name: str
    item_id: str
    item_name: str
    pinned_at: str
    item_kind: str = "Lakehouse"

    def annotations(self, sql_endpoint: str) -> tuple[tuple[str, str], ...]:
        values = (
            ("ONT_WorkspaceId", str(uuid.UUID(self.workspace_id))),
            ("ONT_ItemId", str(uuid.UUID(self.item_id))),
            ("ONT_ItemKind", self.item_kind),
            ("ONT_ItemName", self.item_name),
            ("ONT_WorkspaceName", self.workspace_name),
            ("ONT_SqlEndpoint", sql_endpoint),
            ("ONT_SqlDatabase", self.item_name),
            ("ONT_PinnedAtUtc", self.pinned_at),
        )
        for name, value in values:
            if not value.strip() or any(ch in value for ch in "\r\n"):
                raise OntologyModelError(f"Partition annotation {name} needs a one-line value.")
        return values


def build_definition(
    *,
    sql_endpoint: str,
    sql_database_id: str,
    source: SourceItem,
    available_bronze_tables: Iterable[str],
    schemas: Schemas | None = None,
    display_name: str = ONTOLOGY_NAME,
    layers: Layers = Layers(),
    include_rules: bool = True,
) -> dict[str, str]:
    """Return ``{part path: text}`` for a complete new-experience definition."""
    if not sql_endpoint or any(ch in sql_endpoint for ch in '"\r\n'):
        raise OntologyModelError("A Lakehouse SQL endpoint host is required.")
    sql_database_id = str(uuid.UUID(sql_database_id))
    annotations = source.annotations(sql_endpoint)
    schemas = schemas or load_schemas()
    available = set(available_bronze_tables)
    layer_tables = {"silver": schemas.silver, "gold": schemas.gold}
    layer_schema = {"silver": layers.silver, "gold": layers.gold}

    tables: dict[str, _Table] = {}

    def add_table(name: str, layer: str, only: Iterable[str] | None = None) -> _Table:
        if name in tables:
            return tables[name]
        if layer == "bronze":
            source, types, schema = schemas.bronze, _KQL_TYPES, layers.bronze
        else:
            source, types, schema = layer_tables[layer], _SPARK_TYPES, layer_schema[layer]
        if name not in source:
            raise OntologyModelError(f"Table {name!r} is not in the {layer} contract.")
        columns = _mapped(source[name], types)
        if only is not None:
            wanted = list(only)
            for column in wanted:
                _require(columns, column, name)
            columns = [column for column in columns if column[0] in wanted]
        tables[name] = _Table(name, schema, columns)
        return tables[name]

    plans: dict[str, _EntityPlan] = {}
    for entity in ENTITIES:
        table = add_table(entity.table, entity.layer)
        _require(table.columns, entity.key, entity.name)
        if dict(table.columns)[entity.key] not in {"string", "int64"}:
            raise OntologyModelError(f"{entity.name} key must be string or int64.")
        plan = _EntityPlan(entity)
        used: set[str] = set()
        for column, data_type in table.columns:
            name = snake_case(column)
            if name in used:
                raise OntologyModelError(f"{entity.name}: duplicate property {name!r}.")
            used.add(name)
            plan.properties.append(_Property(
                name, data_type,
                f"Represents the {_label(column)} for the {entity.name} entity, "
                f"sourced from the {entity.table} Lakehouse table.",
                column, entity.table,
            ))
        plans[entity.name] = plan

    tom: list[tuple[str, str, str, bool]] = []  # (name, from, to, active)

    def key_column(entity_name: str) -> tuple[str, str]:
        entity = plans[entity_name].entity
        return entity.table, entity.key

    for rel in RELATIONSHIPS:
        _require(tables[rel.from_table].columns, rel.from_column, rel.name)
        _require(tables[rel.to_table].columns, rel.to_column, rel.name)
        tom.append((
            _tom_name(rel.from_table, rel.from_column, rel.to_table),
            f"{rel.from_table}.{rel.from_column}",
            f"{rel.to_table}.{rel.to_column}",
            rel.active,
        ))

    junction_tom: dict[str, tuple[str, str]] = {}
    for junction in JUNCTIONS:
        add_table(junction.table, junction.layer, (junction.source_column, junction.target_column))
        names = []
        for column, entity_name in (
            (junction.source_column, junction.source),
            (junction.target_column, junction.target),
        ):
            to_table, to_column = key_column(entity_name)
            name = _tom_name(junction.table, column, to_table)
            tom.append((name, f"{junction.table}.{column}", f"{to_table}.{to_column}", True))
            names.append(name)
        junction_tom[junction.name] = (names[0], names[1])

    active_event_tables: set[str] = set()
    for binding in EVENT_BINDINGS:
        if binding.table not in available or binding.entity not in plans:
            continue
        table = add_table(binding.table, "bronze")
        _require(table.columns, binding.key, binding.table)
        _require(table.columns, _TIMESTAMP_COLUMN, binding.table)
        to_table, to_column = key_column(binding.entity)
        rel_name = _tom_name(binding.table, binding.key, to_table)
        active = binding.table not in active_event_tables
        active_event_tables.add(binding.table)
        tom.append((rel_name, f"{binding.table}.{binding.key}", f"{to_table}.{to_column}", active))
        plan = plans[binding.entity]
        plan.additional.append((binding.table, rel_name))
        used = {prop.name for prop in plan.properties}
        for column, data_type in table.columns:
            if column == binding.key:
                continue
            name = f"{snake_case(binding.table)}_{snake_case(column)}"
            if name in used:
                raise OntologyModelError(f"{binding.entity}: duplicate property {name!r}.")
            used.add(name)
            plan.properties.append(_Property(
                name, f"TimeSeries<{data_type}>",
                f"Represents the {_label(column)} observed for the {binding.entity} entity "
                f"in the {binding.table} Eventhouse event stream.",
                column, binding.table, ordering_column=_TIMESTAMP_COLUMN,
            ))

    parts: dict[str, str] = {
        ".platform": json.dumps({
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/"
                       "gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {"type": "Ontology", "displayName": display_name,
                         "description": DESCRIPTION},
            "config": {"version": "2.0", "logicalId": "00000000-0000-0000-0000-000000000000"},
        }, indent=2) + "\n",
        "database.tmdl": _doc(DESCRIPTION)
        + f"database\n\tcompatibilityLevel: {COMPATIBILITY_LEVEL}\n",
        "expressions.tmdl": (
            f"expression {EXPRESSION_NAME} =\n\t\tlet\n"
            f'\t\t    database = Sql.Database("{sql_endpoint}", "{sql_database_id}")\n'
            "\t\tin\n\t\t    database\n"
            f"\tlineageTag: {lineage_tag('expression', EXPRESSION_NAME)}\n"
        ),
        "namespaces/default.tmdl": "namespace default\n\tlineageTag: default\n",
    }
    for table in tables.values():
        lines = [f"table {_quote(table.name)}", f"\tlineageTag: {lineage_tag('table', table.name)}", ""]
        for column, data_type in table.columns:
            lines += [
                f"\tcolumn {_quote(column)}",
                f"\t\tdataType: {data_type}",
                f"\t\tlineageTag: {lineage_tag('column', table.name, column)}",
                f"\t\tsourceColumn: {column}",
                "",
            ]
        lines += [
            f"\tpartition {_quote(table.name)} = entity",
            "\t\tmode: directLake",
            "\t\tsource",
            f"\t\t\tentityName: {table.name}",
            f"\t\t\tschemaName: {table.schema}",
            f"\t\t\texpressionSource: {EXPRESSION_NAME}",
            "",
        ]
        for name, value in annotations:
            lines += [f"\t\tannotation {name} = {value}", ""]
        parts[f"tables/{table.name}.tmdl"] = "\n".join(lines)

    parts["relationships.tmdl"] = "\n".join(
        f"relationship {name}\n"
        + ("" if active else "\tisActive: false\n")
        + f"\tfromColumn: {from_column}\n\ttoColumn: {to_column}\n"
        for name, from_column, to_column, active in tom
    )

    for plan in plans.values():
        entity = plan.entity
        key_name = snake_case(entity.key)
        text = _doc(
            f"Retail {entity.name} business entity sourced from the "
            f"{entity.table} Lakehouse table."
        )
        text += (
            f"entity {entity.name}\n"
            f"\tlineageTag: {lineage_tag('entity', entity.name)}\n"
            f"\tbackingTable: {entity.table}\n"
            f"\tkeyProperty: {key_name}\n\n"
        )
        for prop in plan.properties:
            text += _doc(prop.description, "\t")
            text += (
                f"\tproperty {prop.name}\n"
                f"\t\tdataType: {prop.data_type}\n"
                f"\t\tlineageTag: {lineage_tag('property', entity.name, prop.name)}\n\n"
                "\t\tbackingConfiguration\n"
            )
            if prop.ordering_column:
                text += (
                    "\t\t\ttype: timeSeries\n"
                    f"\t\t\tvalueColumn: {prop.table}.{prop.column}\n"
                    f"\t\t\torderingColumn: {prop.table}.{prop.ordering_column}\n\n"
                )
            else:
                text += f"\t\t\tvalueColumn: {prop.table}.{prop.column}\n\n"
        for table_name, relationship in plan.additional:
            text += (
                "\tadditionalBackingTable\n"
                f"\t\ttable: {table_name}\n"
                f"\t\trelationship: {relationship}\n\n"
            )
        parts[f"entities/{entity.name}.tmdl"] = text

    rel_text = []
    for rel in RELATIONSHIPS:
        rel_text.append(
            _doc(f"{rel.source} to {rel.target} through {rel.from_table}.{rel.from_column}.")
            + f"entityRelationship {_quote(rel.name)}\n"
            f"\tlineageTag: {lineage_tag('entityRelationship', rel.name)}\n"
            f"\tfromEntity: {rel.source}\n\ttoEntity: {rel.target}\n\n"
            "\tbackingConfiguration\n"
            f"\t\trelationship: {_tom_name(rel.from_table, rel.from_column, rel.to_table)}\n"
        )
    for junction in JUNCTIONS:
        from_rel, to_rel = junction_tom[junction.name]
        rel_text.append(
            _doc(f"{junction.source} to {junction.target} through the {junction.table} table.")
            + f"entityRelationship {_quote(junction.name)}\n"
            f"\tlineageTag: {lineage_tag('entityRelationship', junction.name)}\n"
            f"\tfromEntity: {junction.source}\n\ttoEntity: {junction.target}\n\n"
            "\tbackingConfiguration\n"
            f"\t\ttype: table\n\t\ttable: {junction.table}\n"
            f"\t\tfromRelationship: {from_rel}\n\t\ttoRelationship: {to_rel}\n"
        )
    parts["entityRelationships.tmdl"] = "\n".join(rel_text)

    rules = RULES if include_rules else ()
    for rule in rules:
        parts[f"rules/{rule.name}.tmdl"] = render_rule(rule, lineage_tag("rule", rule.name))

    model = ["model Model", ""]
    model += [f"ref table {_quote(name)}" for name in tables] + [""]
    model += [f"ref entity {plan.entity.name}" for plan in plans.values()] + [""]
    model += ["ref namespace default", ""]
    if rules:
        model += [f"ref rule {rule.name}" for rule in rules] + [""]
    parts["model.tmdl"] = "\n".join(model)

    validate_definition(parts, plans)
    return parts


def validate_definition(parts: Mapping[str, str], plans: Mapping[str, _EntityPlan] | None = None) -> None:
    """Check the payload limits and internal references Fabric enforces."""
    if plans:
        for plan in plans.values():
            if len(plan.properties) > 1000:
                raise OntologyModelError(f"{plan.entity.name} exceeds 1,000 properties.")
            if len(plan.additional) > 100:
                raise OntologyModelError(f"{plan.entity.name} exceeds 100 backing tables.")
    rule_count = sum(1 for path in parts if path.startswith("rules/"))
    if rule_count > 1000:
        raise OntologyModelError("Ontology definitions are limited to 1,000 rules.")
    tom_names = set(re.findall(r"^relationship (\S+)$", parts.get("relationships.tmdl", ""), re.M))
    for path, text in parts.items():
        if not path.startswith(("entities/", "entityRelationships")):
            continue
        for reference in re.findall(
            r"^\t+(?:relationship|fromRelationship|toRelationship): (\S+)$", text, re.M
        ):
            if reference not in tom_names:
                raise OntologyModelError(f"{path} references missing relationship {reference}.")


def definition_payload(parts: Mapping[str, str]) -> dict[str, list[dict[str, str]]]:
    """Encode parts as a Fabric item definition payload."""
    return {
        "parts": [
            {
                "path": path,
                "payload": base64.b64encode(text.encode("utf-8")).decode("ascii"),
                "payloadType": "InlineBase64",
            }
            for path, text in sorted(parts.items())
        ]
    }
