# Native retail replenishment Plan

Opt-in **Protect weekend availability** workbook for human-reviewed planning.
The checked-in `.Plan` folder contains 16 native multipart definition files,
not an empty Plan item. No deployment, sign-in, Plan opening, or billing action
is performed by this renderer.

## Workbook

| Sheet | Model evidence and inputs |
| --- | --- |
| Availability analysis | Store/SKU risk probability, days of inventory, signed store stock, baseline forecast and bounds, and risk/forecast freshness |
| Replenishment what-if — assumptions | Read-only stock and forecast beside editable transfer units, expedite units, per-store/SKU budget, and demand uplift percentage |
| DC stock evidence — not allocated supply | DC stock by SKU, deliberately separate from store rows to avoid multiplying the same stock across stores |

Native scenarios are **Baseline** (locked), **Transfer**, and **Expedite**.
All editable inputs start at **zero**, not invented available stock or a
preapproved purchase. Transfer and Expedite reference their respective quantity
columns plus budget/uplift. Enter proposals at the store/SKU leaf level; do not
edit parent totals or interpret summed uplift percentages as meaningful.
The quantity limits (100,000), budget limit (1,000,000), and uplift limit (100%)
are synthetic input guardrails, **not supply, funding, or feasibility evidence**.

These are draft inputs, not computed optimization results. Editing uplift does
not alter the model forecast; no formula engine syntax, simulation cell payload,
solver result, or public “run Optimize” API is invented here. Human review must
reconcile proposed quantities, donor availability, lead times, budget and
expected outcomes before accepting a scenario. This workbook does not place
orders or execute transfers.

### Cohort and evidence limits

The published numeric filter configuration declares a fixed synthetic cohort:

- `dim_stores[ID]` between 1 and 5.
- `dim_products[ID]` between 1 and 20.
- `dc_inventory_position_current[DC ID]` equal to 1, only on the DC sheet.

Forecast measures explicitly select the first seven dates after the model's
source-as-of date. Do not filter `Forecast Horizon Days` to 7: the generation
defaults are 14 days for demand forecasts and 3 days for stockout risk, so
that filter would remove the deployed evidence. Risk retains its model
horizon; it is not relabeled as seven-day risk. Publish the additive
`Next 7 Days` forecast/bound measures with the Reporting model first.

These are **demo ID selections**, not a claim that all 100 store/SKU
combinations exist, have forecasts, or have stock. They are UI filter
configuration, not an authorization boundary. Confirm effective filtering and
actual cohort membership before relying on the workbook. Verify that forecast
dates cover the intended weekend; verify dates,
freshness, and one current model snapshot per store/SKU before review.

Model values remain read-only. Missing, zero, or negative stock is not replaced
with a positive assumption. DC on-hand is a signed ledger balance, not
available-to-promise stock. No fabricated supplier capacity is included.
Bindings use exact TMDL display names from
`fabric/powerbi/retail_model.SemanticModel/definition/tables`; tests check these
against the source model without editing it.

## Renderer API

```python
from deploy.scripts._plan_definition import render_plan_definition

definition = render_plan_definition(
    workspace_id=workspace_id,
    workspace_name=workspace_name,
    semantic_model_id=semantic_model_id,
    semantic_model_name=semantic_model_name,
    connection_id=connection_id,
    display_name=display_name,
)
```

Returns `{"format": "PlanV1", "parts": [...]}`. Each part contains its native
relative `path`, UTF-8 JSON encoded as base64 `payload`, and
`payloadType: "InlineBase64"`. The parent deployment layer supplies this as the
item definition.

All three target IDs must be non-nil UUID strings and all three names must be
nonempty strings. Names are assigned to parsed JSON fields, never substituted
into JSON text, paths, or formulas. Rendering neither changes source files nor
retains target values between calls. No credentials are accepted or stored.

The source `${...}` markers are deliberately **not deployable as-is**: call the
renderer. Stable plan/sheet/visual/comment/scenario UUIDs are content identities
derived with UUIDv5 from `retail-demo:planning:weekend:<content-name>`, not live
workspace, model, or connection IDs. The full documented semantic-model
reference form preserves runtime names and explicitly sets Direct Lake mode.

## Required setup and acceptance gates

The parent deployment/operator owns these prerequisites:

1. Enable and authorize the Planning workload on an appropriate capacity.
   Plan creation can provision a Fabric SQL database for its internal metadata;
   opening and using Planning can incur charges. Obtain approval first.
2. Resolve `retail_model` and its workspace. Create/share an actual **Power BI
   Semantic Model** cloud connection and pass its ID. This is not the SQL
   endpoint connection or the model's underlying OneLake connection.
3. Configure the Direct Lake model's underlying cloud connection with **fixed
   credentials**, not SSO. Check connection access and effective data access
   for intended users. A well-formed UUID does not prove these requirements.
4. After separately authorized deployment, verify native import acceptance,
   visual hydration, effective filters, model measures, editable leaf cells,
   scenario isolation/locking and persistence. Offline schema validation is
   **not evidence of successful service execution**. If a runtime rejects or
   ignores a binding, stop and obtain a sanitized native `getDefinition`
   export of that configured sheet rather than guessing opaque visual state.
5. Keep review/approval in the parent-owned human process. SQL collaboration,
   approval routing, and writeback credentials are not configured here.
   Comments and notifications are disabled. No `writeback.json`, `cube`,
   InfoBridge, PowerTable approvals/automations, or external execution is
   packaged. Adding them requires a separate, approved integration.

No Plan was opened, no connection was created, and no live acceptance was
attempted while authoring these files.

## Schema sources and tests

The [official Plan definition reference][definition-doc] documents multipart
paths and field semantics. Unmodified Microsoft schemas were downloaded on
2026-09-08 into `schemas/` for offline validation:

| Local snapshot | Published schema path below the schema base |
| --- | --- |
| `definition.schema.json` | `item/plan/definition/Definition/1.0.0/schema.json` |
| `planProperties.schema.json` | `item/plan/definition/PlanProperties/1.0.0/schema.json` |
| `sheet.schema.json` | `item/plan/definition/Sheets/Sheet/1.0.0/schema.json` |
| `commentSettings.schema.json` | `item/plan/definition/Sheets/CommentSettings/1.0.0/schema.json` |
| `dataInput.schema.json` | `item/plan/definition/Planning/DataInput/1.0.0/schema.json` |
| `properties.schema.json` | `item/plan/definition/Planning/Properties/1.0.0/schema.json` |
| `scenarios.schema.json` | `item/plan/definition/planning/scenarios/1.0.0/schema.json` |
| `platform.schema.json` | `gitIntegration/platformProperties/2.0.0/schema.json` |

Schema base: `https://developer.microsoft.com/json-schemas/fabric/`.
Use the exact `$id`/`const` casing in the downloaded schemas: some article
examples use lowercase URLs that fail the schema's case-sensitive `const`.
The full required canvas fields are present; the article's shorter canvas
example does not satisfy the downloaded sheet schema.

```powershell
python -m pytest tests\deploy\test_plan_definition.py -q
python -m ruff check deploy\scripts\_plan_definition.py tests\deploy\test_plan_definition.py
```

Tests cover native multipart references, TMDL bindings, immutable source files,
stable IDs, target rebinding, invalid identifiers, hostile names, cohort
configuration, read-only evidence, zero-initialized editable assumptions, and
absence of external execution/writeback. Schema tests use the existing
`jsonschema`/`referencing` installations and make no network requests; pytest
reports a skip if those optional validation packages are unavailable. No
dependency files are changed.

[definition-doc]: https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/plan-definition
[connection-doc]: https://learn.microsoft.com/en-us/fabric/iq/plan/planning-how-to-create-semantic-model-connection
[getting-started]: https://learn.microsoft.com/en-us/fabric/iq/plan/planning-how-to-get-started

Connection requirements: [Create and share a semantic-model connection][connection-doc].
Internal SQL metadata and billing context: [Get started with Planning][getting-started].
