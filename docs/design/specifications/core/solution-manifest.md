# Solution manifest

## Purpose

`contracts/retail-demo.json` is the canonical container for cross-domain
metadata. Its top-level sections are frozen at version `1.3.0`:

- `metadata` and `version`;
- `prerequisites` and `commands`;
- `assets`;
- `profiles`;
- `readiness_expectations`;
- `data_contracts`, `event_paths`, and `exceptions`;
- `ml_contracts`;
- `source_owners`.

The deployment profile entries are executable. The resolver uses them to select
existing logical assets, notebook groups, pipeline folders, and the physical
KQL inventory associated with Eventhouse. ML contract entries are executable:
repository validation compares them with producer notebook declarations,
required runtime-validator declarations, and active TMDL projections.

## Authority boundary

The manifest owns stable IDs, logical ownership, support status, descriptions,
source pointers, profile-to-existing-group/asset selection, dependency
metadata, contract and path declarations, and explicit exceptions. It does not
own item definitions or pipeline bodies. ML schema declarations are validated
projections of their physical producer/TMDL sources, not a second unchecked
physical schema.

| Physical fact | Authoritative source |
| --- | --- |
| Available item types, notebook groups, and physical staging behavior | `deploy/config/deploy.yml` and `deploy/scripts/build_artifacts.py` |
| Event tables, fields, types, and JSON paths | KQL DDL/mappings and `driver-05-stream.py` `ENVELOPE`/`EVENT_PAYLOADS` |
| Historical Lakehouse tables, fields, and types | `utility/src/retail_setup/generation/schemas.py` `TABLES` |
| Active semantic-model tables and Direct Lake bindings | `model.tmdl` references plus each table TMDL's executable `schemaName`/`entityName` partition |
| ML output schemas | Producer notebook `ML_OUTPUT_CONTRACTS`; active TMDL for the four Reporting outputs |

Physical inventories are declarations with a derivation and source selector.
Validators calculate their values from those sources. Item names, event fields,
and table schemas must not be copied into the manifest.

The live event extension follows the same boundary. The manifest stores the
stable envelope/event/path IDs, business and dedupe keys, UTC event-time
semantics, physical targets, terminal coverage, and named exceptions. Parsers
derive fields, types, nullability, JSON paths, transforms, Gold outputs, and
active TMDL bindings from the physical sources during every check.

Each event path must declare its own exact Silver, optional Gold, and optional
semantic-model source pointers. Validation does not pool pointers across paths.
The path mode, source/target asset roles, controlling contract IDs, and named
terminal exception must agree with that path. The envelope declaration must
select the `ENVELOPE` symbol owned by the event contract and share the physical
driver source used for `EVENT_PAYLOADS`.

Every pointer is checked for repository-local resolution. Selector-backed
pointers are parsed by their declared selector, while pointers that name whole
files or directories provide existence/ownership evidence only. Contract-driving
selectors are also consumed by the owning repository validation; they are not
treated as descriptive metadata.

## Readers and validation

`retail_setup.contracts` provides the strict Pydantic loader and repository-aware
validators. Validation checks:

- the frozen schema, globally unique IDs, and internal references;
- repository-local source existence and selectors;
- profile references against existing artifact groups and pipelines;
- profile dependency closure, deployment classification, group/asset
  agreement, exact item/KQL selection, staged counts, and workspace folders;
- source `.platform` and staged item descriptions against manifest-owned asset
  descriptions;
- source-derived inventory relationships;
- Eventhouse DDL, ingestion mappings, and direct-writer field/type/path
  agreement;
- all 18 emitted-event routes, the nine-field envelope, the operational
  `unknown_event` table, Silver/Gold transforms, and active semantic terminals;
- each path's mode, asset/contract ownership, exact per-path sources, and
  semantic-or-exception terminal coverage;
- the derived five-input marketing-attribution route;
- every active TMDL table's executable Direct Lake binding, its agreement with
  `sourceLineageTag`, and the expected `ag` or `au` physical table;
- representative wire fixtures, including nullable and unknown-event cases;
- all 14 ML producer/source/schema/grain/temporal/lineage declarations;
- required ML agreement across producer, validator, and active TMDL;
- all 26 readiness check IDs, categories, profile applicability, and
  required/optional behavior against the unchanged runtime taxonomy;
- declared inventory exceptions.

Run the non-mutating check from a repository checkout:

```powershell
python scripts/check_data_contracts.py
```

It prints stable counts and source-derived streaming-only, historical-only, and
operational boundaries. It never rewrites KQL, notebooks, schemas, or TMDL.

`scripts/solution_manifest.py` is a standard-library-only projection for
bootstrap-safe identity, prerequisites, canonical command examples, profile
names and the declared default, publication counts/folders, manifest version,
and a deterministic semantic SHA-256. `scripts/setup.py` consumes it before
dependencies are installed and derives its `--profile` default from that view.
Both readers reject primitive type coercion. Agreement and negative tests keep
the projection aligned with the typed reader.
Dry-run output, deploy journals, artifact inventories, and readiness reports
carry the same manifest version/hash and resolved profile boundaries.

The canonical human-readable projection is the
[workspace and profile inventory](../../../guides/workspace-inventory.md).

## Extension points

Later work should populate existing sections rather than introduce parallel
manifests:

- profile execution belongs in `profiles`; live standard/full proof remains
  under `IMP-012`;
- ML methodology and outputs belong in executable `ml_contracts`; a fresh live
  required-ML/Reporting run remains the final `IMP-008` gate;
- event fixtures and routes are implemented in `data_contracts`,
  `event_paths`, and `exceptions`; live Fabric evidence remains `IMP-005`;
- readiness taxonomy metadata belongs in `readiness_expectations`; actual live
  evidence remains environment-local and is the remaining `IMP-013` gate;
- live alternate-auth/renamed-target proof remains `IMP-001`, live profile
  capability proof remains `IMP-012`, and fresh required-ML/Reporting proof
  remains `IMP-008`.

Add a source-derived inventory when physical membership is needed. Add an
`exceptions` record for intentional divergence and remove it when the sources
converge.
