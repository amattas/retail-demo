# Documentation and contract map

Canonical repository documentation is organized by ownership:

| Concern | Owner |
| --- | --- |
| User workflows | [`docs/guides/`](docs/guides/README.md) |
| Outcomes and acceptance criteria | [`docs/design/requirements/`](docs/design/requirements/README.md) |
| Exact behavior and interfaces | [`docs/design/specifications/`](docs/design/specifications/README.md) |
| Current topology and data flow | [`docs/design/architecture/`](docs/design/architecture/overview.md) |
| Threats and controls | [`docs/design/security/`](docs/design/security/threat-model.md) |
| Implementation gaps and enhancements | Module backlogs under [`docs/design/requirements/modules/`](docs/design/requirements/modules/) |
| Evidence and state | [`docs/design/requirements/traceability.md`](docs/design/requirements/traceability.md) |

Authoritative implementation contracts:

- Lakehouse tables: `utility/src/retail_setup/generation/schemas.py`
- Live payloads: `utility/notebooks/templates/driver-05-stream.py`
- Eventhouse tables: `fabric/kql_database/01-create-tables.kql`
- Semantic-model inventory:
  `fabric/powerbi/retail_model.SemanticModel/definition/model.tmdl`

`IMPROVEMENTS.md` is an index; module backlogs own the actionable entries.
