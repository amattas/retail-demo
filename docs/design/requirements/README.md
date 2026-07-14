# Requirements ownership

Requirements describe outcomes, constraints, and acceptance criteria. They do
not own implementation detail. Exact interfaces and workflows belong in
[specifications](../specifications/README.md), current structure belongs in
[architecture](../architecture/overview.md), and risks and mitigations belong
in [security](../security/threat-model.md).

## Stable IDs

| Prefix | Owner |
| --- | --- |
| `REQ-CORE-*` | Cross-cutting demo behavior |
| `REQ-SETUP-*` | Bootstrap, configuration, and rendering |
| `REQ-DEPLOY-*` | Provisioning and publication |
| `REQ-GEN-*` | Historical synthetic data |
| `REQ-STREAM-*` | Live events and streaming transforms |
| `REQ-AN-*` | Eventhouse and Lakehouse analytics |
| `REQ-MLAI-*` | Machine learning, ontology, and agents |
| `REQ-BI-*` | Power BI semantic model and report |
| `REQ-OPS-*` | Readiness, monitoring, recovery, and CI |
| `REQ-SEC-*` | Security and governance outcomes |
| `REQ-DOCS-*`, `REQ-PUBLISH-*` | Documentation and publishing |
| `THREAT-*`, `SEC-*` | Security threats and controls |

IDs are permanent. Retire an obsolete ID in
[traceability.md](traceability.md); do not reuse it for a different outcome.

## Traceability states

Only these states are allowed:

| State | Meaning |
| --- | --- |
| `proposed` | Draft outcome that has not been accepted. |
| `accepted` | Normative outcome; implementation is absent or incomplete. |
| `implemented` | Exact implementation evidence exists; verification is incomplete. |
| `verified` | Exact implementation and verification evidence exist. |
| `blocked` | The outcome cannot progress until a named external or upstream gate is resolved. |
| `retired` | The outcome is no longer required and its replacement or rationale is recorded. |

## Module ownership

Each module has one requirements page and one backlog. A backlog contains only:

- `## Open`
- `## Settled — do not reopen`

Completed-work histories do not belong in backlogs. Git history and
[traceability.md](traceability.md) retain completion evidence.
