# Deployment backlog

## Open

### IMP-001 - Verify alternate authentication and target selection {#imp-001}

- **Priority / effort:** P2 / M
- **Current boundary:** Both credential modes receive the configured tenant.
  Orchestrated task-flow deployment uses the Terraform workspace, Lakehouse,
  Eventhouse, and KQL database IDs. Staged shortcuts, ontology, stream
  notebook, and queryset definitions receive the configured KQL database name,
  while configuration continues to reject a split Eventhouse/KQL topology.
- **Remaining outcome:** Live Azure PowerShell and renamed-Eventhouse smoke
  runs must prove the locally contract-tested paths against Fabric.
- **Acceptance:** Both credential types receive the configured tenant;
  task-flow deployment uses the resolved workspace ID; the effective KQL
  database name reaches staged artifacts; configuration rejects an unsupported
  Eventhouse/KQL-name split; request-contract tests cover bearer headers and
  non-default names and IDs; and Azure PowerShell plus renamed-Eventhouse smoke
  tests succeed. All local clauses are covered; the two live smoke clauses keep
  this item open.

### IMP-012 - Introduce tiered deployment profiles and preview gating {#imp-012}

- **Priority / effort:** P2 / M
- **Current boundary:** The shared manifest now resolves dependency-closed
  `core`, `standard`, and `full-demo` inventories. Configure, render, staging,
  Terraform, KQL, pipelines, Reporting, agents, task flow, dry-run output, and
  journals use that exact selection. Local preflight is first and fails on
  blockers, source gaps, unsafe downgrades, and missing acknowledgements before
  mutation. Core is preview-free and excludes Reporting. Standard/full use an
  exact-run required ML gate and two-phase Reporting publication; their prior
  `IMP-008` blockers are removed. Full-demo retains its four explicit preview,
  capacity, task-flow, and manual acknowledgements.
- **Remaining outcome:** Live runs must prove core completion without previews,
  the standard required Reporting gate, and full-demo fail-closed behavior at
  tenant/capacity/manual boundaries.
- **Acceptance:** A tenant without previews can complete the default profile,
  while optional profiles fail preflight before partial publication. Exact
  local inventory, selection, acknowledgement, blocker, and ordering clauses
  are implemented and contract-tested; only live profile/capability proof keeps
  this item open.

## Settled — do not reopen

- `retail-setup deploy` is the supported orchestrator.
- Each environment owns separate Terraform state, Terraform data, generated
  target inputs, bindings, outputs, and deployment journal.
- Terraform provisions resources; `fabric-cicd` publishes supported items; KQL
  scripts remain ordered source.
- The supported Eventhouse topology uses its automatically created default KQL
  database with the same display name.
- Destructive reset assets are never part of an automatic normal run.
