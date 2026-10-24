# Deployment backlog

## Open

- Remote Terraform state coordination and serialization across pipelines and
  manual writers still need design and validation. Environment-local files
  and a pipeline's `concurrency = 1` do not provide those controls.

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
  journals use that exact selection. Local and live preflight fail on blockers,
  source gaps, unsafe downgrades, disabled tenant switches, and unsuitable
  capacities before changing resources. Core is preview-free and excludes
  Reporting. Standard/full use an exact-run required ML gate and two-phase
  Reporting publication; their prior
  `IMP-008` blockers are removed. Full-demo validates preview and capacity
  prerequisites through documented read-only Fabric APIs. A live full-demo run
  has proven tenant/capacity preflight, required Reporting, post-ontology
  publication, and fail-closed readiness.
- **Remaining outcome:** Live runs must still prove core completion without
  previews and the standalone standard Reporting profile. Prior full-demo
  evidence is a baseline. Changes to isolation, resume, saved-plan safeguards,
  staging, and readiness need fresh integrated and live verification.
- **Acceptance:** A tenant without previews can complete the default profile,
  while optional profiles fail preflight before partial publication. Exact
  local inventory, selection, live prerequisite, blocker, and ordering clauses
  have local implementations and tests. Fresh changed-path validation and
  the remaining core/standard profile proof keep this item open.

## Settled — do not reopen

- `retail-setup deploy` is the supported orchestrator.
- Each environment owns separate Terraform state, Terraform data, generated
  target inputs, generation settings, rendered notebooks, staged items
  (including post-ontology), bindings, outputs, and deployment journal.
- Terraform provisions resources; `fabric-cicd` publishes supported items; KQL
  scripts remain ordered source.
- The supported Eventhouse topology uses its automatically created default KQL
  database with the same display name.
- Destructive reset assets are never part of an automatic normal run.
- `full-demo` is the manifest default for every entry point. Reporting
  profiles publish the connected twins report and the native Operations Agent
  by default; `core` and `standard` are explicit reductions. Native Planning
  stays excluded unless explicitly included because it needs fixed credentials
  and starts non-cancellable billing.
- Same-workspace pipeline notebook references use deterministic logical IDs
  and the default-workspace sentinel resolved natively by `fabric-cicd`, not
  generated per-notebook parameter rules.
- `fabric-cicd` bulk publish stays opt-in while its upstream API is
  experimental and post-ontology Data Agent publication needs dynamic
  target-item resolution.
