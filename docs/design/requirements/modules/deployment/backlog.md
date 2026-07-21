# Deployment backlog

## Open

### IMP-001 - Verify alternate authentication and target selection {#imp-001}

- **Priority / effort:** P2 / M
- **Current boundary:** The Azure CLI path using the Terraform-resolved
  workspace and Eventhouse default KQL database is implemented. The selected
  auth mode reaches Fabric REST, KQL, task-flow, export, publication, and
  pipeline clients.
- **Remaining outcome:** Azure PowerShell and renamed-target deployments use
  the configured tenant and Terraform-resolved item IDs without display-name
  ambiguity or contradictory KQL database configuration.
- **Acceptance:** Both credential types receive the configured tenant;
  task-flow deployment uses the resolved workspace ID; the effective KQL
  database name reaches staged artifacts; configuration rejects an unsupported
  Eventhouse/KQL-name split; request-contract tests cover bearer headers and
  non-default names and IDs; and Azure PowerShell plus renamed-Eventhouse smoke
  tests succeed.

### IMP-012 - Introduce tiered deployment profiles and preview gating {#imp-012}

- **Priority / effort:** P2 / M
- **Outcome:** Core, standard, and full-demo profiles publish an explicit
  inventory with cost, runtime, capacity, preview, and manual-step boundaries.
- **Acceptance:** A tenant without previews can complete the default profile,
  while optional profiles fail preflight before partial publication.

## Settled — do not reopen

- `retail-setup deploy` is the supported orchestrator.
- Each environment owns separate Terraform state, Terraform data, generated
  target inputs, bindings, outputs, and deployment journal.
- Terraform provisions resources; `fabric-cicd` publishes supported items; KQL
  scripts remain ordered source.
- The supported Eventhouse topology uses its automatically created default KQL
  database with the same display name.
- Destructive reset assets are never part of an automatic normal run.
