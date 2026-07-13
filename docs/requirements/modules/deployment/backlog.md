# Deployment backlog

## Open

### IMP-001 - Restore the deploy authentication and targeting contract {#imp-001}

- **Priority / effort:** P0 / M
- **Outcome:** Fabric REST, KQL, task-flow, export, and pipeline calls use the
  real token, selected auth mode, workspace, and KQL database.
- **Evidence:** `deploy/scripts/export_items.py`,
  `deploy/scripts/taskflow.py`, `deploy/scripts/apply_kql.py`,
  `deploy/scripts/run_pipeline.py`, and `utility/src/retail_setup/cli/main.py`.
- **Acceptance:** Request-contract tests cover both auth modes and a non-default
  KQL database, followed by a live smoke deployment.

### IMP-004 - Isolate environments and remove operator-specific bindings {#imp-004}

- **Priority / effort:** P2 / L
- **Outcome:** Dev, test, and prod cannot share state or inherit committed
  operator-specific tenant, capacity, workspace, or item identifiers.
- **Acceptance:** Parallel plans use isolated state and `--skip-terraform`
  rejects unresolved or placeholder outputs.

### IMP-012 - Introduce tiered deployment profiles and preview gating {#imp-012}

- **Priority / effort:** P2 / M
- **Outcome:** Core, standard, and full-demo profiles publish an explicit
  inventory with cost, runtime, capacity, preview, and manual-step boundaries.
- **Acceptance:** A tenant without previews can complete the default profile,
  while optional profiles fail preflight before partial publication.

## Settled - do not reopen

- `retail-setup deploy` is the supported orchestrator.
- Terraform provisions resources; `fabric-cicd` publishes supported items; KQL
  scripts remain ordered source.
- Destructive reset assets are never part of an automatic normal run.
