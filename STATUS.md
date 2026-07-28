# Status: Retail Demo Deployment Hardening

**Updated**: 2026-07-28 | **Tasks**: 6/6 complete

## Tasks

| Task | Status | Wave | Notes |
| --- | --- | --- | --- |
| Dynamic report dates | Done | E | Merged in PR #361 |
| Full-demo preflight and profile migration | Done | E | Included in PR #362 |
| Setup and ML deployment recovery | Done | E | Live required and optional pipelines completed |
| Readiness and SQL metadata hardening | Done | E | Exact-run recovery and bundled SQL fallback verified |
| Ontology, Data Agents, and task flow | Done | E | Published and live-validated |
| Documentation structure and accessibility | Done | E | Role-based navigation, glossary, plain-language guides, and aligned technical references |

## Artifacts

| File | Status |
| --- | --- |
| `deploy/.generated/retail-demo/readiness-report.json` | Done |
| `deploy/.generated/retail-demo/deploy-run.json` | Done |
| PR #362 | Active |

## Session Log

- Completed: Full-demo historical setup, required/optional/experimental ML,
  Reporting, Ontology, Data Agents, task flow, and exact-run recovery.
- Completed: SQL endpoint metadata synchronization and profile-aware live
  readiness with only the intentionally unstarted stream evidence degraded.
- Completed: Documentation restructured for business users, business analysts,
  operators, presenters, and entry-level developers; Zensical and documentation
  contract checks pass.
- In progress: PR #362 CI and merge.
- Blocked: None.
