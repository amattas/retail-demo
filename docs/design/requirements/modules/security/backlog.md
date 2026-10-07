# Security backlog

## Open

- Re-run target/authentication and destructive recovery smoke gates after the
  current environment/resume/saved-plan changes (`SEC-001`, `SEC-009`,
  `IMP-001`); local code is not fresh Fabric proof.
- Inspect published current and historical documentation plus search indexes
  after release (`SEC-010`, `THREAT-008`).
- Before shared hosting or external actions, validate delegated identity,
  authorized approvals, durable audit delivery, replay labeling,
  evidence-bound actions, idempotency, and fan-out limits (`SEC-012`,
  `THREAT-009`). These proposed controls do not reopen retired row-level RLS
  or mandatory Data Agent persona requirements.

## Settled — do not reopen

- Row-level RLS, field masking, and mandatory agent-governance metadata for
  generated customer-like records are outside the default release scope. The
  demo uses synthetic data only and relies on Fabric workspace and item
  permissions for access control.
- Secrets are supplied through identity, secret stores, environment variables,
  or ignored local files.
- Workflow actions use reviewed commit SHAs and no workflow installs runtime
  plugin marketplaces; Python dependencies, Terraform providers, and downloaded
  bootstrap installers use committed locks or checksums.
- Operating-system package managers and GitHub-hosted runner images remain
  external trust roots; project dependency changes are repository-reviewed.
- Security guidance without implementation or verification remains `accepted`,
  not `verified`.
