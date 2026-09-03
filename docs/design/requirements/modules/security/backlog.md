# Security backlog

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
