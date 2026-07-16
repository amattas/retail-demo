# Security backlog

## Open

### IMP-011 - Add a default privacy, governance, and AI safety baseline {#imp-011}

- **Priority / effort:** P2 / M
- **Outcome:** Customer-like detail is classified and restricted; agents have
  instructions; ownership, retention, and responsible-ML notes are explicit.
- **Acceptance:** Broad-use users and agents cannot retrieve row-level
  identity-like fields without an intentional privileged path.

## Settled — do not reopen

- Synthetic identity-like data is sensitive demo data, not "no-risk" data.
- Secrets are supplied through identity, secret stores, environment variables,
  or ignored local files.
- Workflow actions use reviewed commit SHAs and no workflow installs runtime
  plugin marketplaces; Python dependencies, Terraform providers, and downloaded
  bootstrap installers use committed locks or checksums.
- Operating-system package managers and GitHub-hosted runner images remain
  external trust roots; project dependency changes are repository-reviewed.
- Security guidance without implementation or verification remains `accepted`,
  not `verified`.
