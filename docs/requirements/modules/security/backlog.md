# Security backlog

## Open

### IMP-003 - Harden CI and dependency supply-chain execution {#imp-003}

- **Priority / effort:** P1 / S
- **Outcome:** Privileged actions, plugins, providers, and dependencies use
  reviewed immutable references and minimum permissions.
- **Acceptance:** Repository-wide audit finds no mutable privileged execution;
  dependency changes are reviewable repository changes.

### IMP-011 - Add a default privacy, governance, and AI safety baseline {#imp-011}

- **Priority / effort:** P2 / M
- **Outcome:** Customer-like detail is classified and restricted; agents have
  instructions; ownership, retention, and responsible-ML notes are explicit.
- **Acceptance:** Broad-use users and agents cannot retrieve row-level
  identity-like fields without an intentional privileged path.

## Settled - do not reopen

- Synthetic identity-like data is sensitive demo data, not "no-risk" data.
- Secrets are supplied through identity, secret stores, environment variables,
  or ignored local files.
- Security guidance without implementation or verification remains `accepted`,
  not `verified`.
