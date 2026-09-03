# Setup requirements

### REQ-SETUP-001 - Guided entry points

Windows users shall have a PowerShell entry point and macOS/Linux users shall
have a shell entry point. Both shall invoke the same Python guided setup.

### REQ-SETUP-002 - Explicit configuration

Configuration shall select an environment, tenant, workspace, capacity,
Lakehouse, Eventhouse/KQL database, store profile, history length in months,
store count, and deterministic seed.

### REQ-SETUP-003 - Rendered notebooks

Rendering shall produce five environment-specific notebooks: setup notebooks
01 through 04 plus `stream-events`. Dictionary content shall be pinned to an
explicit Git reference.

### REQ-SETUP-004 - Observable execution

Guided setup and deploy output shall remain linear and copyable, with each step
and command clearly separated and failures surfaced.

See [the CLI specification](../../../specifications/modules/setup/cli.md).
