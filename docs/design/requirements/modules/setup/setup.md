# Setup requirements

### REQ-SETUP-001 - Guided entry points

Windows users shall have a PowerShell entry point and macOS/Linux users shall
have a shell entry point. Both shall invoke the same Python guided setup.

### REQ-SETUP-002 - Explicit configuration

Configuration shall select an environment, tenant, workspace, capacity,
Lakehouse, Eventhouse/KQL database, store profile, history length in months,
store count, and deterministic seed.

### REQ-SETUP-003 - Rendered notebooks

Rendering shall produce twelve environment-specific notebooks: setup notebooks
01 through 11 plus `stream-events`. Dictionary content shall be pinned to an
explicit Git reference.

### REQ-SETUP-004 - Observable execution

Guided setup and deploy shall show each step, its status, and its output,
and shall report failures explicitly. The interactive setup interface shall
show the step list and live output side by side. It shall ask questions in a
dialog box and write the complete output to a setup log. Plain output
(`--plain` or a non-interactive terminal) and the setup log shall be linear
and copyable, with each step and command separated.

See [the CLI specification](../../../specifications/modules/setup/cli.md).
