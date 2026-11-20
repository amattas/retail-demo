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

### REQ-SETUP-005 - Custom business layouts

Besides the preset store profiles, configuration shall accept a business name
and build a custom layout for it once with GitHub Copilot: a store profile,
departments, products, and brands aligned with what the business sells, with
brand names and product titles changed from the real ones, plus a
brand-inspired color palette and, when Wikimedia Commons has one, the
business's current logo. The operator shall review the layout before it
is used. The accepted layout shall be saved per environment, and generation,
the Power BI and twins reports (colors, page backgrounds, and logo or business
name in page headers), the Fabric Apps, and planning sheets shall
apply it deterministically without calling Copilot again.

See [the CLI specification](../../../specifications/modules/setup/cli.md).
