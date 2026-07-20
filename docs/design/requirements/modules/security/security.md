# Security requirements

### REQ-SEC-001 - Threat and control ownership

Security threats shall have stable `THREAT-*` IDs and map to stable `SEC-*`
controls, implementation evidence, and verification evidence.

### REQ-SEC-002 - Synthetic-but-sensitive classification (retired)

This requirement is retired. The supported demo uses generated synthetic data
only; row-level classification and privacy controls for those records are not a
release requirement.

### REQ-SEC-003 - Credential safety

Credentials and tokens shall not be committed or embedded in generated output.
Required clients shall send the real token only to the intended endpoint.

### REQ-SEC-004 - Least-privilege consumers (retired)

This requirement is retired. Standard Fabric workspace and item permissions
remain the access boundary, but field-level restrictions, RLS, and mandatory
agent-governance metadata are outside the default demo scope.

See [the threat model](../../../security/threat-model.md) and
[controls](../../../security/controls.md).
