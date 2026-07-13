# Security requirements

### REQ-SEC-001 - Threat and control ownership

Security threats shall have stable `THREAT-*` IDs and map to stable `SEC-*`
controls, implementation evidence, and verification evidence.

### REQ-SEC-002 - Synthetic-but-sensitive classification

Customer-like synthetic fields shall receive explicit classification,
least-privilege access, and AI-use boundaries.

### REQ-SEC-003 - Credential safety

Credentials and tokens shall not be committed or embedded in generated output.
Required clients shall send the real token only to the intended endpoint.

### REQ-SEC-004 - Least-privilege consumers

Default semantic-model, ontology, agent, and documentation surfaces shall expose
only the detail required for their audience.

See [the threat model](../../../security/threat-model.md) and
[controls](../../../security/controls.md).
