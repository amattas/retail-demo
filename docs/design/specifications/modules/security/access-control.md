# Access control and data handling

## Identity contexts

| Context | Identity |
| --- | --- |
| Interactive deploy | Azure CLI or Azure PowerShell operator |
| Fabric REST and item deployment | Operator-derived token/credential |
| KQL schema application | Local deploy process with Eventhouse permissions |
| Streaming notebook | Fabric notebook runtime identity |
| Report/KQL/agent consumption | Assigned Fabric/Power BI consumer identity |

Every context must target the configured tenant, workspace, and data item.

## Minimum roles

Apply least privilege:

- deployment operators need only the workspace/item permissions required to
  create or update selected assets;
- streaming notebook identity needs KQL ingestion rights on the target database;
- readers should receive Viewer/Database Viewer or narrower model access;
- administrative roles should not be used for routine consumption.

Exact role assignment varies by tenant policy and deployment mode. Validate it
in the target environment rather than relying on an illustrative command.

## Secrets

Allowed sources:

- Azure sign-in;
- GitHub Actions secrets;
- environment variables;
- Key Vault;
- ignored local configuration.

Do not commit secrets to YAML, Terraform, notebooks, generated output, or docs.
Do not replace bearer values with literal mask text in outgoing requests.

## Data classification

The data is synthetic. Customer-like fields such as names, addresses, phone,
loyalty IDs, BLE IDs, and advertising IDs are still classified as
synthetic-but-sensitive.

Default broad-use surfaces should hide, hash, aggregate, or RLS-gate those
fields.

## Current model and agent gaps

- No checked-in semantic-model RLS roles were found.
- Data Agent datasource instructions and user descriptions are unset.
- The current model exposes customer-like fields.

These are current-state facts, not approved exceptions. Remediation is tracked
by `IMP-011`.

## Documentation publication boundary

Public docs must not contain tenant IDs, workspace IDs, credentials, internal
incident evidence, or generated local configuration. Canonical public content
comes only from reviewed files in `docs/`.

## Verification

- request-level tests for bearer headers and target IDs;
- secret scanning of source and generated output;
- role-based semantic-model, KQL, ontology, and agent queries;
- review of exported deployed permissions and agent instructions;
- Zensical output inspection for publication boundary.
