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

The supported dataset is generated synthetic data for demonstration only. Do
not load production customer data into the demo.

Fabric workspace and item permissions are the default access boundary. The
release does not require field masking, hashing, aggregation, or RLS for
generated customer-like records.

## Model and agent scope

- The semantic model may expose generated customer-like fields to authorized
  workspace consumers.
- Checked-in semantic-model RLS roles are not required.
- Data Agent datasource instructions and user descriptions may remain unset in
  the default release. Persona-specific behavior remains optional under
  `ENH-003`.

## Documentation publication boundary

Public docs must not contain tenant IDs, workspace IDs, credentials, internal
incident evidence, or generated local configuration. Canonical public content
comes only from reviewed files in `docs/`.

## Verification

- request-level tests for bearer headers and target IDs;
- secret scanning of source and generated output;
- review of deployed workspace and item permissions;
- Zensical output inspection for publication boundary.
