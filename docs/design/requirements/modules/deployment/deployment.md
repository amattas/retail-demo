# Deployment requirements

### REQ-DEPLOY-001 - Environment boundary

Every deploy shall resolve one explicit workspace-derived environment and
validate its tenant, workspace, resource identifiers, and isolated Terraform
state before mutation.

### REQ-DEPLOY-002 - Ordered publication

Deployment shall provision required resources, stage and publish Fabric items,
apply ordered KQL scripts, validate outputs, and start only the configured
post-deploy work.

### REQ-DEPLOY-003 - Authentication modes

Azure CLI and Azure PowerShell Python-client authentication modes shall target
the configured tenant and pass real bearer credentials to every required
Fabric API operation. Azure PowerShell session state shall never be presented
as a Fabric Terraform provider credential. Apply/destroy shall require a
provider-supported credential; otherwise deployment shall require validated
outputs with `--skip-terraform`.

### REQ-DEPLOY-004 - Honest result

A required failed step shall produce a failed or explicitly degraded final
result. Optional steps shall be named and reported separately.

### REQ-DEPLOY-005 - Profiles and previews

The default profile shall be GA-safe and capacity-conscious. Heavy, preview,
destructive, and manual demo assets shall require explicit selection and
capability preflight.

See [the deployment specification](../../../specifications/modules/deployment/framework.md).
