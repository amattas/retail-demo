# Deployment requirements

### REQ-DEPLOY-001 - Environment boundary

Every deploy shall resolve one explicit environment and validate its tenant,
workspace, resource identifiers, and isolated Terraform state before mutation.

### REQ-DEPLOY-002 - Ordered publication

Deployment shall provision required resources, stage and publish Fabric items,
apply ordered KQL scripts, validate outputs, and start only the configured
post-deploy work.

### REQ-DEPLOY-003 - Authentication modes

Azure CLI and Azure PowerShell authentication modes shall target the configured
tenant and pass real bearer credentials to every required Fabric operation.

### REQ-DEPLOY-004 - Honest result

A required failed step shall produce a failed or explicitly degraded final
result. Optional steps shall be named and reported separately.

### REQ-DEPLOY-005 - Profiles and previews

The default profile shall be GA-safe and capacity-conscious. Heavy, preview,
destructive, and manual demo assets shall require explicit selection and
capability preflight.

See [the deployment specification](../../../specifications/modules/deployment/framework.md).
