output "deployment_environment" {
  value       = var.environment
  description = "Workspace-derived deployment environment identity."
}

output "deployment_profile" {
  value       = var.deployment_profile
  description = "Executable manifest deployment profile."
}

output "tenant_id" {
  value       = var.tenant_id
  description = "Microsoft Entra tenant associated with this deployment state."
}

output "workspace_id" {
  value       = local.workspace_id
  description = "Target Fabric workspace ID."
}

output "workspace_name" {
  value       = local.workspace_name
  description = "Target Fabric workspace display name."
}

output "lakehouse_id" {
  value       = fabric_lakehouse.main.id
  description = "Retail Lakehouse item ID."
}

output "lakehouse_name" {
  value       = fabric_lakehouse.main.display_name
  description = "Retail Lakehouse display name."
}

output "eventhouse_id" {
  value       = var.eventhouse_enabled ? fabric_eventhouse.main[0].id : null
  description = "Retail Eventhouse item ID."
}

output "eventhouse_name" {
  value       = var.eventhouse_enabled ? fabric_eventhouse.main[0].display_name : null
  description = "Retail Eventhouse display name."
}

output "kql_database_id" {
  value       = var.eventhouse_enabled ? tolist(fabric_eventhouse.main[0].properties.database_ids)[0] : null
  description = "Retail KQL Database item ID (the Eventhouse's default database)."
}

output "kql_database_name" {
  value       = var.eventhouse_enabled ? fabric_eventhouse.main[0].display_name : null
  description = "Retail KQL Database display name (matches the Eventhouse name)."
}

output "spark_custom_pool_id" {
  value       = var.spark_custom_pool_enabled ? fabric_spark_custom_pool.setup[0].id : null
  description = "Custom Spark pool item ID when the custom pool is enabled."
}
