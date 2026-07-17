output "deployment_environment" {
  value       = var.environment
  description = "Workspace-derived deployment environment identity."
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
  value       = fabric_eventhouse.main.id
  description = "Retail Eventhouse item ID."
}

output "eventhouse_name" {
  value       = fabric_eventhouse.main.display_name
  description = "Retail Eventhouse display name."
}

output "kql_database_id" {
  value       = tolist(fabric_eventhouse.main.properties.database_ids)[0]
  description = "Retail KQL Database item ID (the Eventhouse's default database)."
}

output "kql_database_name" {
  value       = fabric_eventhouse.main.display_name
  description = "Retail KQL Database display name (matches the Eventhouse name)."
}

output "spark_custom_pool_id" {
  value       = var.spark_custom_pool_enabled ? fabric_spark_custom_pool.setup[0].id : null
  description = "Custom Spark pool item ID when the custom pool is enabled."
}
