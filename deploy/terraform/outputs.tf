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

output "spark_realtime_pool_id" {
  value       = var.spark_realtime_pool_enabled ? fabric_spark_custom_pool.realtime[0].id : null
  description = "Secondary real-time Spark pool item ID when enabled (null otherwise)."
}

output "spark_realtime_pool_name" {
  value       = var.spark_realtime_pool_enabled ? fabric_spark_custom_pool.realtime[0].name : null
  description = "Secondary real-time Spark pool name to select in notebook compute settings (null when disabled)."
}

output "clickstream_eventhouse_id" {
  value       = var.clickstream_enabled ? fabric_eventhouse.clickstream[0].id : null
  description = "Clickstream Eventhouse item ID (null when clickstream is disabled)."
}

output "clickstream_eventhouse_name" {
  value       = var.clickstream_enabled ? fabric_eventhouse.clickstream[0].display_name : null
  description = "Clickstream Eventhouse display name (null when clickstream is disabled)."
}

output "clickstream_kql_database_id" {
  value       = var.clickstream_enabled ? fabric_kql_database.clickstream[0].id : null
  description = "Clickstream KQL database item ID (null when clickstream is disabled)."
}

output "clickstream_kql_database_name" {
  value       = var.clickstream_enabled ? fabric_kql_database.clickstream[0].display_name : null
  description = "Clickstream KQL database display name (null when clickstream is disabled)."
}

output "clickstream_eventstream_id" {
  value       = var.clickstream_enabled ? fabric_eventstream.clickstream[0].id : null
  description = "Clickstream Eventstream item ID (null when clickstream is disabled)."
}

output "clickstream_eventstream_name" {
  value       = var.clickstream_enabled ? fabric_eventstream.clickstream[0].display_name : null
  description = "Clickstream Eventstream display name (null when clickstream is disabled)."
}
