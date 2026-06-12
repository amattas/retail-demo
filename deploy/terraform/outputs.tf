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
  value       = fabric_kql_database.main.id
  description = "Retail KQL Database item ID."
}

output "kql_database_name" {
  value       = fabric_kql_database.main.display_name
  description = "Retail KQL Database display name."
}

output "eventstream_id" {
  value       = var.eventstream_enabled ? fabric_eventstream.main[0].id : null
  description = "Retail Eventstream item ID when enabled."
}
