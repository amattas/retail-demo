data "fabric_workspace" "existing" {
  count = var.existing_workspace_id == null ? 0 : 1
  id    = var.existing_workspace_id
}

resource "fabric_workspace" "main" {
  count                          = var.existing_workspace_id == null ? 1 : 0
  display_name                   = var.workspace_name
  description                    = var.workspace_description
  capacity_id                    = var.capacity_id
  skip_capacity_state_validation = var.skip_capacity_state_validation
}

locals {
  workspace_id = (
    var.existing_workspace_id == null
    ? fabric_workspace.main[0].id
    : data.fabric_workspace.existing[0].id
  )
  workspace_name = var.workspace_name
}

resource "fabric_workspace_role_assignment" "main" {
  for_each = {
    for assignment in var.role_assignments :
    "${assignment.principal.type}-${assignment.principal.id}-${assignment.role}" => assignment
  }

  workspace_id = local.workspace_id
  principal    = each.value.principal
  role         = each.value.role
}

resource "fabric_lakehouse" "main" {
  display_name = var.lakehouse_name
  workspace_id = local.workspace_id

  configuration = {
    enable_schemas = var.lakehouse_enable_schemas
  }
}

resource "fabric_eventhouse" "main" {
  display_name = var.eventhouse_name
  workspace_id = local.workspace_id

  configuration = var.eventhouse_minimum_consumption_units == null ? null : {
    minimum_consumption_units = var.eventhouse_minimum_consumption_units
  }
}

resource "fabric_kql_database" "main" {
  display_name = var.kql_database_name
  workspace_id = local.workspace_id

  configuration = {
    database_type = "ReadWrite"
    eventhouse_id = fabric_eventhouse.main.id
  }
}

resource "fabric_eventstream" "main" {
  count        = var.eventstream_enabled ? 1 : 0
  display_name = var.eventstream_name
  workspace_id = local.workspace_id
}
