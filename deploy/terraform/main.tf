data "fabric_workspace" "existing" {
  count = var.existing_workspace_id == null ? 0 : 1
  id    = var.existing_workspace_id
}

# Resolve the Fabric capacity by display name when an explicit capacity_id is
# not provided. A workspace without an assigned Fabric capacity cannot create
# Lakehouse/Eventhouse items (the API returns FeatureNotAvailable).
data "fabric_capacity" "main" {
  count        = var.existing_workspace_id == null && var.capacity_id == null && var.capacity_name != null ? 1 : 0
  display_name = var.capacity_name

  lifecycle {
    postcondition {
      condition     = self.state == "Active"
      error_message = "Fabric capacity '${var.capacity_name}' is not Active. Assign an active Fabric (F) SKU capacity before deploying."
    }
  }
}

locals {
  resolved_capacity_id = (
    var.capacity_id != null
    ? var.capacity_id
    : try(data.fabric_capacity.main[0].id, null)
  )
}

resource "fabric_workspace" "main" {
  count                          = var.existing_workspace_id == null ? 1 : 0
  display_name                   = var.workspace_name
  description                    = var.workspace_description
  capacity_id                    = local.resolved_capacity_id
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
  count        = var.eventhouse_enabled ? 1 : 0
  display_name = var.eventhouse_name
  workspace_id = local.workspace_id

  configuration = var.eventhouse_minimum_consumption_units == null ? null : {
    minimum_consumption_units = var.eventhouse_minimum_consumption_units
  }
}

# The Eventhouse auto-creates a single default KQL database with the same display
# name (its id is exposed in properties.database_ids). We use that default database
# for all tables/scripts rather than creating a second fabric_kql_database — one
# Eventhouse, one KQL database. The id/name are surfaced via outputs.

# Capacity-aware custom Spark pool for the setup run (opt-in via
# spark_custom_pool_enabled). Live preflight derives Medium-node bounds from
# base capacity only, reserves 25% headroom, and accounts for every concurrent
# required-ML producer. Executors cap at max_node_count - 1 because one node
# runs the driver.
resource "fabric_spark_custom_pool" "setup" {
  count        = var.spark_custom_pool_enabled ? 1 : 0
  workspace_id = local.workspace_id
  name         = var.spark_custom_pool_name
  node_family  = "MemoryOptimized"
  node_size    = var.spark_node_size
  type         = "Workspace"

  lifecycle {
    precondition {
      condition     = var.spark_max_node_count >= var.spark_min_node_count
      error_message = "spark_max_node_count must be greater than or equal to spark_min_node_count."
    }
  }

  auto_scale = {
    enabled        = true
    min_node_count = var.spark_min_node_count
    max_node_count = var.spark_max_node_count
  }

  dynamic_executor_allocation = {
    enabled       = true
    min_executors = 1
    max_executors = max(var.spark_max_node_count - 1, 1)
  }
}

# Make the custom pool the workspace default. The setup pipeline's notebook
# (TridentNotebook) activities have no per-activity pool selector, so the
# workspace default pool is how they pick up the custom pool.
resource "fabric_spark_workspace_settings" "main" {
  count        = var.spark_custom_pool_enabled ? 1 : 0
  workspace_id = local.workspace_id

  pool = {
    customize_compute_enabled = true
    default_pool = {
      name = fabric_spark_custom_pool.setup[0].name
      type = "Workspace"
    }
  }
}
