# Clickstream real-time path: a dedicated Eventhouse that receives high-volume
# synthetic clickstream events through a Fabric Eventstream.
#
# Topology (see deploy/terraform/clickstream/*.tmpl):
#   Python generator --> Eventstream CustomEndpoint source --> Eventstream
#   default stream --> Eventhouse ProcessedIngestion destination -->
#   clickstream_eventhouse / <kql database> / clickstream_events table.
#
# The whole path is opt-in via `clickstream_enabled` so environments that do not
# need the clickstream demo (or lack the capacity headroom) skip it entirely.
# ProcessedIngestion mode maps event JSON columns to the destination table by
# name, so no named ingestion mapping is required; the explicit table below is
# the schema contract the generator and downstream queries share.

locals {
  clickstream_enabled = var.clickstream_enabled
}

resource "fabric_eventhouse" "clickstream" {
  count        = local.clickstream_enabled ? 1 : 0
  display_name = var.clickstream_eventhouse_name
  description  = "Clickstream real-time events eventhouse"
  workspace_id = local.workspace_id

  configuration = var.clickstream_eventhouse_minimum_consumption_units == null ? null : {
    minimum_consumption_units = var.clickstream_eventhouse_minimum_consumption_units
  }
}

# Explicit KQL database + table so the clickstream schema is a versioned
# contract rather than inferred at first ingestion. Bound to the eventhouse via
# `parentEventhouseItemId` in DatabaseProperties.json.
resource "fabric_kql_database" "clickstream" {
  count        = local.clickstream_enabled ? 1 : 0
  display_name = var.clickstream_kql_database_name
  description  = "Clickstream KQL database (clickstream_events table)"
  workspace_id = local.workspace_id
  format       = "Default"

  definition = {
    "DatabaseProperties.json" = {
      source = "${path.module}/clickstream/kql/DatabaseProperties.json.tmpl"
      tokens = {
        EventhouseItemId = fabric_eventhouse.clickstream[0].id
      }
    }
    "DatabaseSchema.kql" = {
      source = "${path.module}/clickstream/kql/DatabaseSchema.kql.tmpl"
      tokens = {
        TableName = var.clickstream_table_name
      }
    }
  }
}

# Eventstream: CustomEndpoint source (the Python generator pushes here) routed
# straight to the eventhouse table. Token substitution injects the resolved
# workspace/eventhouse/database identifiers into the topology definition.
resource "fabric_eventstream" "clickstream" {
  count        = local.clickstream_enabled ? 1 : 0
  display_name = var.clickstream_eventstream_name
  description  = "Clickstream ingestion eventstream (custom endpoint to eventhouse)"
  workspace_id = local.workspace_id
  format       = "Default"

  definition = {
    "eventstream.json" = {
      source = "${path.module}/clickstream/eventstream.json.tmpl"
      tokens = {
        WorkspaceId       = local.workspace_id
        KqlDatabaseItemId = fabric_kql_database.clickstream[0].id
        DatabaseName      = fabric_kql_database.clickstream[0].display_name
        TableName         = var.clickstream_table_name
      }
    }
  }

  depends_on = [fabric_kql_database.clickstream]
}
