variable "environment" {
  type        = string
  description = "Deployment environment name, such as dev, test, or prod."
}

variable "tenant_id" {
  type        = string
  default     = null
  nullable    = true
  description = "Microsoft Entra tenant ID. Authentication is supplied outside Terraform."
}

variable "subscription_id" {
  type        = string
  default     = null
  nullable    = true
  description = "Azure subscription ID for CI environments that need it."
}

variable "workspace_name" {
  type        = string
  description = "Fabric workspace display name."
}

variable "workspace_description" {
  type        = string
  default     = "Microsoft Fabric retail demo workspace"
  description = "Fabric workspace description."
}

variable "existing_workspace_id" {
  type        = string
  default     = null
  nullable    = true
  description = "Existing Fabric workspace ID. When null, Terraform creates a workspace."
}

variable "capacity_id" {
  type        = string
  default     = null
  nullable    = true
  description = "Fabric capacity ID to assign to the workspace."
}

variable "capacity_name" {
  type        = string
  default     = null
  nullable    = true
  description = "Optional capacity display name for documentation and generated config."
}

variable "skip_capacity_state_validation" {
  type        = bool
  default     = false
  description = "Skip capacity state validation when the caller cannot list capacities."
}

variable "role_assignments" {
  type = list(object({
    principal = object({
      id   = string
      type = string
    })
    role = string
  }))
  default     = []
  description = "Workspace role assignments for users, groups, service principals, or managed identities."
}

variable "lakehouse_name" {
  type        = string
  default     = "retail_lakehouse"
  description = "Fabric Lakehouse display name."
}

variable "lakehouse_enable_schemas" {
  type        = bool
  default     = true
  description = "Enable Lakehouse schemas for ag and au tables."
}

variable "eventhouse_name" {
  type        = string
  default     = "retail_eventhouse"
  description = "Fabric Eventhouse display name."
}

variable "eventhouse_minimum_consumption_units" {
  type        = string
  default     = null
  nullable    = true
  description = "Optional Eventhouse minimum consumption units."
}

variable "kql_database_name" {
  type        = string
  default     = "retail_kql"
  description = "Fabric KQL Database display name."
}

variable "eventstream_enabled" {
  type        = bool
  default     = false
  description = "Create an Eventstream shell."
}

variable "eventstream_name" {
  type        = string
  default     = "retail_eventstream"
  description = "Fabric Eventstream display name."
}

variable "spark_custom_pool_enabled" {
  type        = bool
  default     = false
  description = "Create an F64-optimized custom Spark pool and set it as the workspace default pool so the setup pipeline runs on it. When false, setup uses the workspace starter pool."
}

variable "spark_custom_pool_name" {
  type        = string
  default     = "retail_setup_pool"
  description = "Display name for the custom Spark pool."
}

variable "spark_node_size" {
  type        = string
  default     = "Medium"
  description = "Custom Spark pool node size (MemoryOptimized family). One of: Small, Medium, Large, XLarge, XXLarge."

  validation {
    condition     = contains(["Small", "Medium", "Large", "XLarge", "XXLarge"], var.spark_node_size)
    error_message = "spark_node_size must be one of: Small, Medium, Large, XLarge, XXLarge."
  }
}

variable "spark_min_node_count" {
  type        = number
  default     = 1
  description = "Custom Spark pool autoscale minimum node count."
}

variable "spark_max_node_count" {
  type        = number
  default     = 10
  description = "Custom Spark pool autoscale maximum node count. The F64 default of 10 Medium (8 vCore) nodes = 80 vCores, inside an F64's 128 base Spark vCores (no bursting)."
}
