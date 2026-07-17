terraform {
  required_version = ">= 1.8, < 2.0"

  backend "local" {}

  required_providers {
    fabric = {
      source  = "microsoft/fabric"
      version = ">= 1.0.0"
    }
  }
}

provider "fabric" {
  # fabric_spark_custom_pool is a preview resource; enable preview mode only when
  # the custom pool is requested. Provider config can reference input variables.
  preview = var.spark_custom_pool_enabled
}
