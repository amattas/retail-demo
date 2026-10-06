terraform {
  required_version = ">= 1.8, < 2.0"

  backend "local" {}

  required_providers {
    fabric = {
      source  = "microsoft/fabric"
      version = "1.14.0"
    }
  }
}

provider "fabric" {
  tenant_id = var.tenant_id
  use_cli   = var.fabric_use_cli
}
