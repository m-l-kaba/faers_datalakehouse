terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
    }
    databricks = {
      source = "databricks/databricks"
    }
  }
  backend "azurerm" {
    container_name      = "tfstate"
    key                 = "terraform.tfstate"
    resource_group_name = "faers-lakehouse-rg"
    storage_account_name = "faerslakehouse"
  }
}

provider "azurerm" {
  features {}
}

# Account level provider for initial setup
provider "databricks" {
  alias      = "adm"
  host       = "https://accounts.azuredatabricks.net"
  account_id = var.account_id
}

# Workspace level provider - uses Azure authentication
provider "databricks" {
  alias               = "ws"
  azure_workspace_resource_id = "/subscriptions/ecb989f5-517b-4269-8395-1da237c7293c/resourceGroups/faers-lakehouse-rg/providers/Microsoft.Databricks/workspaces/faers-databricks-ws"
  azure_client_id     = var.azure_client_id
  azure_client_secret = var.azure_client_secret
  azure_tenant_id     = var.azure_tenant_id
}