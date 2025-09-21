terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
    }
    databricks = {
      source = "databricks/databricks"
    }
  }
  backend "local" {
    path = "terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

provider "databricks" {
  alias               = "ws"
  host                = local.databricks_workspace_host
  azure_client_id     = var.azure_client_id     # Application (client) ID
  azure_client_secret = var.azure_client_secret # Client secret value
  azure_tenant_id     = var.azure_tenant_id     # Directory (tenant) ID

}

provider "databricks" {
  alias      = "adm"
  host       = local.databricks_accounts_host
  account_id = var.account_id
  #   azure_client_id     = var.azure_client_id     # Application (client) ID
  #   azure_client_secret = var.azure_client_secret # Client secret value
  #   azure_tenant_id     = var.azure_tenant_id     # Directory (tenant) ID
}

data "azurerm_databricks_workspace" "faers_databricks_ws" {
  name                = azurerm_databricks_workspace.fears_databricks_ws.name
  resource_group_name = azurerm_resource_group.faers_lakehouse_rg.name
}

locals {
  databricks_workspace_host = data.azurerm_databricks_workspace.faers_databricks_ws.workspace_url
  databricks_accounts_host  = "https://accounts.azuredatabricks.net"
}