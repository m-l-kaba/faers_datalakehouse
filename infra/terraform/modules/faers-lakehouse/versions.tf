# ==============================================================================
# TERRAFORM AND PROVIDER REQUIREMENTS
# ==============================================================================

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.90"
      configuration_aliases = [databricks.workspace]
    }
  }
}