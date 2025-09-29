# ==============================================================================
# MODULE OUTPUTS
# ==============================================================================

output "resource_group_name" {
  description = "Name of the created resource group"
  value       = module.faers_lakehouse.resource_group_name
}

output "storage_account_name" {
  description = "Name of the created storage account"
  value       = module.faers_lakehouse.storage_account_name
}

output "databricks_workspace_url" {
  description = "URL of the Databricks workspace"
  value       = module.faers_lakehouse.databricks_workspace_url
}

output "catalog_names" {
  description = "Names of created catalogs"
  value       = module.faers_lakehouse.catalog_names
}

output "external_location_url" {
  description = "URL of the external location"
  value       = module.faers_lakehouse.external_location_url
}

output "schema_names" {
  description = "List of all created schemas with their catalog information"
  value       = module.faers_lakehouse.schema_names
}